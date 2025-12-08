//! Synchronous PostgreSQL connection.

use std::net::TcpStream;
use std::os::unix::net::UnixStream;

use crate::error::{Error, Result};
use crate::opts::Opts;
use crate::protocol::backend::BackendKeyData;
use crate::protocol::frontend::write_terminate;
use crate::protocol::types::TransactionStatus;
use crate::state::action::Action;
use crate::state::connection::{ConnectionState, ConnectionStateMachine, SslAction};
use crate::state::simple_query::{BufferSet, ControlFlow, QueryHandler, SimpleQueryStateMachine};

use super::stream::Stream;

/// Read a message from the stream into the buffer set.
fn read_message_into(stream: &mut Stream, buffer_set: &mut BufferSet) -> Result<()> {
    // Read type byte
    let mut type_byte = [0u8; 1];
    stream.read_exact(&mut type_byte)?;
    buffer_set.type_byte = type_byte[0];

    // Read length (4 bytes, big-endian)
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes);

    if length < 4 {
        return Err(Error::Protocol(format!(
            "Invalid message length: {}",
            length
        )));
    }

    // Read payload
    let payload_len = (length - 4) as usize;
    buffer_set.read_buffer.clear();
    buffer_set.read_buffer.resize(payload_len, 0); // TODO: use read_buf
    stream.read_exact(&mut buffer_set.read_buffer)?;

    Ok(())
}

/// Synchronous PostgreSQL connection.
pub struct Conn {
    stream: Stream,
    buffer_set: BufferSet,
    write_buffer: Vec<u8>,
    backend_key: Option<BackendKeyData>,
    server_params: Vec<(String, String)>,
    transaction_status: TransactionStatus,
    is_broken: bool,
}

impl Conn {
    /// Connect to a PostgreSQL server.
    pub fn new<O: TryInto<Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts = opts.try_into()?;

        let stream = if let Some(ref socket_path) = opts.socket {
            Stream::unix(UnixStream::connect(socket_path)?)
        } else {
            if opts.host.is_empty() {
                return Err(Error::InvalidUsage("host is empty".into()));
            }
            let addr = format!("{}:{}", opts.host, opts.port);
            let tcp = TcpStream::connect(&addr)?;
            tcp.set_nodelay(true)?;
            Stream::tcp(tcp)
        };

        Self::new_with_stream(stream, opts)
    }

    /// Connect using an existing stream.
    pub fn new_with_stream(mut stream: Stream, options: Opts) -> Result<Self> {
        let mut buffer_set = BufferSet::new();

        let mut state_machine = ConnectionStateMachine::new(options.clone());

        // Start the connection process
        match state_machine.start() {
            Action::WritePacket(data) => {
                stream.write_all(data)?;
                stream.flush()?;
            }
            _ => return Err(Error::Protocol("Unexpected initial action".into())),
        }

        // Handle SSL negotiation or continue with startup
        if state_machine.state() == ConnectionState::WaitingSslResponse {
            // Read single byte SSL response
            let mut ssl_response = [0u8; 1];
            stream.read_exact(&mut ssl_response)?;

            match state_machine.process_ssl_response(ssl_response[0])? {
                SslAction::StartHandshake => {
                    #[cfg(feature = "sync-tls")]
                    {
                        return Err(Error::Unsupported("TLS not fully implemented".into()));
                    }
                    #[cfg(not(feature = "sync-tls"))]
                    {
                        return Err(Error::Unsupported(
                            "TLS requested but sync-tls feature not enabled".into(),
                        ));
                    }
                }
                SslAction::SendStartup(data) => {
                    stream.write_all(data)?;
                    stream.flush()?;
                }
            }
        }

        // Drive the state machine to completion
        loop {
            // Read next message into buffer set
            read_message_into(&mut stream, &mut buffer_set)?;

            match state_machine.step(&mut buffer_set)? {
                Action::NeedPacket(_) => {
                    // Continue reading
                }
                Action::WritePacket(data) => {
                    stream.write_all(data)?;
                    stream.flush()?;
                }
                Action::AsyncMessage(_async_msg) => {
                    // Handle async message during startup
                }
                Action::Finished => {
                    break;
                }
            }
        }

        let conn = Self {
            stream,
            buffer_set,
            write_buffer: Vec::with_capacity(8192),
            backend_key: state_machine.backend_key().copied(),
            server_params: state_machine.server_params().to_vec(),
            transaction_status: state_machine.transaction_status(),
            is_broken: false,
        };

        // Upgrade to Unix socket if connected via TCP to loopback
        let conn = if options.upgrade_to_unix_socket && conn.stream.is_tcp_loopback() {
            conn.try_upgrade_to_unix_socket(&options)
        } else {
            conn
        };

        Ok(conn)
    }

    /// Try to upgrade to Unix socket connection.
    /// Returns upgraded conn on success, original conn on failure.
    fn try_upgrade_to_unix_socket(mut self, opts: &Opts) -> Self {
        // Query unix_socket_directories from server
        let mut handler = ShowVarHandler { value: None };
        if self.query("SHOW unix_socket_directories", &mut handler).is_err() {
            return self;
        }

        let socket_dir = match handler.value {
            Some(dirs) => {
                // May contain multiple directories, use the first one
                match dirs.split(',').next() {
                    Some(d) if !d.trim().is_empty() => d.trim().to_string(),
                    _ => return self,
                }
            }
            None => return self,
        };

        // Build socket path: {directory}/.s.PGSQL.{port}
        let socket_path = format!("{}/.s.PGSQL.{}", socket_dir, opts.port);

        // Connect via Unix socket
        let unix_stream = match UnixStream::connect(&socket_path) {
            Ok(s) => s,
            Err(_) => return self,
        };

        // Create new connection over Unix socket
        let mut opts_unix = opts.clone();
        opts_unix.upgrade_to_unix_socket = false;

        match Self::new_with_stream(Stream::unix(unix_stream), opts_unix) {
            Ok(new_conn) => new_conn,
            Err(_) => self,
        }
    }

    /// Get the backend key data for query cancellation.
    pub fn backend_key(&self) -> Option<&BackendKeyData> {
        self.backend_key.as_ref()
    }

    /// Get server parameters.
    pub fn server_params(&self) -> &[(String, String)] {
        &self.server_params
    }

    /// Get a specific server parameter.
    pub fn get_param(&self, name: &str) -> Option<&str> {
        self.server_params
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Get the current transaction status.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Check if currently in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.transaction_status.in_transaction()
    }

    /// Check if the connection is broken.
    pub fn is_broken(&self) -> bool {
        self.is_broken
    }

    /// Execute a simple query with a handler.
    pub fn query<H: QueryHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let result = self.query_inner(sql, handler);
        if let Err(ref e) = result {
            if e.is_connection_broken() {
                self.is_broken = true;
            }
        }
        result
    }

    fn query_inner<H: QueryHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let mut state_machine = SimpleQueryStateMachine::new(HandlerWrapper { inner: handler });

        // Start query
        match state_machine.start(sql) {
            Action::WritePacket(data) => {
                self.stream.write_all(data)?;
                self.stream.flush()?;
            }
            _ => return Err(Error::Protocol("Unexpected start action".into())),
        }

        // Drive the state machine
        loop {
            read_message_into(&mut self.stream, &mut self.buffer_set)?;

            match state_machine.step(&mut self.buffer_set)? {
                Action::NeedPacket(_) => {
                    // Continue reading
                }
                Action::WritePacket(data) => {
                    self.stream.write_all(data)?;
                    self.stream.flush()?;
                }
                Action::AsyncMessage(_async_msg) => {
                    // Handle async message
                }
                Action::Finished => {
                    self.transaction_status = state_machine.transaction_status();
                    break;
                }
            }
        }

        Ok(())
    }

    /// Execute a simple query and discard results.
    pub fn query_drop(&mut self, sql: &str) -> Result<Option<u64>> {
        let mut handler = crate::state::simple_query::DropHandler::new();
        self.query(sql, &mut handler)?;
        Ok(handler.rows_affected())
    }

    /// Execute a simple query and collect all rows.
    pub fn query_collect(
        &mut self,
        sql: &str,
    ) -> Result<(Option<Vec<String>>, Vec<Vec<Option<Vec<u8>>>>)> {
        let mut handler = crate::state::simple_query::CollectHandler::new();
        self.query(sql, &mut handler)?;
        Ok((handler.columns().map(|c| c.to_vec()), handler.take_rows()))
    }

    /// Close the connection gracefully.
    pub fn close(mut self) -> Result<()> {
        self.write_buffer.clear();
        write_terminate(&mut self.write_buffer);
        self.stream.write_all(&self.write_buffer)?;
        self.stream.flush()?;
        Ok(())
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        // Try to send Terminate message, ignore errors
        self.write_buffer.clear();
        write_terminate(&mut self.write_buffer);
        let _ = self.stream.write_all(&self.write_buffer);
        let _ = self.stream.flush();
    }
}

/// Wrapper to adapt external handlers to internal state machine.
struct HandlerWrapper<'a, H> {
    inner: &'a mut H,
}

impl<'a, H: QueryHandler> QueryHandler for HandlerWrapper<'a, H> {
    fn columns(&mut self, desc: crate::protocol::backend::RowDescription<'_>) -> Result<()> {
        self.inner.columns(desc)
    }

    fn row(&mut self, row: crate::protocol::backend::DataRow<'_>) -> Result<ControlFlow> {
        self.inner.row(row)
    }

    fn command_complete(
        &mut self,
        complete: crate::protocol::backend::CommandComplete<'_>,
    ) -> Result<()> {
        self.inner.command_complete(complete)
    }

    fn empty_query(&mut self) -> Result<()> {
        self.inner.empty_query()
    }
}

/// Handler to capture a single value from SHOW query
struct ShowVarHandler {
    value: Option<String>,
}

impl QueryHandler for ShowVarHandler {
    fn columns(&mut self, _desc: crate::protocol::backend::RowDescription<'_>) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, row: crate::protocol::backend::DataRow<'_>) -> Result<ControlFlow> {
        if let Some(Some(bytes)) = row.iter().next() {
            self.value = String::from_utf8(bytes.to_vec()).ok();
        }
        Ok(ControlFlow::Stop)
    }
}
