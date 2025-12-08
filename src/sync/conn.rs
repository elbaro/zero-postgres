//! Synchronous PostgreSQL connection.

use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

use crate::error::{Error, Result};
use crate::protocol::backend::BackendKeyData;
use crate::protocol::frontend::write_terminate;
use crate::protocol::types::TransactionStatus;
use crate::state::action::Action;
use crate::state::connection::{ConnectionState, ConnectionStateMachine, Opts, SslAction};
use crate::state::simple_query::{BufferSet, ControlFlow, QueryHandler, SimpleQueryStateMachine};

/// Stream wrapper for TCP or TLS connections.
enum Stream {
    Tcp(BufReader<TcpStream>, BufWriter<TcpStream>),
    #[cfg(feature = "sync-tls")]
    Tls(
        BufReader<native_tls::TlsStream<TcpStream>>,
        BufWriter<native_tls::TlsStream<TcpStream>>,
    ),
}

impl Stream {
    fn tcp(stream: TcpStream) -> Result<Self> {
        let reader = stream.try_clone()?;
        Ok(Self::Tcp(BufReader::new(reader), BufWriter::new(stream)))
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        match self {
            Stream::Tcp(reader, _) => reader.read_exact(buf)?,
            #[cfg(feature = "sync-tls")]
            Stream::Tls(reader, _) => reader.read_exact(buf)?,
        }
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            Stream::Tcp(_, writer) => writer.write_all(buf)?,
            #[cfg(feature = "sync-tls")]
            Stream::Tls(_, writer) => writer.write_all(buf)?,
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Stream::Tcp(_, writer) => writer.flush()?,
            #[cfg(feature = "sync-tls")]
            Stream::Tls(_, writer) => writer.flush()?,
        }
        Ok(())
    }
}

/// Read a message from the stream into the buffer set.
fn read_message_into(stream: &mut Stream, buffer_set: &mut BufferSet) -> Result<()> {
    // Read type byte
    let mut type_byte = [0u8; 1];
    stream.read_exact(&mut type_byte)?;
    buffer_set.type_byte = type_byte[0];

    // Read length (4 bytes, big-endian)
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = i32::from_be_bytes(length_bytes) as usize;

    if length < 4 {
        return Err(Error::Protocol(format!(
            "Invalid message length: {}",
            length
        )));
    }

    // Read payload
    let payload_len = length - 4;
    buffer_set.read_buffer.clear();
    buffer_set.read_buffer.resize(payload_len, 0);
    stream.read_exact(&mut buffer_set.read_buffer)?;

    Ok(())
}

/// Synchronous PostgreSQL connection.
pub struct Conn {
    stream: Stream,
    buffer_set: BufferSet,
    /// Write buffer for outgoing messages
    write_buffer: Vec<u8>,
    /// Backend key data for query cancellation
    backend_key: Option<BackendKeyData>,
    /// Server parameters
    server_params: Vec<(String, String)>,
    /// Current transaction status
    transaction_status: TransactionStatus,
    /// Connection is broken
    is_broken: bool,
}

impl Conn {
    /// Connect to a PostgreSQL server.
    pub fn connect(host: &str, port: u16, options: Opts) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)?;
        stream.set_nodelay(true)?;

        Self::connect_with_stream(stream, options)
    }

    /// Connect using an existing TCP stream.
    pub fn connect_with_stream(stream: TcpStream, options: Opts) -> Result<Self> {
        let mut conn_stream = Stream::tcp(stream)?;
        let mut buffer_set = BufferSet::new();

        let mut state_machine = ConnectionStateMachine::new(options);

        // Start the connection process
        match state_machine.start() {
            Action::WritePacket(data) => {
                conn_stream.write_all(data)?;
                conn_stream.flush()?;
            }
            _ => return Err(Error::Protocol("Unexpected initial action".into())),
        }

        // Handle SSL negotiation or continue with startup
        if state_machine.state() == ConnectionState::WaitingSslResponse {
            // Read single byte SSL response
            let mut ssl_response = [0u8; 1];
            conn_stream.read_exact(&mut ssl_response)?;

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
                    conn_stream.write_all(data)?;
                    conn_stream.flush()?;
                }
            }
        }

        // Drive the state machine to completion
        loop {
            // Read next message into buffer set
            read_message_into(&mut conn_stream, &mut buffer_set)?;

            match state_machine.step(&mut buffer_set)? {
                Action::NeedPacket(_) => {
                    // Continue reading
                }
                Action::WritePacket(data) => {
                    conn_stream.write_all(data)?;
                    conn_stream.flush()?;
                }
                Action::AsyncMessage(_async_msg) => {
                    // Handle async message during startup
                }
                Action::Finished => {
                    break;
                }
            }
        }

        Ok(Self {
            stream: conn_stream,
            buffer_set,
            write_buffer: Vec::with_capacity(8192),
            backend_key: state_machine.backend_key().copied(),
            server_params: state_machine.server_params().to_vec(),
            transaction_status: state_machine.transaction_status(),
            is_broken: false,
        })
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
