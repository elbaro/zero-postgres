//! Asynchronous PostgreSQL connection.

use tokio::net::TcpStream;
use tokio::net::UnixStream;

use crate::buffer_pool::PooledBufferSet;
use crate::conversion::ToParams;
use crate::error::{Error, Result};
use crate::handler::{AsyncMessageHandler, BinaryHandler, DropHandler, FirstRowHandler, TextHandler};
use crate::opts::Opts;
use crate::protocol::backend::BackendKeyData;
use crate::protocol::frontend::write_terminate;
use crate::protocol::types::TransactionStatus;
use crate::state::action::Action;
use crate::state::connection::ConnectionStateMachine;
use crate::state::extended::{ExtendedQueryStateMachine, PreparedStatement};
use crate::state::simple_query::SimpleQueryStateMachine;
use crate::state::StateMachine;
use crate::statement::IntoStatement;

use super::stream::Stream;


/// Asynchronous PostgreSQL connection.
pub struct Conn {
    stream: Stream,
    buffer_set: PooledBufferSet,
    backend_key: Option<BackendKeyData>,
    server_params: Vec<(String, String)>,
    transaction_status: TransactionStatus,
    is_broken: bool,
    stmt_counter: u64,
    async_message_handler: Option<Box<dyn AsyncMessageHandler>>,
}

impl Conn {
    /// Connect to a PostgreSQL server.
    pub async fn new<O: TryInto<Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts = opts.try_into()?;

        let stream = if let Some(socket_path) = &opts.socket {
            Stream::unix(UnixStream::connect(socket_path).await?)
        } else {
            if opts.host.is_empty() {
                return Err(Error::InvalidUsage("host is empty".into()));
            }
            let addr = format!("{}:{}", opts.host, opts.port);
            let tcp = TcpStream::connect(&addr).await?;
            tcp.set_nodelay(true)?;
            Stream::tcp(tcp)
        };

        Self::new_with_stream(stream, opts).await
    }

    /// Connect using an existing stream.
    #[allow(unused_mut)]
    pub async fn new_with_stream(mut stream: Stream, options: Opts) -> Result<Self> {
        let mut buffer_set = options.buffer_pool.get_buffer_set();
        let mut state_machine = ConnectionStateMachine::new(options.clone());

        // Drive the connection state machine
        loop {
            match state_machine.step(&mut buffer_set)? {
                Action::WriteAndReadByte => {
                    stream.write_all(&buffer_set.write_buffer).await?;
                    stream.flush().await?;
                    let byte = stream.read_u8().await?;
                    state_machine.set_ssl_response(byte);
                }
                Action::ReadMessage => {
                    stream.read_message(&mut buffer_set).await?;
                }
                Action::Write => {
                    stream.write_all(&buffer_set.write_buffer).await?;
                    stream.flush().await?;
                }
                Action::WriteAndReadMessage => {
                    stream.write_all(&buffer_set.write_buffer).await?;
                    stream.flush().await?;
                    stream.read_message(&mut buffer_set).await?;
                }
                Action::TlsHandshake => {
                    #[cfg(feature = "tokio-tls")]
                    {
                        stream = stream.upgrade_to_tls(&options.host).await?;
                    }
                    #[cfg(not(feature = "tokio-tls"))]
                    {
                        return Err(Error::Unsupported(
                            "TLS requested but tokio-tls feature not enabled".into(),
                        ));
                    }
                }
                Action::HandleAsyncMessageAndReadMessage(_) => {
                    // Ignore async messages during startup, read next message
                    stream.read_message(&mut buffer_set).await?;
                }
                Action::Finished => break,
            }
        }

        let conn = Self {
            stream,
            buffer_set,
            backend_key: state_machine.backend_key().copied(),
            server_params: state_machine.take_server_params(),
            transaction_status: state_machine.transaction_status(),
            is_broken: false,
            stmt_counter: 0,
            async_message_handler: None,
        };

        // Upgrade to Unix socket if connected via TCP to loopback
        let conn = if options.prefer_unix_socket && conn.stream.is_tcp_loopback() {
            conn.try_upgrade_to_unix_socket(&options).await
        } else {
            conn
        };

        Ok(conn)
    }

    /// Try to upgrade to Unix socket connection.
    /// Returns upgraded conn on success, original conn on failure.
    fn try_upgrade_to_unix_socket(
        mut self,
        opts: &Opts,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Self> + Send + '_>> {
        let opts = opts.clone();
        Box::pin(async move {
            // Query unix_socket_directories from server
            let mut handler = FirstRowHandler::<(String,)>::new();
            if self
                .query("SHOW unix_socket_directories", &mut handler)
                .await
                .is_err()
            {
                return self;
            }

            let socket_dir = match handler.into_row() {
                Some((dirs,)) => {
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
            let unix_stream = match UnixStream::connect(&socket_path).await {
                Ok(s) => s,
                Err(_) => return self,
            };

            // Create new connection over Unix socket
            let mut opts_unix = opts.clone();
            opts_unix.prefer_unix_socket = false;

            match Self::new_with_stream(Stream::unix(unix_stream), opts_unix).await {
                Ok(new_conn) => new_conn,
                Err(_) => self,
            }
        })
    }

    /// Get the backend key data for query cancellation.
    pub fn backend_key(&self) -> Option<&BackendKeyData> {
        self.backend_key.as_ref()
    }

    /// Get the connection ID (backend process ID).
    ///
    /// Returns 0 if the backend key data is not available.
    pub fn connection_id(&self) -> u32 {
        self.backend_key.as_ref().map_or(0, |k| k.process_id())
    }

    /// Get server parameters.
    pub fn server_params(&self) -> &[(String, String)] {
        &self.server_params
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

    /// Set the async message handler.
    ///
    /// The handler is called when the server sends asynchronous messages:
    /// - `Notification` - from LISTEN/NOTIFY
    /// - `Notice` - warnings and informational messages
    /// - `ParameterChanged` - server parameter updates
    pub fn set_async_message_handler<H: AsyncMessageHandler + 'static>(&mut self, handler: H) {
        self.async_message_handler = Some(Box::new(handler));
    }

    /// Remove the async message handler.
    pub fn clear_async_message_handler(&mut self) {
        self.async_message_handler = None;
    }

    /// Ping the server with an empty query to check connection aliveness.
    pub async fn ping(&mut self) -> Result<()> {
        self.query_drop("").await?;
        Ok(())
    }

    /// Drive a state machine to completion.
    async fn drive<S: StateMachine>(&mut self, state_machine: &mut S) -> Result<()> {
        loop {
            match state_machine.step(&mut self.buffer_set)? {
                Action::WriteAndReadByte => {
                    return Err(Error::Protocol("Unexpected WriteAndReadByte in query state machine".into()));
                }
                Action::ReadMessage => {
                    self.stream.read_message(&mut self.buffer_set).await?;
                }
                Action::Write => {
                    self.stream.write_all(&self.buffer_set.write_buffer).await?;
                    self.stream.flush().await?;
                }
                Action::WriteAndReadMessage => {
                    self.stream.write_all(&self.buffer_set.write_buffer).await?;
                    self.stream.flush().await?;
                    self.stream.read_message(&mut self.buffer_set).await?;
                }
                Action::TlsHandshake => {
                    return Err(Error::Protocol("Unexpected TlsHandshake in query state machine".into()));
                }
                Action::HandleAsyncMessageAndReadMessage(ref async_msg) => {
                    if let Some(ref mut h) = self.async_message_handler {
                        h.handle(async_msg);
                    }
                    // Read next message after handling async message
                    self.stream.read_message(&mut self.buffer_set).await?;
                }
                Action::Finished => {
                    self.transaction_status = state_machine.transaction_status();
                    break;
                }
            }
        }
        Ok(())
    }

    /// Execute a simple query with a handler.
    pub async fn query<H: TextHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let result = self.query_inner(sql, handler).await;
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    async fn query_inner<H: TextHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let mut state_machine = SimpleQueryStateMachine::new(handler, sql);
        self.drive(&mut state_machine).await
    }

    /// Execute a simple query and discard results.
    pub async fn query_drop(&mut self, sql: &str) -> Result<Option<u64>> {
        let mut handler = DropHandler::new();
        self.query(sql, &mut handler).await?;
        Ok(handler.rows_affected())
    }

    /// Execute a simple query and collect typed rows.
    pub async fn query_collect<T: for<'a> crate::conversion::FromRow<'a>>(
        &mut self,
        sql: &str,
    ) -> Result<Vec<T>> {
        let mut handler = crate::handler::CollectHandler::<T>::new();
        self.query(sql, &mut handler).await?;
        Ok(handler.into_rows())
    }

    /// Execute a simple query and return the first typed row.
    pub async fn query_first<T: for<'a> crate::conversion::FromRow<'a>>(
        &mut self,
        sql: &str,
    ) -> Result<Option<T>> {
        let mut handler = crate::handler::FirstRowHandler::<T>::new();
        self.query(sql, &mut handler).await?;
        Ok(handler.into_row())
    }

    /// Close the connection gracefully.
    pub async fn close(mut self) -> Result<()> {
        self.buffer_set.write_buffer.clear();
        write_terminate(&mut self.buffer_set.write_buffer);
        self.stream.write_all(&self.buffer_set.write_buffer).await?;
        self.stream.flush().await?;
        Ok(())
    }

    // === Extended Query Protocol ===

    /// Prepare a statement using the extended query protocol.
    pub async fn prepare(&mut self, query: &str) -> Result<PreparedStatement> {
        self.prepare_typed(query, &[]).await
    }

    /// Prepare a statement with explicit parameter types.
    pub async fn prepare_typed(
        &mut self,
        query: &str,
        param_oids: &[u32],
    ) -> Result<PreparedStatement> {
        self.stmt_counter += 1;
        let idx = self.stmt_counter;
        let result = self.prepare_inner(idx, query, param_oids).await;
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    async fn prepare_inner(
        &mut self,
        idx: u64,
        query: &str,
        param_oids: &[u32],
    ) -> Result<PreparedStatement> {
        let mut handler = DropHandler::new();
        let mut state_machine = ExtendedQueryStateMachine::prepare(&mut handler, &mut self.buffer_set, idx, query, param_oids);
        self.drive(&mut state_machine).await?;
        state_machine
            .take_prepared_statement()
            .ok_or_else(|| Error::Protocol("No prepared statement".into()))
    }

    /// Execute a statement with a handler.
    ///
    /// The statement can be either:
    /// - A `&PreparedStatement` returned from `prepare()`
    /// - A raw SQL `&str` for one-shot execution
    pub async fn exec<S: IntoStatement, P: ToParams, H: BinaryHandler>(
        &mut self,
        statement: S,
        params: P,
        handler: &mut H,
    ) -> Result<()> {
        let result = self.exec_inner(&statement, &params, handler).await;
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    async fn exec_inner<S: IntoStatement, P: ToParams, H: BinaryHandler>(
        &mut self,
        statement: &S,
        params: &P,
        handler: &mut H,
    ) -> Result<()> {
        let mut state_machine = if statement.needs_parse() {
            ExtendedQueryStateMachine::execute_sql(handler, &mut self.buffer_set, statement.as_sql().unwrap(), params)
        } else {
            let stmt = statement.as_prepared().unwrap();
            ExtendedQueryStateMachine::execute(handler, &mut self.buffer_set, &stmt.wire_name(), params)
        };

        self.drive(&mut state_machine).await
    }

    /// Execute a statement and discard results.
    ///
    /// The statement can be either a `&PreparedStatement` or a raw SQL `&str`.
    pub async fn exec_drop<S: IntoStatement, P: ToParams>(
        &mut self,
        statement: S,
        params: P,
    ) -> Result<Option<u64>> {
        let mut handler = DropHandler::new();
        self.exec(statement, params, &mut handler).await?;
        Ok(handler.rows_affected())
    }

    /// Execute a statement and collect typed rows.
    ///
    /// The statement can be either a `&PreparedStatement` or a raw SQL `&str`.
    pub async fn exec_collect<T: for<'a> crate::conversion::FromRow<'a>, S: IntoStatement, P: ToParams>(
        &mut self,
        statement: S,
        params: P,
    ) -> Result<Vec<T>> {
        let mut handler = crate::handler::CollectHandler::<T>::new();
        self.exec(statement, params, &mut handler).await?;
        Ok(handler.into_rows())
    }

    /// Close a prepared statement.
    pub async fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<()> {
        let result = self.close_statement_inner(&stmt.wire_name()).await;
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    async fn close_statement_inner(&mut self, name: &str) -> Result<()> {
        let mut handler = DropHandler::new();
        let mut state_machine = ExtendedQueryStateMachine::close_statement(&mut handler, &mut self.buffer_set, name);
        self.drive(&mut state_machine).await
    }

    /// Execute a closure within a transaction.
    ///
    /// If the closure returns `Ok`, the transaction is committed.
    /// If the closure returns `Err` or the transaction is not explicitly
    /// committed or rolled back, the transaction is rolled back.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidUsage` if called while already in a transaction.
    pub async fn run_transaction<F, R, Fut>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Conn, super::transaction::Transaction) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        if self.in_transaction() {
            return Err(Error::InvalidUsage(
                "nested transactions are not supported".into(),
            ));
        }

        self.query_drop("BEGIN").await?;

        let tx = super::transaction::Transaction::new(self.connection_id());

        // We need to use unsafe to work around the borrow checker here
        // because async closures can't capture &mut self properly
        let result = f(self, tx).await;

        // If still in a transaction (not committed or rolled back), roll it back
        if self.in_transaction() {
            let rollback_result = self.query_drop("ROLLBACK").await;

            // Return the first error (either from closure or rollback)
            if let Err(e) = result {
                return Err(e);
            }
            rollback_result?;
        }

        result
    }
}

