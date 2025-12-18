//! Synchronous PostgreSQL connection.

use std::net::TcpStream;
use std::os::unix::net::UnixStream;

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
use crate::state::extended::{BindStateMachine, ExtendedQueryStateMachine, PreparedStatement};
use crate::state::simple_query::SimpleQueryStateMachine;
use crate::state::StateMachine;
use crate::statement::IntoStatement;

use super::stream::Stream;
use super::unnamed_portal::UnnamedPortal;


/// Synchronous PostgreSQL connection.
pub struct Conn {
    pub(crate) stream: Stream,
    pub(crate) buffer_set: PooledBufferSet,
    backend_key: Option<BackendKeyData>,
    server_params: Vec<(String, String)>,
    pub(crate) transaction_status: TransactionStatus,
    pub(crate) is_broken: bool,
    stmt_counter: u64,
    async_message_handler: Option<Box<dyn AsyncMessageHandler>>,
}

impl Conn {
    /// Connect to a PostgreSQL server.
    pub fn new<O: TryInto<Opts>>(opts: O) -> Result<Self>
    where
        Error: From<O::Error>,
    {
        let opts = opts.try_into()?;

        let stream = if let Some(socket_path) = &opts.socket {
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
    #[allow(unused_mut)]
    pub fn new_with_stream(mut stream: Stream, options: Opts) -> Result<Self> {
        let mut buffer_set = options.buffer_pool.get_buffer_set();
        let mut state_machine = ConnectionStateMachine::new(options.clone());

        // Drive the connection state machine
        loop {
            match state_machine.step(&mut buffer_set)? {
                Action::WriteAndReadByte => {
                    stream.write_all(&buffer_set.write_buffer)?;
                    stream.flush()?;
                    let byte = stream.read_u8()?;
                    state_machine.set_ssl_response(byte);
                }
                Action::ReadMessage => {
                    stream.read_message(&mut buffer_set)?;
                }
                Action::Write => {
                    stream.write_all(&buffer_set.write_buffer)?;
                    stream.flush()?;
                }
                Action::WriteAndReadMessage => {
                    stream.write_all(&buffer_set.write_buffer)?;
                    stream.flush()?;
                    stream.read_message(&mut buffer_set)?;
                }
                Action::TlsHandshake => {
                    #[cfg(feature = "sync-tls")]
                    {
                        stream = stream.upgrade_to_tls(&options.host)?;
                    }
                    #[cfg(not(feature = "sync-tls"))]
                    {
                        return Err(Error::Unsupported(
                            "TLS requested but sync-tls feature not enabled".into(),
                        ));
                    }
                }
                Action::HandleAsyncMessageAndReadMessage(_) => {
                    // Ignore async messages during startup, read next message
                    stream.read_message(&mut buffer_set)?;
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
        let mut handler = FirstRowHandler::<(String,)>::new();
        if self
            .query("SHOW unix_socket_directories", &mut handler)
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
        let unix_stream = match UnixStream::connect(&socket_path) {
            Ok(s) => s,
            Err(_) => return self,
        };

        // Create new connection over Unix socket
        let mut opts_unix = opts.clone();
        opts_unix.prefer_unix_socket = false;

        match Self::new_with_stream(Stream::unix(unix_stream), opts_unix) {
            Ok(new_conn) => new_conn,
            Err(_) => self,
        }
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

    /// Enter pipeline mode for batching multiple queries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut pipeline = conn.pipeline();
    ///
    /// pipeline.queue("insert_user", &[&"alice"])?;
    /// pipeline.queue("insert_user", &[&"bob"])?;
    /// pipeline.queue("get_users", &[])?;
    ///
    /// pipeline.fetch_drop()?;
    /// pipeline.fetch_drop()?;
    /// let users: Vec<User> = pipeline.fetch_collect()?;
    ///
    /// pipeline.sync()?;
    /// ```
    pub fn pipeline(&mut self) -> super::pipeline::Pipeline<'_> {
        super::pipeline::Pipeline::new(self)
    }

    /// Ping the server with an empty query to check connection aliveness.
    pub fn ping(&mut self) -> Result<()> {
        self.query_drop("")?;
        Ok(())
    }

    /// Drive a state machine to completion.
    fn drive<S: StateMachine>(&mut self, state_machine: &mut S) -> Result<()> {
        loop {
            match state_machine.step(&mut self.buffer_set)? {
                Action::WriteAndReadByte => {
                    return Err(Error::Protocol("Unexpected WriteAndReadByte in query state machine".into()));
                }
                Action::ReadMessage => {
                    self.stream.read_message(&mut self.buffer_set)?;
                }
                Action::Write => {
                    self.stream.write_all(&self.buffer_set.write_buffer)?;
                    self.stream.flush()?;
                }
                Action::WriteAndReadMessage => {
                    self.stream.write_all(&self.buffer_set.write_buffer)?;
                    self.stream.flush()?;
                    self.stream.read_message(&mut self.buffer_set)?;
                }
                Action::TlsHandshake => {
                    return Err(Error::Protocol("Unexpected TlsHandshake in query state machine".into()));
                }
                Action::HandleAsyncMessageAndReadMessage(ref async_msg) => {
                    if let Some(ref mut h) = self.async_message_handler {
                        h.handle(async_msg);
                    }
                    // Read next message after handling async message
                    self.stream.read_message(&mut self.buffer_set)?;
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
    pub fn query<H: TextHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let result = self.query_inner(sql, handler);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn query_inner<H: TextHandler>(&mut self, sql: &str, handler: &mut H) -> Result<()> {
        let mut state_machine = SimpleQueryStateMachine::new(handler, sql);
        self.drive(&mut state_machine)
    }

    /// Execute a simple query and discard results.
    pub fn query_drop(&mut self, sql: &str) -> Result<Option<u64>> {
        let mut handler = DropHandler::new();
        self.query(sql, &mut handler)?;
        Ok(handler.rows_affected())
    }

    /// Execute a simple query and collect typed rows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let rows: Vec<(i32, String)> = conn.query_typed("SELECT id, name FROM users")?;
    /// for (id, name) in rows {
    ///     println!("{}: {}", id, name);
    /// }
    /// ```
    pub fn query_collect<T: for<'a> crate::conversion::FromRow<'a>>(
        &mut self,
        sql: &str,
    ) -> Result<Vec<T>> {
        let mut handler = crate::handler::CollectHandler::<T>::new();
        self.query(sql, &mut handler)?;
        Ok(handler.into_rows())
    }

    /// Execute a simple query and return the first typed row.
    pub fn query_first<T: for<'a> crate::conversion::FromRow<'a>>(
        &mut self,
        sql: &str,
    ) -> Result<Option<T>> {
        let mut handler = crate::handler::FirstRowHandler::<T>::new();
        self.query(sql, &mut handler)?;
        Ok(handler.into_row())
    }

    /// Close the connection gracefully.
    pub fn close(mut self) -> Result<()> {
        self.buffer_set.write_buffer.clear();
        write_terminate(&mut self.buffer_set.write_buffer);
        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;
        Ok(())
    }

    // === Extended Query Protocol ===

    /// Prepare a statement using the extended query protocol.
    pub fn prepare(&mut self, query: &str) -> Result<PreparedStatement> {
        self.prepare_typed(query, &[])
    }

    /// Prepare a statement with explicit parameter types.
    pub fn prepare_typed(&mut self, query: &str, param_oids: &[u32]) -> Result<PreparedStatement> {
        self.stmt_counter += 1;
        let idx = self.stmt_counter;
        let result = self.prepare_inner(idx, query, param_oids);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn prepare_inner(
        &mut self,
        idx: u64,
        query: &str,
        param_oids: &[u32],
    ) -> Result<PreparedStatement> {
        let mut handler = DropHandler::new();
        let mut state_machine = ExtendedQueryStateMachine::prepare(&mut handler, &mut self.buffer_set, idx, query, param_oids);
        self.drive(&mut state_machine)?;
        state_machine
            .take_prepared_statement()
            .ok_or_else(|| Error::Protocol("No prepared statement".into()))
    }

    /// Execute a statement with a handler.
    ///
    /// The statement can be either:
    /// - A `&PreparedStatement` returned from `prepare()`
    /// - A raw SQL `&str` for one-shot execution
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Using prepared statement
    /// let stmt = conn.prepare("SELECT $1::int")?;
    /// conn.exec(&stmt, (42,), &mut handler)?;
    ///
    /// // Using raw SQL
    /// conn.exec("SELECT $1::int", (42,), &mut handler)?;
    /// ```
    pub fn exec<S: IntoStatement, P: ToParams, H: BinaryHandler>(
        &mut self,
        statement: S,
        params: P,
        handler: &mut H,
    ) -> Result<()> {
        let result = self.exec_inner(&statement, &params, handler);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn exec_inner<S: IntoStatement, P: ToParams, H: BinaryHandler>(
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

        self.drive(&mut state_machine)
    }

    /// Execute a statement and discard results.
    ///
    /// The statement can be either a `&PreparedStatement` or a raw SQL `&str`.
    pub fn exec_drop<S: IntoStatement, P: ToParams>(
        &mut self,
        statement: S,
        params: P,
    ) -> Result<Option<u64>> {
        let mut handler = DropHandler::new();
        self.exec(statement, params, &mut handler)?;
        Ok(handler.rows_affected())
    }

    /// Execute a statement and collect typed rows.
    ///
    /// The statement can be either a `&PreparedStatement` or a raw SQL `&str`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stmt = conn.prepare("SELECT id, name FROM users WHERE id = $1")?;
    /// let rows: Vec<(i32, String)> = conn.exec_collect(&stmt, (42,))?;
    ///
    /// // Or with raw SQL:
    /// let rows: Vec<(i32, String)> = conn.exec_collect("SELECT id, name FROM users", ())?;
    /// ```
    pub fn exec_collect<T: for<'a> crate::conversion::FromRow<'a>, S: IntoStatement, P: ToParams>(
        &mut self,
        statement: S,
        params: P,
    ) -> Result<Vec<T>> {
        let mut handler = crate::handler::CollectHandler::<T>::new();
        self.exec(statement, params, &mut handler)?;
        Ok(handler.into_rows())
    }

    /// Close a prepared statement.
    pub fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<()> {
        let result = self.close_statement_inner(&stmt.wire_name());
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn close_statement_inner(&mut self, name: &str) -> Result<()> {
        let mut handler = DropHandler::new();
        let mut state_machine = ExtendedQueryStateMachine::close_statement(&mut handler, &mut self.buffer_set, name);
        self.drive(&mut state_machine)
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
    pub fn run_transaction<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Conn, super::transaction::Transaction) -> Result<R>,
    {
        if self.in_transaction() {
            return Err(Error::InvalidUsage(
                "nested transactions are not supported".into(),
            ));
        }

        self.query_drop("BEGIN")?;

        let tx = super::transaction::Transaction::new(self.connection_id());
        let result = f(self, tx);

        // If still in a transaction (not committed or rolled back), roll it back
        if self.in_transaction() {
            let rollback_result = self.query_drop("ROLLBACK");

            // Return the first error (either from closure or rollback)
            if let Err(e) = result {
                return Err(e);
            }
            rollback_result?;
        }

        result
    }
}

// === Low-level Extended Query Protocol ===

impl Conn {
    /// Low-level bind: send BIND message and receive BindComplete.
    ///
    /// This allows creating named portals. Unlike `exec()`, this does NOT
    /// send EXECUTE or SYNC - the caller controls when to execute and sync.
    ///
    /// # Arguments
    /// - `portal`: Portal name (empty string "" for unnamed portal)
    /// - `statement_name`: Prepared statement name
    /// - `params`: Parameter values
    pub fn lowlevel_bind<P: ToParams>(
        &mut self,
        portal: &str,
        statement_name: &str,
        params: P,
    ) -> Result<()> {
        let result = self.lowlevel_bind_inner(portal, statement_name, &params);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn lowlevel_bind_inner<P: ToParams>(
        &mut self,
        portal: &str,
        statement_name: &str,
        params: &P,
    ) -> Result<()> {
        use crate::protocol::backend::{msg_type, BindComplete, ErrorResponse, RawMessage};
        use crate::protocol::frontend::{write_bind, write_flush};

        self.buffer_set.write_buffer.clear();
        write_bind(&mut self.buffer_set.write_buffer, portal, statement_name, params, &[]);
        write_flush(&mut self.buffer_set.write_buffer);

        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;

        loop {
            self.stream.read_message(&mut self.buffer_set)?;
            let type_byte = self.buffer_set.type_byte;

            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            match type_byte {
                msg_type::BIND_COMPLETE => {
                    BindComplete::parse(&self.buffer_set.read_buffer)?;
                    return Ok(());
                }
                msg_type::ERROR_RESPONSE => {
                    let error = ErrorResponse::parse(&self.buffer_set.read_buffer)?;
                    return Err(error.into_error());
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "Expected BindComplete or ErrorResponse, got '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }

    /// Low-level execute: send EXECUTE message and receive results.
    ///
    /// Executes a previously bound portal. Does NOT send SYNC.
    ///
    /// # Arguments
    /// - `portal`: Portal name (empty string "" for unnamed portal)
    /// - `max_rows`: Maximum rows to return (0 = unlimited)
    /// - `handler`: Handler to receive rows
    ///
    /// # Returns
    /// - `Ok(true)` if more rows available (PortalSuspended received)
    /// - `Ok(false)` if execution completed (CommandComplete received)
    pub fn lowlevel_execute<H: BinaryHandler>(
        &mut self,
        portal: &str,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<bool> {
        let result = self.lowlevel_execute_inner(portal, max_rows, handler);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn lowlevel_execute_inner<H: BinaryHandler>(
        &mut self,
        portal: &str,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<bool> {
        use crate::protocol::backend::{
            msg_type, CommandComplete, DataRow, ErrorResponse, NoData, PortalSuspended,
            RawMessage, RowDescription,
        };
        use crate::protocol::frontend::{write_describe_portal, write_execute, write_flush};

        self.buffer_set.write_buffer.clear();
        write_describe_portal(&mut self.buffer_set.write_buffer, portal);
        write_execute(&mut self.buffer_set.write_buffer, portal, max_rows);
        write_flush(&mut self.buffer_set.write_buffer);

        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;

        let mut column_buffer: Vec<u8> = Vec::new();

        loop {
            self.stream.read_message(&mut self.buffer_set)?;
            let type_byte = self.buffer_set.type_byte;

            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            match type_byte {
                msg_type::ROW_DESCRIPTION => {
                    column_buffer.clear();
                    column_buffer.extend_from_slice(&self.buffer_set.read_buffer);
                    let cols = RowDescription::parse(&column_buffer)?;
                    handler.result_start(cols)?;
                }
                msg_type::NO_DATA => {
                    NoData::parse(&self.buffer_set.read_buffer)?;
                }
                msg_type::DATA_ROW => {
                    let cols = RowDescription::parse(&column_buffer)?;
                    let row = DataRow::parse(&self.buffer_set.read_buffer)?;
                    handler.row(cols, row)?;
                }
                msg_type::COMMAND_COMPLETE => {
                    let complete = CommandComplete::parse(&self.buffer_set.read_buffer)?;
                    handler.result_end(complete)?;
                    return Ok(false); // No more rows
                }
                msg_type::PORTAL_SUSPENDED => {
                    PortalSuspended::parse(&self.buffer_set.read_buffer)?;
                    return Ok(true); // More rows available
                }
                msg_type::ERROR_RESPONSE => {
                    let error = ErrorResponse::parse(&self.buffer_set.read_buffer)?;
                    return Err(error.into_error());
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "Unexpected message in execute: '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }

    /// Low-level sync: send SYNC and receive ReadyForQuery.
    ///
    /// This ends an extended query sequence and:
    /// - Commits implicit transaction if successful
    /// - Rolls back implicit transaction if failed
    /// - Updates transaction status
    pub fn lowlevel_sync(&mut self) -> Result<()> {
        let result = self.lowlevel_sync_inner();
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn lowlevel_sync_inner(&mut self) -> Result<()> {
        use crate::protocol::backend::{msg_type, ErrorResponse, RawMessage, ReadyForQuery};
        use crate::protocol::frontend::write_sync;

        self.buffer_set.write_buffer.clear();
        write_sync(&mut self.buffer_set.write_buffer);

        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;

        let mut pending_error: Option<Error> = None;

        loop {
            self.stream.read_message(&mut self.buffer_set)?;
            let type_byte = self.buffer_set.type_byte;

            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            match type_byte {
                msg_type::READY_FOR_QUERY => {
                    let ready = ReadyForQuery::parse(&self.buffer_set.read_buffer)?;
                    self.transaction_status = ready.transaction_status().unwrap_or_default();
                    if let Some(e) = pending_error {
                        return Err(e);
                    }
                    return Ok(());
                }
                msg_type::ERROR_RESPONSE => {
                    let error = ErrorResponse::parse(&self.buffer_set.read_buffer)?;
                    pending_error = Some(error.into_error());
                }
                _ => {
                    // Ignore other messages before ReadyForQuery
                }
            }
        }
    }

    /// Low-level flush: send FLUSH to force server to send pending responses.
    ///
    /// Unlike SYNC, FLUSH does not end the transaction or wait for ReadyForQuery.
    /// It just forces the server to send any pending responses (like ParseComplete,
    /// BindComplete, RowDescription, DataRow, etc.) without ending the extended
    /// query sequence.
    pub fn lowlevel_flush(&mut self) -> Result<()> {
        use crate::protocol::frontend::write_flush;

        self.buffer_set.write_buffer.clear();
        write_flush(&mut self.buffer_set.write_buffer);

        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;
        Ok(())
    }

    /// Execute a statement with iterative row fetching.
    ///
    /// Creates an unnamed portal and passes it to the closure. The closure can
    /// call `portal.fetch(n, handler)` multiple times to retrieve rows in batches.
    /// Sync is called after the closure returns to end the implicit transaction.
    ///
    /// The statement can be either:
    /// - A `&PreparedStatement` returned from `prepare()`
    /// - A raw SQL `&str` for one-shot execution
    ///
    /// # Example
    /// ```ignore
    /// // Using prepared statement
    /// let stmt = conn.prepare("SELECT * FROM users")?;
    /// conn.exec_iter(&stmt, (), |portal| {
    ///     while portal.fetch(100, &mut handler)? {
    ///         // process handler.into_rows()...
    ///     }
    ///     Ok(())
    /// })?;
    ///
    /// // Using raw SQL
    /// conn.exec_iter("SELECT * FROM users", (), |portal| {
    ///     while portal.fetch(100, &mut handler)? {
    ///         // process handler.into_rows()...
    ///     }
    ///     Ok(())
    /// })?;
    /// ```
    pub fn exec_iter<S: IntoStatement, P, F, T>(
        &mut self,
        statement: S,
        params: P,
        f: F,
    ) -> Result<T>
    where
        P: ToParams,
        F: FnOnce(&mut UnnamedPortal<'_>) -> Result<T>,
    {
        let result = self.exec_iter_inner(&statement, &params, f);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn exec_iter_inner<S: IntoStatement, P, F, T>(
        &mut self,
        statement: &S,
        params: &P,
        f: F,
    ) -> Result<T>
    where
        P: ToParams,
        F: FnOnce(&mut UnnamedPortal<'_>) -> Result<T>,
    {
        // Create bind state machine
        let mut state_machine = if let Some(sql) = statement.as_sql() {
            BindStateMachine::bind_sql(&mut self.buffer_set, sql, params)
        } else {
            let stmt = statement.as_prepared().unwrap();
            BindStateMachine::bind_prepared(&mut self.buffer_set, &stmt.wire_name(), params)
        };

        // Drive the state machine to completion (ParseComplete + BindComplete)
        loop {
            match state_machine.step(&mut self.buffer_set)? {
                Action::ReadMessage => {
                    self.stream.read_message(&mut self.buffer_set)?;
                }
                Action::Write => {
                    self.stream.write_all(&self.buffer_set.write_buffer)?;
                    self.stream.flush()?;
                }
                Action::WriteAndReadMessage => {
                    self.stream.write_all(&self.buffer_set.write_buffer)?;
                    self.stream.flush()?;
                    self.stream.read_message(&mut self.buffer_set)?;
                }
                Action::Finished => break,
                _ => return Err(Error::Protocol("Unexpected action in bind".into())),
            }
        }

        // Execute closure with portal handle
        let mut portal = UnnamedPortal { conn: self };
        let result = f(&mut portal);

        // Always sync to end implicit transaction (even on error)
        let sync_result = portal.conn.lowlevel_sync();

        // Return closure result, or sync error if closure succeeded but sync failed
        match (result, sync_result) {
            (Ok(v), Ok(())) => Ok(v),
            (Err(e), _) => Err(e),
            (Ok(_), Err(e)) => Err(e),
        }
    }

    /// Low-level close portal: send Close(Portal) and receive CloseComplete.
    pub fn lowlevel_close_portal(&mut self, portal: &str) -> Result<()> {
        let result = self.lowlevel_close_portal_inner(portal);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.is_broken = true;
        }
        result
    }

    fn lowlevel_close_portal_inner(&mut self, portal: &str) -> Result<()> {
        use crate::protocol::backend::{msg_type, CloseComplete, ErrorResponse, RawMessage};
        use crate::protocol::frontend::{write_close_portal, write_flush};

        self.buffer_set.write_buffer.clear();
        write_close_portal(&mut self.buffer_set.write_buffer, portal);
        write_flush(&mut self.buffer_set.write_buffer);

        self.stream.write_all(&self.buffer_set.write_buffer)?;
        self.stream.flush()?;

        loop {
            self.stream.read_message(&mut self.buffer_set)?;
            let type_byte = self.buffer_set.type_byte;

            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            match type_byte {
                msg_type::CLOSE_COMPLETE => {
                    CloseComplete::parse(&self.buffer_set.read_buffer)?;
                    return Ok(());
                }
                msg_type::ERROR_RESPONSE => {
                    let error = ErrorResponse::parse(&self.buffer_set.read_buffer)?;
                    return Err(error.into_error());
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "Expected CloseComplete or ErrorResponse, got '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        // Try to send Terminate message, ignore errors
        self.buffer_set.write_buffer.clear();
        write_terminate(&mut self.buffer_set.write_buffer);
        let _ = self.stream.write_all(&self.buffer_set.write_buffer);
        let _ = self.stream.flush();
    }
}

