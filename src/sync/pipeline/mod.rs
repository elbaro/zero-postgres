//! Pipeline mode for batching multiple queries.
//!
//! Pipeline mode allows sending multiple queries to the server without waiting
//! for responses, reducing round-trip latency. This implementation provides
//! typed handles for compile-time safety and supports:
//!
//! - Preparing statements within the pipeline
//! - Creating named portals for cursor-style iteration
//! - Multiple executions on the same portal with row limits
//!
//! # Example
//!
//! ```ignore
//! let mut p = conn.pipeline();
//!
//! // Prepare a statement in the pipeline
//! let prep = p.prepare("SELECT id, name FROM users WHERE active = $1")?;
//!
//! // Execute it with different parameters
//! let q1 = p.exec::<(i32, String), _>(&prep, (true,))?;
//! let q2 = p.exec::<(i32, String), _>(&prep, (false,))?;
//!
//! p.sync()?;
//!
//! // Harvest results in order
//! let stmt = p.harvest(prep)?;
//! let active: ExecResult<(i32, String)> = p.harvest(q1)?;
//! let inactive: ExecResult<(i32, String)> = p.harvest(q2)?;
//! ```
//!
//! # Portal Example (Cursor-style)
//!
//! ```ignore
//! let mut p = conn.pipeline();
//!
//! let prep = p.prepare("SELECT * FROM big_table")?;
//! let portal = p.bind(&prep, ())?;
//! let batch1 = p.execute::<Row>(&portal, 100)?;  // max 100 rows
//! let batch2 = p.execute::<Row>(&portal, 100)?;  // next 100 rows
//!
//! p.sync()?;
//!
//! let _stmt = p.harvest(prep)?;
//! p.harvest(portal)?;
//! let result1 = p.harvest(batch1)?;
//! let result2 = p.harvest(batch2)?;
//!
//! if result1.suspended {
//!     // More rows were available after batch1
//! }
//! ```

mod handles;

pub use handles::{ExecResult, QueuedExec, QueuedPortal, QueuedPrepare};

use std::marker::PhantomData;

use crate::conversion::{FromRow, ToParams};
use crate::error::{Error, Result};
use crate::protocol::backend::{
    BindComplete, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, NoData,
    ParameterDescription, ParseComplete, PortalSuspended, RawMessage, ReadyForQuery,
    RowDescription, msg_type,
};
use crate::protocol::frontend::{
    write_bind, write_describe_portal, write_describe_statement, write_execute, write_flush,
    write_parse, write_sync,
};
use crate::protocol::types::Oid;
use crate::state::extended::PreparedStatement;

use super::conn::Conn;

/// What response sequence to expect for a queued operation.
#[derive(Debug, Clone, Copy)]
enum Expectation {
    /// Parse + Describe(S): ParseComplete + ParameterDescription + RowDescription/NoData
    Prepare,
    /// Bind: BindComplete
    BindPortal,
    /// Bind + Execute: BindComplete + RowDescription/NoData + DataRow* + terminal
    BindExecute,
    /// Execute only: RowDescription (first time) + DataRow* + terminal
    Execute { first: bool },
}

/// Pipeline mode for batching multiple queries.
///
/// Created by [`Conn::pipeline`].
pub struct Pipeline<'a> {
    conn: &'a mut Conn,
    /// Monotonically increasing counter for queued operations
    queue_seq: usize,
    /// Next sequence number to harvest
    harvest_seq: usize,
    /// Expected responses for each queued operation
    expectations: Vec<Expectation>,
    /// Counter for generating unique statement names
    stmt_counter: usize,
    /// Counter for generating unique portal names
    portal_counter: usize,
    /// Whether we have queued data that needs to be flushed
    needs_flush: bool,
    /// Whether the pipeline is in aborted state (error occurred)
    aborted: bool,
    /// Buffer for column descriptions during row processing
    column_buffer: Vec<u8>,
    /// Captured ParameterDescription OIDs during prepare harvest
    param_oids: Vec<Oid>,
    /// Captured RowDescription payload during prepare harvest
    row_desc_payload: Option<Vec<u8>>,
}

impl<'a> Pipeline<'a> {
    /// Create a new pipeline.
    pub(crate) fn new(conn: &'a mut Conn) -> Self {
        Self {
            conn,
            queue_seq: 0,
            harvest_seq: 0,
            expectations: Vec::new(),
            stmt_counter: 0,
            portal_counter: 0,
            needs_flush: false,
            aborted: false,
            column_buffer: Vec::new(),
            param_oids: Vec::new(),
            row_desc_payload: None,
        }
    }

    /// Generate a unique statement name for this pipeline.
    fn next_stmt_name(&mut self) -> String {
        self.stmt_counter += 1;
        format!("_zero_pipe_{}", self.stmt_counter)
    }

    /// Generate a unique portal name for this pipeline.
    fn next_portal_name(&mut self) -> String {
        self.portal_counter += 1;
        format!("_zero_portal_{}", self.portal_counter)
    }

    // ========================================================================
    // Queue Operations
    // ========================================================================

    /// Queue a Parse + Describe to prepare a statement.
    ///
    /// Returns a handle that can be:
    /// - Passed to [`exec`](Self::exec) for immediate execution
    /// - Passed to [`bind`](Self::bind) to create a named portal
    /// - Harvested to get a reusable [`PreparedStatement`]
    pub fn prepare(&mut self, sql: &str) -> Result<QueuedPrepare> {
        let stmt_name = self.next_stmt_name();
        let seq = self.queue_seq;
        self.queue_seq += 1;

        let result = self.prepare_inner(&stmt_name, sql);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result?;

        self.expectations.push(Expectation::Prepare);
        Ok(QueuedPrepare { seq, stmt_name })
    }

    fn prepare_inner(&mut self, stmt_name: &str, sql: &str) -> Result<()> {
        self.conn.buffer_set.write_buffer.clear();
        write_parse(&mut self.conn.buffer_set.write_buffer, stmt_name, sql, &[]);
        write_describe_statement(&mut self.conn.buffer_set.write_buffer, stmt_name);
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)?;
        self.needs_flush = true;
        Ok(())
    }

    /// Queue a Bind to create a named portal from a prepared statement.
    ///
    /// The portal can be executed multiple times with [`execute`](Self::execute),
    /// allowing cursor-style iteration with row limits.
    pub fn bind<P: ToParams>(&mut self, stmt: &QueuedPrepare, params: P) -> Result<QueuedPortal> {
        let portal_name = self.next_portal_name();
        let seq = self.queue_seq;
        self.queue_seq += 1;

        let result = self.bind_inner(&portal_name, &stmt.stmt_name, &params);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result?;

        self.expectations.push(Expectation::BindPortal);
        Ok(QueuedPortal {
            seq,
            portal_name,
            first_execute_done: std::cell::Cell::new(false),
        })
    }

    fn bind_inner<P: ToParams>(
        &mut self,
        portal_name: &str,
        stmt_name: &str,
        params: &P,
    ) -> Result<()> {
        let param_oids = params.natural_oids();
        self.conn.buffer_set.write_buffer.clear();
        write_bind(
            &mut self.conn.buffer_set.write_buffer,
            portal_name,
            stmt_name,
            params,
            &param_oids,
            &[],
        )?;
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)?;
        self.needs_flush = true;
        Ok(())
    }

    /// Queue an Execute on a named portal.
    ///
    /// - `max_rows`: Maximum number of rows to return. Use 0 for unlimited.
    ///
    /// If `max_rows` is reached, the result will have `suspended = true`,
    /// and you can call `execute` again on the same portal to get more rows.
    pub fn execute<T>(&mut self, portal: &QueuedPortal, max_rows: u32) -> Result<QueuedExec<T>>
    where
        T: for<'b> FromRow<'b>,
    {
        let seq = self.queue_seq;
        self.queue_seq += 1;

        let first_execute = !portal.first_execute_done.get();
        portal.first_execute_done.set(true);

        let result = self.execute_inner(&portal.portal_name, max_rows, first_execute);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result?;

        self.expectations.push(Expectation::Execute {
            first: first_execute,
        });
        Ok(QueuedExec {
            seq,
            _phantom: PhantomData,
        })
    }

    fn execute_inner(
        &mut self,
        portal_name: &str,
        max_rows: u32,
        first_execute: bool,
    ) -> Result<()> {
        self.conn.buffer_set.write_buffer.clear();
        // On first execute, we need to Describe the portal to get RowDescription
        if first_execute {
            write_describe_portal(&mut self.conn.buffer_set.write_buffer, portal_name);
        }
        write_execute(
            &mut self.conn.buffer_set.write_buffer,
            portal_name,
            max_rows,
        );
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)?;
        self.needs_flush = true;
        Ok(())
    }

    /// Queue a Bind + Execute for immediate execution (convenience method).
    ///
    /// This is equivalent to calling [`bind`](Self::bind) followed by
    /// [`execute`](Self::execute) with `max_rows = 0`, but uses an unnamed
    /// portal and is more efficient for one-shot queries.
    pub fn exec<T, P>(&mut self, stmt: &QueuedPrepare, params: P) -> Result<QueuedExec<T>>
    where
        T: for<'b> FromRow<'b>,
        P: ToParams,
    {
        let seq = self.queue_seq;
        self.queue_seq += 1;

        let result = self.exec_inner(&stmt.stmt_name, &params);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result?;

        self.expectations.push(Expectation::BindExecute);
        Ok(QueuedExec {
            seq,
            _phantom: PhantomData,
        })
    }

    fn exec_inner<P: ToParams>(&mut self, stmt_name: &str, params: &P) -> Result<()> {
        let param_oids = params.natural_oids();
        self.conn.buffer_set.write_buffer.clear();
        write_bind(
            &mut self.conn.buffer_set.write_buffer,
            "",
            stmt_name,
            params,
            &param_oids,
            &[],
        )?;
        write_describe_portal(&mut self.conn.buffer_set.write_buffer, ""); // Get RowDescription
        write_execute(&mut self.conn.buffer_set.write_buffer, "", 0);
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)?;
        self.needs_flush = true;
        Ok(())
    }

    /// Send a FLUSH message to trigger server response.
    ///
    /// This forces the server to send all pending responses without establishing
    /// a transaction boundary. Called automatically by harvest methods when needed.
    pub fn flush(&mut self) -> Result<()> {
        if self.needs_flush {
            self.conn.buffer_set.write_buffer.clear();
            write_flush(&mut self.conn.buffer_set.write_buffer);
            self.conn
                .stream
                .write_all(&self.conn.buffer_set.write_buffer)?;
            self.conn.stream.flush()?;
            self.needs_flush = false;
        }
        Ok(())
    }

    /// Send a SYNC message to establish a transaction boundary.
    ///
    /// After calling sync, you must harvest all queued operations in order.
    /// The final ReadyForQuery message will be consumed when all operations
    /// are harvested.
    pub fn sync(&mut self) -> Result<()> {
        let result = self.sync_inner();
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result
    }

    fn sync_inner(&mut self) -> Result<()> {
        self.conn.buffer_set.write_buffer.clear();
        write_sync(&mut self.conn.buffer_set.write_buffer);
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)?;
        self.conn.stream.flush()?;
        self.needs_flush = false;
        Ok(())
    }

    /// Wait for ReadyForQuery after all operations are harvested.
    fn finish(&mut self) -> Result<()> {
        // Wait for ReadyForQuery
        loop {
            self.conn.stream.read_message(&mut self.conn.buffer_set)?;
            let type_byte = self.conn.buffer_set.type_byte;

            // Handle async messages
            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            // Handle error
            if type_byte == msg_type::ERROR_RESPONSE {
                let error = ErrorResponse::parse(&self.conn.buffer_set.read_buffer)?;
                return Err(error.into_error());
            }

            if type_byte == msg_type::READY_FOR_QUERY {
                let ready = ReadyForQuery::parse(&self.conn.buffer_set.read_buffer)?;
                self.conn.transaction_status = ready.transaction_status().unwrap_or_default();
                // Reset pipeline state
                self.queue_seq = 0;
                self.harvest_seq = 0;
                self.expectations.clear();
                self.aborted = false;
                return Ok(());
            }
        }
    }

    // ========================================================================
    // Harvest Operations
    // ========================================================================

    /// Harvest the result of a queued operation.
    ///
    /// Results must be harvested in the same order they were queued.
    pub fn harvest<H: Harvest>(&mut self, handle: H) -> Result<H::Output> {
        handle.harvest(self)
    }

    /// Check that the handle sequence matches the expected harvest sequence.
    fn check_sequence(&self, seq: usize) -> Result<()> {
        if seq != self.harvest_seq {
            return Err(Error::InvalidUsage(format!(
                "harvest out of order: expected seq {}, got {}",
                self.harvest_seq, seq
            )));
        }
        Ok(())
    }

    /// Check if all operations are harvested and consume ReadyForQuery if so.
    fn maybe_finish(&mut self) -> Result<()> {
        if self.harvest_seq == self.queue_seq {
            self.finish()?;
        }
        Ok(())
    }

    /// Harvest a prepare operation.
    fn harvest_prepare(&mut self, handle: QueuedPrepare) -> Result<PreparedStatement> {
        self.check_sequence(handle.seq)?;
        self.flush()?;

        if self.aborted {
            self.harvest_seq += 1;
            self.maybe_finish()?;
            return Err(Error::Protocol(
                "pipeline aborted due to earlier error".into(),
            ));
        }

        let result = self.harvest_prepare_inner(&handle.stmt_name);
        if let Err(e) = &result {
            if e.is_connection_broken() {
                self.conn.is_broken = true;
            }
            self.aborted = true;
        }
        self.harvest_seq += 1;
        self.maybe_finish()?;
        result
    }

    fn harvest_prepare_inner(&mut self, stmt_name: &str) -> Result<PreparedStatement> {
        // Expect: ParseComplete
        self.read_next_message()?;
        if self.conn.buffer_set.type_byte != msg_type::PARSE_COMPLETE {
            return self.unexpected_message("ParseComplete");
        }
        ParseComplete::parse(&self.conn.buffer_set.read_buffer)?;

        // Expect: ParameterDescription
        self.read_next_message()?;
        if self.conn.buffer_set.type_byte != msg_type::PARAMETER_DESCRIPTION {
            return self.unexpected_message("ParameterDescription");
        }
        let param_desc = ParameterDescription::parse(&self.conn.buffer_set.read_buffer)?;
        self.param_oids = param_desc.oids().to_vec();

        // Expect: RowDescription or NoData
        self.read_next_message()?;
        match self.conn.buffer_set.type_byte {
            msg_type::ROW_DESCRIPTION => {
                self.row_desc_payload = Some(self.conn.buffer_set.read_buffer.clone());
            }
            msg_type::NO_DATA => {
                NoData::parse(&self.conn.buffer_set.read_buffer)?;
                self.row_desc_payload = None;
            }
            _ => return self.unexpected_message("RowDescription or NoData"),
        }

        // Create PreparedStatement
        // We need to give it a unique idx - use a hash of the statement name
        let idx = stmt_name
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));

        Ok(PreparedStatement::new(
            idx,
            std::mem::take(&mut self.param_oids),
            self.row_desc_payload.take(),
            stmt_name.to_string(),
        ))
    }

    /// Harvest a portal bind operation.
    fn harvest_portal(&mut self, handle: QueuedPortal) -> Result<()> {
        self.check_sequence(handle.seq)?;
        self.flush()?;

        if self.aborted {
            self.harvest_seq += 1;
            self.maybe_finish()?;
            return Err(Error::Protocol(
                "pipeline aborted due to earlier error".into(),
            ));
        }

        let result = self.harvest_portal_inner();
        if let Err(e) = &result {
            if e.is_connection_broken() {
                self.conn.is_broken = true;
            }
            self.aborted = true;
        }
        self.harvest_seq += 1;
        self.maybe_finish()?;
        result
    }

    fn harvest_portal_inner(&mut self) -> Result<()> {
        // Expect: BindComplete
        self.read_next_message()?;
        if self.conn.buffer_set.type_byte != msg_type::BIND_COMPLETE {
            return self.unexpected_message("BindComplete");
        }
        BindComplete::parse(&self.conn.buffer_set.read_buffer)?;
        Ok(())
    }

    /// Harvest an execute operation.
    fn harvest_exec<T: for<'b> FromRow<'b>>(
        &mut self,
        handle: QueuedExec<T>,
    ) -> Result<ExecResult<T>> {
        self.check_sequence(handle.seq)?;
        self.flush()?;

        if self.aborted {
            self.harvest_seq += 1;
            self.maybe_finish()?;
            return Err(Error::Protocol(
                "pipeline aborted due to earlier error".into(),
            ));
        }

        // Check if this is a BindExecute (includes BindComplete) or just Execute
        let expectation = self.expectations.get(handle.seq).copied();

        let result = match expectation {
            Some(Expectation::BindExecute) => self.harvest_bind_exec_inner(),
            Some(Expectation::Execute { first }) => self.harvest_execute_inner(first),
            _ => Err(Error::Protocol("unexpected expectation type".into())),
        };

        if let Err(e) = &result {
            if e.is_connection_broken() {
                self.conn.is_broken = true;
            }
            self.aborted = true;
        }
        self.harvest_seq += 1;
        self.maybe_finish()?;
        result
    }

    /// Harvest Bind + Execute (for exec() calls).
    fn harvest_bind_exec_inner<T: for<'b> FromRow<'b>>(&mut self) -> Result<ExecResult<T>> {
        // Expect: BindComplete
        self.read_next_message()?;
        if self.conn.buffer_set.type_byte != msg_type::BIND_COMPLETE {
            return self.unexpected_message("BindComplete");
        }
        BindComplete::parse(&self.conn.buffer_set.read_buffer)?;

        // Now read rows
        self.harvest_rows_inner()
    }

    /// Harvest Execute only (for execute() calls on named portals).
    fn harvest_execute_inner<T: for<'b> FromRow<'b>>(
        &mut self,
        first: bool,
    ) -> Result<ExecResult<T>> {
        // For named portals, server sends RowDescription on first execute
        // Subsequent executes don't get RowDescription
        self.harvest_rows_inner_with_row_desc(first)
    }

    /// Common row harvesting logic (expects RowDescription).
    fn harvest_rows_inner<T: for<'b> FromRow<'b>>(&mut self) -> Result<ExecResult<T>> {
        self.harvest_rows_inner_with_row_desc(true)
    }

    /// Row harvesting logic with optional RowDescription expectation.
    ///
    /// - `expect_row_desc`: If true, expects RowDescription/NoData before data rows.
    ///   If false, uses cached column_buffer (for subsequent portal executes).
    fn harvest_rows_inner_with_row_desc<T: for<'b> FromRow<'b>>(
        &mut self,
        expect_row_desc: bool,
    ) -> Result<ExecResult<T>> {
        let mut rows = Vec::new();

        // If we expect RowDescription, read it first
        if expect_row_desc {
            self.read_next_message()?;
            match self.conn.buffer_set.type_byte {
                msg_type::ROW_DESCRIPTION => {
                    self.column_buffer.clear();
                    self.column_buffer
                        .extend_from_slice(&self.conn.buffer_set.read_buffer);
                }
                msg_type::NO_DATA => {
                    NoData::parse(&self.conn.buffer_set.read_buffer)?;
                    // No rows will follow, but we still need terminal message
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "expected RowDescription or NoData, got '{}'",
                        self.conn.buffer_set.type_byte as char
                    )));
                }
            }
        }

        // Now read data rows until terminal message
        loop {
            self.read_next_message()?;
            let type_byte = self.conn.buffer_set.type_byte;

            match type_byte {
                msg_type::DATA_ROW => {
                    let cols = RowDescription::parse(&self.column_buffer)?;
                    let row = DataRow::parse(&self.conn.buffer_set.read_buffer)?;
                    rows.push(T::from_row(cols.fields(), row)?);
                }
                msg_type::COMMAND_COMPLETE => {
                    CommandComplete::parse(&self.conn.buffer_set.read_buffer)?;
                    return Ok(ExecResult::new(rows, false));
                }
                msg_type::EMPTY_QUERY_RESPONSE => {
                    EmptyQueryResponse::parse(&self.conn.buffer_set.read_buffer)?;
                    return Ok(ExecResult::new(rows, false));
                }
                msg_type::PORTAL_SUSPENDED => {
                    PortalSuspended::parse(&self.conn.buffer_set.read_buffer)?;
                    return Ok(ExecResult::new(rows, true));
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "unexpected message type in pipeline harvest: '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }

    /// Read the next message, skipping async messages and handling errors.
    fn read_next_message(&mut self) -> Result<()> {
        loop {
            self.conn.stream.read_message(&mut self.conn.buffer_set)?;
            let type_byte = self.conn.buffer_set.type_byte;

            // Handle async messages
            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            // Handle error
            if type_byte == msg_type::ERROR_RESPONSE {
                let error = ErrorResponse::parse(&self.conn.buffer_set.read_buffer)?;
                return Err(error.into_error());
            }

            return Ok(());
        }
    }

    /// Create an error for unexpected message type.
    fn unexpected_message<T>(&self, expected: &str) -> Result<T> {
        Err(Error::Protocol(format!(
            "expected {}, got '{}'",
            expected, self.conn.buffer_set.type_byte as char
        )))
    }

    /// Returns the number of operations that have been queued but not yet harvested.
    pub fn pending_count(&self) -> usize {
        self.queue_seq - self.harvest_seq
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }
}

// ============================================================================
// Harvest Trait
// ============================================================================

/// Trait for harvesting results from pipeline handles.
pub trait Harvest {
    /// The output type when harvesting this handle.
    type Output;

    /// Harvest the result from the pipeline.
    fn harvest(self, pipeline: &mut Pipeline<'_>) -> Result<Self::Output>;
}

impl Harvest for QueuedPrepare {
    type Output = PreparedStatement;

    fn harvest(self, pipeline: &mut Pipeline<'_>) -> Result<Self::Output> {
        pipeline.harvest_prepare(self)
    }
}

impl Harvest for QueuedPortal {
    type Output = ();

    fn harvest(self, pipeline: &mut Pipeline<'_>) -> Result<Self::Output> {
        pipeline.harvest_portal(self)
    }
}

impl<T: for<'b> FromRow<'b>> Harvest for QueuedExec<T> {
    type Output = ExecResult<T>;

    fn harvest(self, pipeline: &mut Pipeline<'_>) -> Result<Self::Output> {
        pipeline.harvest_exec(self)
    }
}
