//! Async pipeline mode for batching multiple queries.
//!
//! Pipeline mode allows sending multiple queries to the server without waiting
//! for responses, reducing round-trip latency.
//!
//! # Example
//!
//! ```ignore
//! // Prepare statements outside the pipeline
//! let stmts = conn.prepare_batch(&[
//!     "SELECT id, name FROM users WHERE active = $1",
//!     "INSERT INTO users (name) VALUES ($1) RETURNING id",
//! ]).await?;
//!
//! let (active, inactive, count) = conn.run_pipeline(|p| async move {
//!     // Queue executions
//!     let t1 = p.exec(&stmts[0], (true,)).await?;
//!     let t2 = p.exec(&stmts[0], (false,)).await?;
//!     let t3 = p.exec("SELECT COUNT(*) FROM users", ()).await?;
//!
//!     p.sync().await?;
//!
//!     // Claim results in order with different methods
//!     let active: Vec<(i32, String)> = p.claim_collect(t1).await?;
//!     let inactive: Option<(i32, String)> = p.claim_one(t2).await?;
//!     let count: Vec<(i64,)> = p.claim_collect(t3).await?;
//!
//!     Ok((active, inactive, count))
//! }).await?;
//! ```

pub use crate::pipeline::Ticket;
use crate::pipeline::Expectation;

use crate::conversion::{FromRow, ToParams};
use crate::error::{Error, Result};
use crate::handler::BinaryHandler;
use crate::protocol::backend::{
    BindComplete, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, NoData,
    ParseComplete, RawMessage, ReadyForQuery, RowDescription, msg_type,
};
use crate::protocol::frontend::{
    write_bind, write_describe_portal, write_execute, write_flush, write_parse, write_sync,
};
use crate::statement::IntoStatement;

use super::conn::Conn;

/// Async pipeline mode for batching multiple queries.
///
/// Created by [`Conn::run_pipeline`].
pub struct Pipeline<'a> {
    conn: &'a mut Conn,
    /// Monotonically increasing counter for queued operations
    queue_seq: usize,
    /// Next sequence number to claim
    claim_seq: usize,
    /// Whether we have queued data that needs to be flushed
    needs_flush: bool,
    /// Whether the pipeline is in aborted state (error occurred)
    aborted: bool,
    /// Buffer for column descriptions during row processing
    column_buffer: Vec<u8>,
    /// Expected responses for each queued operation
    expectations: Vec<Expectation>,
}

impl<'a> Pipeline<'a> {
    /// Create a new pipeline.
    ///
    /// Prefer using [`Conn::run_pipeline`] which handles cleanup automatically.
    /// This constructor is available for advanced use cases.
    #[cfg(feature = "lowlevel")]
    pub fn new(conn: &'a mut Conn) -> Self {
        Self::new_inner(conn)
    }

    /// Create a new pipeline (internal).
    pub(crate) fn new_inner(conn: &'a mut Conn) -> Self {
        Self {
            conn,
            queue_seq: 0,
            claim_seq: 0,
            needs_flush: false,
            aborted: false,
            column_buffer: Vec::new(),
            expectations: Vec::new(),
        }
    }

    /// Cleanup the pipeline, draining any unclaimed tickets.
    ///
    /// This is called automatically by [`Conn::run_pipeline`].
    pub(crate) async fn cleanup(&mut self) {
        if self.queue_seq == self.claim_seq {
            return;
        }

        // Send sync if we have pending operations
        if self.needs_flush {
            let _ = self.sync().await;
        }

        // Drain remaining tickets
        while self.claim_seq < self.queue_seq {
            let _ = self.drain_one().await;
            self.claim_seq += 1;
        }

        // Consume ReadyForQuery
        let _ = self.finish().await;
    }

    /// Drain one ticket's worth of messages.
    async fn drain_one(&mut self) {
        let Some(expectation) = self.expectations.get(self.claim_seq).copied() else {
            return;
        };
        let mut handler = crate::handler::DropHandler::new();

        let _ = match expectation {
            Expectation::ParseBindExecute => {
                self.claim_parse_bind_exec_inner(&mut handler).await
            }
            Expectation::BindExecute => self.claim_bind_exec_inner(&mut handler).await,
        };
    }

    // ========================================================================
    // Queue Operations
    // ========================================================================

    /// Queue a statement execution.
    ///
    /// The statement can be either:
    /// - A `&PreparedStatement` returned from `conn.prepare()` or `conn.prepare_batch()`
    /// - A raw SQL `&str` for one-shot execution
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stmt = conn.prepare("SELECT id, name FROM users WHERE id = $1").await?;
    ///
    /// let (r1, r2) = conn.run_pipeline(|p| async move {
    ///     let t1 = p.exec(&stmt, (1,)).await?;
    ///     let t2 = p.exec("SELECT COUNT(*) FROM users", ()).await?;
    ///     p.sync().await?;
    ///
    ///     let r1: Vec<(i32, String)> = p.claim_collect(t1).await?;
    ///     let r2: Option<(i64,)> = p.claim_one(t2).await?;
    ///     Ok((r1, r2))
    /// }).await?;
    /// ```
    pub async fn exec<P: ToParams>(
        &mut self,
        statement: impl IntoStatement,
        params: P,
    ) -> Result<Ticket> {
        let seq = self.queue_seq;
        self.queue_seq += 1;

        if statement.needs_parse() {
            self.exec_sql_inner(statement.as_sql().unwrap(), &params)
                .await?;
        } else {
            let stmt = statement.as_prepared().unwrap();
            self.exec_prepared_inner(&stmt.wire_name(), &stmt.param_oids, &params)
                .await?;
        };

        Ok(Ticket { seq })
    }

    async fn exec_sql_inner<P: ToParams>(&mut self, sql: &str, params: &P) -> Result<()> {
        let param_oids = params.natural_oids();
        self.conn.buffer_set.write_buffer.clear();
        write_parse(&mut self.conn.buffer_set.write_buffer, "", sql, &param_oids);
        write_bind(
            &mut self.conn.buffer_set.write_buffer,
            "",
            "",
            params,
            &param_oids,
        )?;
        write_describe_portal(&mut self.conn.buffer_set.write_buffer, "");
        write_execute(&mut self.conn.buffer_set.write_buffer, "", 0);
        if let Err(e) = self
            .conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)
            .await
        {
            self.conn.is_broken = true;
            return Err(e.into());
        }
        self.needs_flush = true;
        self.expectations.push(Expectation::ParseBindExecute);
        Ok(())
    }

    async fn exec_prepared_inner<P: ToParams>(
        &mut self,
        stmt_name: &str,
        param_oids: &[u32],
        params: &P,
    ) -> Result<()> {
        self.conn.buffer_set.write_buffer.clear();
        write_bind(
            &mut self.conn.buffer_set.write_buffer,
            "",
            stmt_name,
            params,
            param_oids,
        )?;
        write_describe_portal(&mut self.conn.buffer_set.write_buffer, "");
        write_execute(&mut self.conn.buffer_set.write_buffer, "", 0);
        if let Err(e) = self
            .conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)
            .await
        {
            self.conn.is_broken = true;
            return Err(e.into());
        }
        self.needs_flush = true;
        self.expectations.push(Expectation::BindExecute);
        Ok(())
    }

    /// Send a FLUSH message to trigger server response.
    ///
    /// This forces the server to send all pending responses without establishing
    /// a transaction boundary. Called automatically by claim methods when needed.
    pub async fn flush(&mut self) -> Result<()> {
        if self.needs_flush {
            self.conn.buffer_set.write_buffer.clear();
            write_flush(&mut self.conn.buffer_set.write_buffer);
            self.conn
                .stream
                .write_all(&self.conn.buffer_set.write_buffer)
                .await?;
            self.conn.stream.flush().await?;
            self.needs_flush = false;
        }
        Ok(())
    }

    /// Send a SYNC message to establish a transaction boundary.
    ///
    /// After calling sync, you must claim all queued operations in order.
    /// The final ReadyForQuery message will be consumed when all operations
    /// are claimed.
    pub async fn sync(&mut self) -> Result<()> {
        let result = self.sync_inner().await;
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result
    }

    async fn sync_inner(&mut self) -> Result<()> {
        self.conn.buffer_set.write_buffer.clear();
        write_sync(&mut self.conn.buffer_set.write_buffer);
        self.conn
            .stream
            .write_all(&self.conn.buffer_set.write_buffer)
            .await?;
        self.conn.stream.flush().await?;
        self.needs_flush = false;
        Ok(())
    }

    /// Wait for ReadyForQuery after all operations are claimed.
    async fn finish(&mut self) -> Result<()> {
        // Wait for ReadyForQuery
        loop {
            self.conn
                .stream
                .read_message(&mut self.conn.buffer_set)
                .await?;
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
                self.claim_seq = 0;
                self.expectations.clear();
                self.aborted = false;
                return Ok(());
            }
        }
    }

    // ========================================================================
    // Claim Operations
    // ========================================================================

    /// Claim with a custom handler.
    ///
    /// Results must be claimed in the same order they were queued.
    #[cfg(feature = "lowlevel")]
    pub async fn claim<H: BinaryHandler>(
        &mut self,
        ticket: Ticket,
        handler: &mut H,
    ) -> Result<()> {
        self.claim_with_handler(ticket, handler).await
    }

    async fn claim_with_handler<H: BinaryHandler>(
        &mut self,
        ticket: Ticket,
        handler: &mut H,
    ) -> Result<()> {
        self.check_sequence(ticket.seq)?;
        self.flush().await?;

        if self.aborted {
            self.claim_seq += 1;
            self.maybe_finish().await?;
            return Err(Error::Protocol(
                "pipeline aborted due to earlier error".into(),
            ));
        }

        let expectation = self.expectations.get(ticket.seq).copied();

        let result = match expectation {
            Some(Expectation::ParseBindExecute) => {
                self.claim_parse_bind_exec_inner(handler).await
            }
            Some(Expectation::BindExecute) => self.claim_bind_exec_inner(handler).await,
            None => Err(Error::Protocol("unexpected expectation type".into())),
        };

        if let Err(e) = &result {
            if e.is_connection_broken() {
                self.conn.is_broken = true;
            }
            self.aborted = true;
        }
        self.claim_seq += 1;
        self.maybe_finish().await?;
        result
    }

    /// Claim and collect all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    pub async fn claim_collect<T: for<'b> FromRow<'b>>(
        &mut self,
        ticket: Ticket,
    ) -> Result<Vec<T>> {
        let mut handler = crate::handler::CollectHandler::<T>::new();
        self.claim_with_handler(ticket, &mut handler).await?;
        Ok(handler.into_rows())
    }

    /// Claim and return just the first row.
    ///
    /// Results must be claimed in the same order they were queued.
    pub async fn claim_one<T: for<'b> FromRow<'b>>(
        &mut self,
        ticket: Ticket,
    ) -> Result<Option<T>> {
        let mut handler = crate::handler::FirstRowHandler::<T>::new();
        self.claim_with_handler(ticket, &mut handler).await?;
        Ok(handler.into_row())
    }

    /// Claim and discard all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    pub async fn claim_drop(&mut self, ticket: Ticket) -> Result<()> {
        let mut handler = crate::handler::DropHandler::new();
        self.claim_with_handler(ticket, &mut handler).await
    }

    /// Check that the ticket sequence matches the expected claim sequence.
    fn check_sequence(&self, seq: usize) -> Result<()> {
        if seq != self.claim_seq {
            return Err(Error::InvalidUsage(format!(
                "claim out of order: expected seq {}, got {}",
                self.claim_seq, seq
            )));
        }
        Ok(())
    }

    /// Check if all operations are claimed and consume ReadyForQuery if so.
    async fn maybe_finish(&mut self) -> Result<()> {
        if self.claim_seq == self.queue_seq {
            self.finish().await?;
        }
        Ok(())
    }

    /// Claim Parse + Bind + Execute (for raw SQL exec() calls).
    async fn claim_parse_bind_exec_inner<H: BinaryHandler>(
        &mut self,
        handler: &mut H,
    ) -> Result<()> {
        // Expect: ParseComplete
        self.read_next_message().await?;
        if self.conn.buffer_set.type_byte != msg_type::PARSE_COMPLETE {
            return self.unexpected_message("ParseComplete");
        }
        ParseComplete::parse(&self.conn.buffer_set.read_buffer)?;

        // Expect: BindComplete
        self.read_next_message().await?;
        if self.conn.buffer_set.type_byte != msg_type::BIND_COMPLETE {
            return self.unexpected_message("BindComplete");
        }
        BindComplete::parse(&self.conn.buffer_set.read_buffer)?;

        // Now read rows
        self.claim_rows_inner(handler).await
    }

    /// Claim Bind + Execute (for prepared statement exec() calls).
    async fn claim_bind_exec_inner<H: BinaryHandler>(&mut self, handler: &mut H) -> Result<()> {
        // Expect: BindComplete
        self.read_next_message().await?;
        if self.conn.buffer_set.type_byte != msg_type::BIND_COMPLETE {
            return self.unexpected_message("BindComplete");
        }
        BindComplete::parse(&self.conn.buffer_set.read_buffer)?;

        // Now read rows
        self.claim_rows_inner(handler).await
    }

    /// Common row reading logic.
    async fn claim_rows_inner<H: BinaryHandler>(&mut self, handler: &mut H) -> Result<()> {
        // Expect RowDescription or NoData
        self.read_next_message().await?;
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

        // Now read data rows until terminal message
        loop {
            self.read_next_message().await?;
            let type_byte = self.conn.buffer_set.type_byte;

            match type_byte {
                msg_type::DATA_ROW => {
                    let cols = RowDescription::parse(&self.column_buffer)?;
                    let row = DataRow::parse(&self.conn.buffer_set.read_buffer)?;
                    handler.row(cols, row)?;
                }
                msg_type::COMMAND_COMPLETE => {
                    let cmd = CommandComplete::parse(&self.conn.buffer_set.read_buffer)?;
                    handler.result_end(cmd)?;
                    return Ok(());
                }
                msg_type::EMPTY_QUERY_RESPONSE => {
                    EmptyQueryResponse::parse(&self.conn.buffer_set.read_buffer)?;
                    return Ok(());
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "unexpected message type in pipeline claim: '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }

    /// Read the next message, skipping async messages and handling errors.
    async fn read_next_message(&mut self) -> Result<()> {
        loop {
            self.conn
                .stream
                .read_message(&mut self.conn.buffer_set)
                .await?;
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

    /// Returns the number of operations that have been queued but not yet claimed.
    pub fn pending_count(&self) -> usize {
        self.queue_seq - self.claim_seq
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }
}
