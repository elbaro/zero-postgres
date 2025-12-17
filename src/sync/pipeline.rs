//! Pipeline mode for batching multiple queries.
//!
//! Pipeline mode allows sending multiple queries to the server without waiting
//! for responses, reducing round-trip latency.
//!
//! # Example
//!
//! ```ignore
//! let insert_stmt = conn.prepare("INSERT INTO users (name) VALUES ($1)")?;
//! let get_stmt = conn.prepare("SELECT * FROM users")?;
//!
//! let mut pipeline = conn.pipeline();
//!
//! pipeline.queue(&insert_stmt, (&"alice",))?;
//! pipeline.queue(&insert_stmt, (&"bob",))?;
//! pipeline.queue(&get_stmt, ())?;
//!
//! pipeline.fetch_drop()?;
//! pipeline.fetch_drop()?;
//! let users: Vec<User> = pipeline.fetch_collect()?;
//!
//! pipeline.sync()?;
//! ```

use crate::conversion::{FromRow, ToParams};
use crate::error::{Error, Result};
use crate::handler::{BinaryHandler, CollectHandler, DropHandler, FirstRowHandler};
use crate::protocol::backend::{
    msg_type, BindComplete, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, NoData,
    PortalSuspended, RawMessage, ReadyForQuery, RowDescription,
};
use crate::protocol::frontend::{write_bind, write_execute, write_flush, write_sync};
use crate::state::extended::PreparedStatement;

use super::conn::{read_message_into, Conn};

/// Pipeline mode for batching multiple queries.
///
/// Created by [`Conn::pipeline`].
pub struct Pipeline<'a> {
    conn: &'a mut Conn,
    /// Whether we have queued data that needs to be flushed
    needs_flush: bool,
    /// Number of queries queued (pending responses)
    pending: usize,
    /// Number of queries fetched
    fetched: usize,
    /// Buffer for column descriptions during row processing
    column_buffer: Vec<u8>,
    /// Whether the pipeline is in aborted state (error occurred)
    aborted: bool,
}

impl<'a> Pipeline<'a> {
    /// Create a new pipeline.
    pub(crate) fn new(conn: &'a mut Conn) -> Self {
        Self {
            conn,
            needs_flush: false,
            pending: 0,
            fetched: 0,
            column_buffer: Vec::new(),
            aborted: false,
        }
    }

    /// Queue execution of a prepared statement.
    ///
    /// Sends Bind + Execute messages immediately but does not wait for response.
    /// Call [`fetch`](Self::fetch) or variants to retrieve results.
    pub fn queue<P: ToParams>(&mut self, stmt: &PreparedStatement, params: P) -> Result<()> {
        let result = self.queue_inner(&stmt.wire_name(), &params);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result
    }

    fn queue_inner<P: ToParams>(&mut self, statement_name: &str, params: &P) -> Result<()> {
        self.conn.write_buffer.clear();
        write_bind(&mut self.conn.write_buffer, "", statement_name, params, &[]);
        write_execute(&mut self.conn.write_buffer, "", 0);
        self.conn.stream.write_all(&self.conn.write_buffer)?;
        self.needs_flush = true;
        self.pending += 1;
        Ok(())
    }

    /// Fetch next result using a custom handler.
    ///
    /// Provides zero-copy access to result data through the handler callbacks.
    /// Results must be fetched in the same order they were queued.
    pub fn fetch<H: BinaryHandler>(&mut self, handler: &mut H) -> Result<()> {
        self.check_pending()?;
        let result = self.fetch_inner(handler);
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result
    }

    /// Fetch next result and discard it.
    ///
    /// Returns the number of rows affected, if applicable.
    pub fn fetch_drop(&mut self) -> Result<Option<u64>> {
        let mut handler = DropHandler::new();
        self.fetch(&mut handler)?;
        Ok(handler.rows_affected())
    }

    /// Fetch next result as typed rows.
    pub fn fetch_collect<T: for<'b> FromRow<'b>>(&mut self) -> Result<Vec<T>> {
        let mut handler = CollectHandler::<T>::new();
        self.fetch(&mut handler)?;
        Ok(handler.into_rows())
    }

    /// Fetch next result as a single typed row.
    pub fn fetch_first<T: for<'b> FromRow<'b>>(&mut self) -> Result<Option<T>> {
        let mut handler = FirstRowHandler::<T>::new();
        self.fetch(&mut handler)?;
        Ok(handler.into_row())
    }

    /// Send a FLUSH message to trigger server response.
    ///
    /// This forces the server to send all pending responses without establishing
    /// a transaction boundary. Called automatically by fetch methods when needed.
    pub fn flush(&mut self) -> Result<()> {
        if self.needs_flush {
            self.conn.write_buffer.clear();
            write_flush(&mut self.conn.write_buffer);
            self.conn.stream.write_all(&self.conn.write_buffer)?;
            self.conn.stream.flush()?;
            self.needs_flush = false;
        }
        Ok(())
    }

    /// Send a SYNC message and wait for ReadyForQuery.
    ///
    /// This establishes a transaction boundary. All queued queries must be
    /// fetched before calling sync.
    pub fn sync(&mut self) -> Result<()> {
        if self.fetched != self.pending {
            return Err(Error::InvalidUsage(format!(
                "cannot sync: {} queries not yet fetched",
                self.pending - self.fetched
            )));
        }

        let result = self.sync_inner();
        if let Err(e) = &result
            && e.is_connection_broken()
        {
            self.conn.is_broken = true;
        }
        result
    }

    fn sync_inner(&mut self) -> Result<()> {
        self.conn.write_buffer.clear();
        write_sync(&mut self.conn.write_buffer);
        self.conn.stream.write_all(&self.conn.write_buffer)?;
        self.conn.stream.flush()?;
        self.needs_flush = false;

        // Wait for ReadyForQuery
        loop {
            read_message_into(&mut self.conn.stream, &mut self.conn.buffer_set)?;
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
                self.pending = 0;
                self.fetched = 0;
                self.aborted = false;
                return Ok(());
            }
        }
    }

    /// Check that there are pending queries to fetch.
    fn check_pending(&self) -> Result<()> {
        if self.fetched >= self.pending {
            return Err(Error::InvalidUsage("no more queries to fetch".into()));
        }
        Ok(())
    }

    /// Fetch results for the next query.
    fn fetch_inner<H: BinaryHandler>(&mut self, handler: &mut H) -> Result<()> {
        // Ensure data is flushed before reading
        self.flush()?;

        // If pipeline is aborted, return error
        if self.aborted {
            self.fetched += 1;
            return Err(Error::Protocol(
                "pipeline aborted due to earlier error".into(),
            ));
        }

        // Read BindComplete
        read_message_into(&mut self.conn.stream, &mut self.conn.buffer_set)?;
        let bind_type = self.conn.buffer_set.type_byte;
        if let Err(e) = self.handle_bind_complete(bind_type) {
            self.aborted = true;
            self.fetched += 1;
            return Err(e);
        }

        // Read RowDescription/NoData, then DataRows, then CommandComplete
        loop {
            read_message_into(&mut self.conn.stream, &mut self.conn.buffer_set)?;
            let type_byte = self.conn.buffer_set.type_byte;

            // Handle async messages
            if RawMessage::is_async_type(type_byte) {
                continue;
            }

            // Handle error
            if type_byte == msg_type::ERROR_RESPONSE {
                let error = ErrorResponse::parse(&self.conn.buffer_set.read_buffer)?;
                self.aborted = true;
                self.fetched += 1;
                return Err(error.into_error());
            }

            match type_byte {
                msg_type::ROW_DESCRIPTION => {
                    self.column_buffer.clear();
                    self.column_buffer.extend_from_slice(&self.conn.buffer_set.read_buffer);
                    let cols = RowDescription::parse(&self.column_buffer)?;
                    handler.result_start(cols)?;
                }
                msg_type::NO_DATA => {
                    NoData::parse(&self.conn.buffer_set.read_buffer)?;
                }
                msg_type::DATA_ROW => {
                    let cols = RowDescription::parse(&self.column_buffer)?;
                    let row = DataRow::parse(&self.conn.buffer_set.read_buffer)?;
                    handler.row(cols, row)?;
                }
                msg_type::COMMAND_COMPLETE => {
                    let complete = CommandComplete::parse(&self.conn.buffer_set.read_buffer)?;
                    handler.result_end(complete)?;
                    self.fetched += 1;
                    return Ok(());
                }
                msg_type::EMPTY_QUERY_RESPONSE => {
                    EmptyQueryResponse::parse(&self.conn.buffer_set.read_buffer)?;
                    // Portal was created from an empty query string
                    self.fetched += 1;
                    return Ok(());
                }
                msg_type::PORTAL_SUSPENDED => {
                    PortalSuspended::parse(&self.conn.buffer_set.read_buffer)?;
                    // Row limit reached, more rows available
                    self.fetched += 1;
                    return Ok(());
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "unexpected message type in pipeline fetch: '{}'",
                        type_byte as char
                    )));
                }
            }
        }
    }

    /// Handle BindComplete message.
    fn handle_bind_complete(&mut self, mut type_byte: u8) -> Result<()> {
        // Handle async messages by reading more
        while RawMessage::is_async_type(type_byte) {
            read_message_into(&mut self.conn.stream, &mut self.conn.buffer_set)?;
            type_byte = self.conn.buffer_set.type_byte;
        }

        // Handle error
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&self.conn.buffer_set.read_buffer)?;
            return Err(error.into_error());
        }

        if type_byte != msg_type::BIND_COMPLETE {
            return Err(Error::Protocol(format!(
                "expected BindComplete, got '{}'",
                type_byte as char
            )));
        }

        BindComplete::parse(&self.conn.buffer_set.read_buffer)?;
        Ok(())
    }

    /// Returns the number of queries that have been queued but not yet fetched.
    pub fn pending_count(&self) -> usize {
        self.pending - self.fetched
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }
}
