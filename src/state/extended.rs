//! Extended query protocol state machine.

use crate::conversion::ToParams;
use crate::error::{Error, Result};
use crate::handler::BinaryHandler;
use crate::protocol::backend::{
    BindComplete, CloseComplete, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse,
    NoData, ParameterDescription, ParseComplete, PortalSuspended, RawMessage, ReadyForQuery,
    RowDescription, msg_type,
};
use crate::protocol::frontend::{
    write_bind, write_close_statement, write_describe_portal, write_describe_statement,
    write_execute, write_parse, write_sync,
};
use crate::protocol::types::{Oid, TransactionStatus};

use super::StateMachine;
use super::action::{Action, AsyncMessage};
use crate::buffer_set::BufferSet;

/// Extended query state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Initial,
    WaitingParse,
    WaitingBind,
    WaitingDescribe,
    WaitingRowDesc,
    ProcessingRows,
    WaitingReady,
    Finished,
}

/// Prepared statement information.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Statement index (unique within connection)
    pub idx: u64,
    /// Parameter type OIDs
    pub param_oids: Vec<Oid>,
    /// Raw RowDescription payload (if the statement returns rows)
    row_desc_payload: Option<Vec<u8>>,
    /// Custom wire name (if set, used instead of default)
    custom_wire_name: Option<String>,
}

impl PreparedStatement {
    /// Create a new prepared statement with custom wire name.
    pub fn new(
        idx: u64,
        param_oids: Vec<Oid>,
        row_desc_payload: Option<Vec<u8>>,
        wire_name: String,
    ) -> Self {
        Self {
            idx,
            param_oids,
            row_desc_payload,
            custom_wire_name: Some(wire_name),
        }
    }

    /// Get the wire protocol statement name.
    pub fn wire_name(&self) -> String {
        if let Some(name) = &self.custom_wire_name {
            name.clone()
        } else {
            format!("_zero_{}", self.idx)
        }
    }

    /// Parse column descriptions from stored RowDescription payload.
    ///
    /// Returns `None` if the statement doesn't return rows.
    pub fn parse_columns(&self) -> Option<Result<RowDescription<'_>>> {
        self.row_desc_payload
            .as_ref()
            .map(|bytes| RowDescription::parse(bytes))
    }

    /// Get the raw RowDescription payload.
    ///
    /// Returns `None` if the statement doesn't return rows.
    pub fn row_desc_payload(&self) -> Option<&[u8]> {
        self.row_desc_payload.as_deref()
    }
}

/// Operation type marker for tracking what operation is in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    /// Preparing a statement (Parse + Describe + Sync)
    Prepare,
    /// Executing a prepared statement (Bind + Describe + Execute + Sync)
    Execute,
    /// Executing raw SQL (Parse + Bind + Describe + Execute + Sync)
    ExecuteSql,
    /// Closing a statement (Close + Sync)
    CloseStatement,
}

/// Extended query protocol state machine.
pub struct ExtendedQueryStateMachine<'a, H> {
    state: State,
    handler: &'a mut H,
    operation: Operation,
    transaction_status: TransactionStatus,
    prepared_stmt: Option<PreparedStatement>,
}

impl<'a, H: BinaryHandler> ExtendedQueryStateMachine<'a, H> {
    /// Take the prepared statement (after prepare completes).
    pub fn take_prepared_statement(&mut self) -> Option<PreparedStatement> {
        self.prepared_stmt.take()
    }

    /// Prepare a statement.
    ///
    /// Writes Parse + DescribeStatement + Sync to `buffer_set.write_buffer`.
    pub fn prepare(
        handler: &'a mut H,
        buffer_set: &mut BufferSet,
        idx: u64,
        query: &str,
        param_oids: &[Oid],
    ) -> Self {
        let stmt_name = format!("_zero_{}", idx);
        buffer_set.write_buffer.clear();
        write_parse(&mut buffer_set.write_buffer, &stmt_name, query, param_oids);
        write_describe_statement(&mut buffer_set.write_buffer, &stmt_name);
        write_sync(&mut buffer_set.write_buffer);

        Self {
            state: State::Initial,
            handler,
            operation: Operation::Prepare,
            transaction_status: TransactionStatus::Idle,
            prepared_stmt: Some(PreparedStatement {
                idx,
                param_oids: Vec::new(),
                row_desc_payload: None,
                custom_wire_name: None,
            }),
        }
    }

    /// Execute a prepared statement.
    ///
    /// Writes Bind + DescribePortal + Execute + Sync to `buffer_set.write_buffer`.
    ///
    /// Uses the server-provided parameter OIDs to encode parameters, which allows
    /// flexible type conversion (e.g., i64 encoded as INT4 if server expects INT4).
    pub fn execute<P: ToParams>(
        handler: &'a mut H,
        buffer_set: &mut BufferSet,
        statement_name: &str,
        param_oids: &[Oid],
        params: &P,
    ) -> Result<Self> {
        buffer_set.write_buffer.clear();
        write_bind(
            &mut buffer_set.write_buffer,
            "",
            statement_name,
            params,
            param_oids,
        )?;
        write_describe_portal(&mut buffer_set.write_buffer, "");
        write_execute(&mut buffer_set.write_buffer, "", 0);
        write_sync(&mut buffer_set.write_buffer);

        Ok(Self {
            state: State::Initial,
            handler,
            operation: Operation::Execute,
            transaction_status: TransactionStatus::Idle,
            prepared_stmt: None,
        })
    }

    /// Execute raw SQL (unnamed statement).
    ///
    /// Writes Parse + Bind + DescribePortal + Execute + Sync to `buffer_set.write_buffer`.
    ///
    /// Uses the natural OIDs from the parameters to inform the server about parameter types,
    /// which prevents "incorrect binary data format" errors when the server would otherwise
    /// infer a different type (e.g., INT4 vs INT8).
    pub fn execute_sql<P: ToParams>(
        handler: &'a mut H,
        buffer_set: &mut BufferSet,
        sql: &str,
        params: &P,
    ) -> Result<Self> {
        let param_oids = params.natural_oids();
        buffer_set.write_buffer.clear();
        write_parse(&mut buffer_set.write_buffer, "", sql, &param_oids);
        write_bind(&mut buffer_set.write_buffer, "", "", params, &param_oids)?;
        write_describe_portal(&mut buffer_set.write_buffer, "");
        write_execute(&mut buffer_set.write_buffer, "", 0);
        write_sync(&mut buffer_set.write_buffer);

        Ok(Self {
            state: State::Initial,
            handler,
            operation: Operation::ExecuteSql,
            transaction_status: TransactionStatus::Idle,
            prepared_stmt: None,
        })
    }

    /// Close a prepared statement.
    ///
    /// Writes Close + Sync to `buffer_set.write_buffer`.
    pub fn close_statement(handler: &'a mut H, buffer_set: &mut BufferSet, name: &str) -> Self {
        buffer_set.write_buffer.clear();
        write_close_statement(&mut buffer_set.write_buffer, name);
        write_sync(&mut buffer_set.write_buffer);

        Self {
            state: State::Initial,
            handler,
            operation: Operation::CloseStatement,
            transaction_status: TransactionStatus::Idle,
            prepared_stmt: None,
        }
    }

    fn handle_parse(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::PARSE_COMPLETE {
            return Err(Error::Protocol(format!(
                "Expected ParseComplete, got '{}'",
                type_byte as char
            )));
        }

        ParseComplete::parse(&buffer_set.read_buffer)?;
        // For SQL execute, next we get BindComplete
        // For prepare, go to WaitingDescribe to get ParameterDescription
        self.state = match self.operation {
            Operation::ExecuteSql => State::WaitingBind,
            Operation::Prepare => State::WaitingDescribe,
            _ => unreachable!("handle_parse called for non-parse operation"),
        };
        Ok(Action::ReadMessage)
    }

    fn handle_describe(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::PARAMETER_DESCRIPTION {
            return Err(Error::Protocol(format!(
                "Expected ParameterDescription, got '{}'",
                type_byte as char
            )));
        }

        let param_desc = ParameterDescription::parse(&buffer_set.read_buffer)?;
        if let Some(ref mut stmt) = self.prepared_stmt {
            stmt.param_oids = param_desc.oids().to_vec();
        }

        self.state = State::WaitingRowDesc;
        Ok(Action::ReadMessage)
    }

    fn handle_row_desc(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                if let Some(ref mut stmt) = self.prepared_stmt {
                    stmt.row_desc_payload = Some(buffer_set.read_buffer.clone());
                }
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            msg_type::NO_DATA => {
                let payload = &buffer_set.read_buffer;
                NoData::parse(payload)?;
                // Statement doesn't return rows
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            _ => Err(Error::Protocol(format!(
                "Expected RowDescription or NoData, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_bind(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;

        match type_byte {
            msg_type::BIND_COMPLETE => {
                BindComplete::parse(&buffer_set.read_buffer)?;
                self.state = State::ProcessingRows;
                Ok(Action::ReadMessage)
            }
            msg_type::ROW_DESCRIPTION => {
                // Store column buffer for later use in row callbacks
                buffer_set.column_buffer.clear();
                buffer_set
                    .column_buffer
                    .extend_from_slice(&buffer_set.read_buffer);
                let cols = RowDescription::parse(&buffer_set.column_buffer)?;
                self.handler.result_start(cols)?;
                self.state = State::ProcessingRows;
                Ok(Action::ReadMessage)
            }
            _ => Err(Error::Protocol(format!(
                "Expected BindComplete, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_rows(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                // Store column buffer for later use in row callbacks
                buffer_set.column_buffer.clear();
                buffer_set.column_buffer.extend_from_slice(payload);
                let cols = RowDescription::parse(&buffer_set.column_buffer)?;
                self.handler.result_start(cols)?;
                Ok(Action::ReadMessage)
            }
            msg_type::NO_DATA => {
                // Statement doesn't return rows (e.g., INSERT without RETURNING)
                NoData::parse(payload)?;
                Ok(Action::ReadMessage)
            }
            msg_type::DATA_ROW => {
                let cols = RowDescription::parse(&buffer_set.column_buffer)?;
                let row = DataRow::parse(payload)?;
                self.handler.row(cols, row)?;
                Ok(Action::ReadMessage)
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.result_end(complete)?;
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            msg_type::EMPTY_QUERY_RESPONSE => {
                EmptyQueryResponse::parse(payload)?;
                // Portal was created from an empty query string
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            msg_type::PORTAL_SUSPENDED => {
                PortalSuspended::parse(payload)?;
                // Row limit reached, need to Execute again to get more
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            msg_type::READY_FOR_QUERY => {
                let ready = ReadyForQuery::parse(payload)?;
                self.transaction_status = ready.transaction_status().unwrap_or_default();
                self.state = State::Finished;
                Ok(Action::Finished)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected message in rows: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_ready(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::READY_FOR_QUERY => {
                let ready = ReadyForQuery::parse(payload)?;
                self.transaction_status = ready.transaction_status().unwrap_or_default();
                self.state = State::Finished;
                Ok(Action::Finished)
            }
            msg_type::CLOSE_COMPLETE => {
                CloseComplete::parse(payload)?;
                // Continue waiting for ReadyForQuery
                Ok(Action::ReadMessage)
            }
            _ => Err(Error::Protocol(format!(
                "Expected ReadyForQuery, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_async_message(&self, msg: &RawMessage<'_>) -> Result<Action> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::Notice(notice.0),
                ))
            }
            msg_type::PARAMETER_STATUS => {
                let param = crate::protocol::backend::auth::ParameterStatus::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::ParameterChanged {
                        name: param.name.to_string(),
                        value: param.value.to_string(),
                    },
                ))
            }
            msg_type::NOTIFICATION_RESPONSE => {
                let notification =
                    crate::protocol::backend::auth::NotificationResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::Notification {
                        pid: notification.pid,
                        channel: notification.channel.to_string(),
                        payload: notification.payload.to_string(),
                    },
                ))
            }
            _ => Err(Error::Protocol(format!(
                "Unknown async message type: '{}'",
                msg.type_byte as char
            ))),
        }
    }
}

impl<H: BinaryHandler> StateMachine for ExtendedQueryStateMachine<'_, H> {
    fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Initial state: write buffer was pre-filled by constructor
        if self.state == State::Initial {
            // Determine initial waiting state based on operation
            self.state = match self.operation {
                Operation::Prepare => State::WaitingParse,
                Operation::Execute => State::WaitingBind, // First we get BindComplete
                Operation::ExecuteSql => State::WaitingParse,
                Operation::CloseStatement => State::WaitingReady,
            };
            return Ok(Action::WriteAndReadMessage);
        }

        let type_byte = buffer_set.type_byte;

        // Handle async messages
        if RawMessage::is_async_type(type_byte) {
            let msg = RawMessage::new(type_byte, &buffer_set.read_buffer);
            return self.handle_async_message(&msg);
        }

        // Handle error response
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&buffer_set.read_buffer)?;
            // After error, server skips to Sync response
            self.state = State::WaitingReady;
            return Err(error.into_error());
        }

        match self.state {
            State::WaitingParse => self.handle_parse(buffer_set),
            State::WaitingDescribe => self.handle_describe(buffer_set),
            State::WaitingRowDesc => self.handle_row_desc(buffer_set),
            State::WaitingBind => self.handle_bind(buffer_set),
            State::ProcessingRows => self.handle_rows(buffer_set),
            State::WaitingReady => self.handle_ready(buffer_set),
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }
}

// === Bind Portal State Machine ===
// Used by exec_iter to create a portal without executing it.

use crate::protocol::frontend::write_flush;

/// State for bind portal flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindState {
    Initial,
    WaitingParse,
    WaitingBind,
    Finished,
}

/// State machine for binding a portal (Parse + Bind, no Execute/Sync).
///
/// Used by `exec_iter` to create a portal that can be executed multiple times.
pub struct BindStateMachine {
    state: BindState,
    needs_parse: bool,
}

impl BindStateMachine {
    /// Bind a prepared statement to an unnamed portal.
    ///
    /// Writes Bind + Flush to `buffer_set.write_buffer`.
    ///
    /// Uses the server-provided parameter OIDs to encode parameters.
    pub fn bind_prepared<P: ToParams>(
        buffer_set: &mut BufferSet,
        statement_name: &str,
        param_oids: &[Oid],
        params: &P,
    ) -> Result<Self> {
        buffer_set.write_buffer.clear();
        write_bind(
            &mut buffer_set.write_buffer,
            "",
            statement_name,
            params,
            param_oids,
        )?;
        write_flush(&mut buffer_set.write_buffer);

        Ok(Self {
            state: BindState::Initial,
            needs_parse: false,
        })
    }

    /// Parse raw SQL and bind to an unnamed portal.
    ///
    /// Writes Parse + Bind + Flush to `buffer_set.write_buffer`.
    ///
    /// Uses the natural OIDs from the parameters to inform the server about parameter types.
    pub fn bind_sql<P: ToParams>(buffer_set: &mut BufferSet, sql: &str, params: &P) -> Result<Self> {
        let param_oids = params.natural_oids();
        buffer_set.write_buffer.clear();
        write_parse(&mut buffer_set.write_buffer, "", sql, &param_oids);
        write_bind(&mut buffer_set.write_buffer, "", "", params, &param_oids)?;
        write_flush(&mut buffer_set.write_buffer);

        Ok(Self {
            state: BindState::Initial,
            needs_parse: true,
        })
    }

    /// Process input and return the next action.
    pub fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Initial state: write buffer was pre-filled by constructor
        if self.state == BindState::Initial {
            self.state = if self.needs_parse {
                BindState::WaitingParse
            } else {
                BindState::WaitingBind
            };
            return Ok(Action::WriteAndReadMessage);
        }

        let type_byte = buffer_set.type_byte;

        // Handle async messages - need to keep reading
        if RawMessage::is_async_type(type_byte) {
            return Ok(Action::ReadMessage);
        }

        // Handle error response
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&buffer_set.read_buffer)?;
            return Err(error.into_error());
        }

        match self.state {
            BindState::WaitingParse => {
                if type_byte != msg_type::PARSE_COMPLETE {
                    return Err(Error::Protocol(format!(
                        "Expected ParseComplete, got '{}'",
                        type_byte as char
                    )));
                }
                ParseComplete::parse(&buffer_set.read_buffer)?;
                self.state = BindState::WaitingBind;
                Ok(Action::ReadMessage)
            }
            BindState::WaitingBind => {
                if type_byte != msg_type::BIND_COMPLETE {
                    return Err(Error::Protocol(format!(
                        "Expected BindComplete, got '{}'",
                        type_byte as char
                    )));
                }
                BindComplete::parse(&buffer_set.read_buffer)?;
                self.state = BindState::Finished;
                Ok(Action::Finished)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }
}

// === Batch Execution State Machine ===
// Used by exec_batch to execute multiple parameter sets efficiently.

/// State for batch execution flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchState {
    Initial,
    WaitingParse,
    Processing,
    Finished,
}

/// State machine for batch execution (Parse? + (Bind + Execute)* + Sync).
///
/// Used by `exec_batch` to execute a statement with multiple parameter sets.
pub struct BatchStateMachine {
    state: BatchState,
    needs_parse: bool,
    transaction_status: TransactionStatus,
}

impl BatchStateMachine {
    /// Create a new batch state machine.
    ///
    /// The caller is responsible for populating `buffer_set.write_buffer` with:
    /// - Parse (optional, if needs_parse is true)
    /// - Bind + Execute for each parameter set
    /// - Sync
    pub fn new(needs_parse: bool) -> Self {
        Self {
            state: BatchState::Initial,
            needs_parse,
            transaction_status: TransactionStatus::Idle,
        }
    }

    /// Get the transaction status after completion.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Process input and return the next action.
    pub fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Initial state: write buffer was pre-filled by caller
        if self.state == BatchState::Initial {
            self.state = if self.needs_parse {
                BatchState::WaitingParse
            } else {
                BatchState::Processing
            };
            return Ok(Action::WriteAndReadMessage);
        }

        let type_byte = buffer_set.type_byte;

        // Handle async messages - need to keep reading
        if RawMessage::is_async_type(type_byte) {
            return Ok(Action::ReadMessage);
        }

        // Handle error response - continue reading until ReadyForQuery
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&buffer_set.read_buffer)?;
            self.state = BatchState::Processing;
            return Err(error.into_error());
        }

        match self.state {
            BatchState::WaitingParse => {
                if type_byte != msg_type::PARSE_COMPLETE {
                    return Err(Error::Protocol(format!(
                        "Expected ParseComplete, got '{}'",
                        type_byte as char
                    )));
                }
                ParseComplete::parse(&buffer_set.read_buffer)?;
                self.state = BatchState::Processing;
                Ok(Action::ReadMessage)
            }
            BatchState::Processing => {
                match type_byte {
                    msg_type::BIND_COMPLETE => {
                        BindComplete::parse(&buffer_set.read_buffer)?;
                        Ok(Action::ReadMessage)
                    }
                    msg_type::NO_DATA => {
                        NoData::parse(&buffer_set.read_buffer)?;
                        Ok(Action::ReadMessage)
                    }
                    msg_type::ROW_DESCRIPTION => {
                        // Discard row description - we don't process rows in batch
                        RowDescription::parse(&buffer_set.read_buffer)?;
                        Ok(Action::ReadMessage)
                    }
                    msg_type::DATA_ROW => {
                        // Discard data rows - batch doesn't return data
                        Ok(Action::ReadMessage)
                    }
                    msg_type::COMMAND_COMPLETE => {
                        CommandComplete::parse(&buffer_set.read_buffer)?;
                        Ok(Action::ReadMessage)
                    }
                    msg_type::EMPTY_QUERY_RESPONSE => {
                        EmptyQueryResponse::parse(&buffer_set.read_buffer)?;
                        Ok(Action::ReadMessage)
                    }
                    msg_type::READY_FOR_QUERY => {
                        let ready = ReadyForQuery::parse(&buffer_set.read_buffer)?;
                        self.transaction_status = ready.transaction_status().unwrap_or_default();
                        self.state = BatchState::Finished;
                        Ok(Action::Finished)
                    }
                    _ => Err(Error::Protocol(format!(
                        "Unexpected message in batch: '{}'",
                        type_byte as char
                    ))),
                }
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }
}
