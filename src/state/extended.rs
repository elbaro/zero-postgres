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
}

/// Extended query protocol state machine.
pub struct ExtendedQueryStateMachine<'a, H> {
    state: State,
    handler: &'a mut H,
    write_buffer: Vec<u8>,
    column_buffer: Vec<u8>,
    transaction_status: TransactionStatus,
    prepared_stmt: Option<PreparedStatement>,
    /// True when executing raw SQL (Parse+Bind flow) vs prepared statement (Bind-only flow)
    is_sql_execute: bool,
}

impl<'a, H: BinaryHandler> ExtendedQueryStateMachine<'a, H> {
    /// Create a new extended query state machine.
    pub fn new(handler: &'a mut H) -> Self {
        Self {
            state: State::Initial,
            handler,
            write_buffer: Vec::new(),
            column_buffer: Vec::new(),
            transaction_status: TransactionStatus::Idle,
            prepared_stmt: None,
            is_sql_execute: false,
        }
    }

    /// Get the transaction status.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Take the prepared statement (after prepare completes).
    pub fn take_prepared_statement(&mut self) -> Option<PreparedStatement> {
        self.prepared_stmt.take()
    }

    /// Prepare a statement.
    ///
    /// This sends Parse + Describe + Sync messages.
    pub fn prepare(&mut self, idx: u64, query: &str, param_oids: &[Oid]) -> Action<'_> {
        let name = format!("_zero_{}", idx);
        self.write_buffer.clear();
        write_parse(&mut self.write_buffer, &name, query, param_oids);
        write_describe_statement(&mut self.write_buffer, &name);
        write_sync(&mut self.write_buffer);

        self.prepared_stmt = Some(PreparedStatement {
            idx,
            param_oids: Vec::new(),
            row_desc_payload: None,
            custom_wire_name: None,
        });
        self.state = State::WaitingParse;
        Action::WritePacket(&self.write_buffer)
    }

    /// Execute a prepared statement.
    ///
    /// This sends Bind + Describe + Execute + Sync messages.
    pub fn execute<P: ToParams>(&mut self, statement_name: &str, params: &P) -> Action<'_> {
        self.write_buffer.clear();
        write_bind(
            &mut self.write_buffer,
            "", // unnamed portal
            statement_name,
            params,
            &[], // result formats (empty = use default)
        );
        write_describe_portal(&mut self.write_buffer, ""); // get RowDescription
        write_execute(&mut self.write_buffer, "", 0); // unnamed portal, unlimited rows
        write_sync(&mut self.write_buffer);

        self.is_sql_execute = false;
        self.state = State::WaitingBind;
        Action::WritePacket(&self.write_buffer)
    }

    /// Execute raw SQL (unnamed statement).
    ///
    /// This sends Parse + Bind + Describe + Execute + Sync messages.
    pub fn execute_sql<P: ToParams>(&mut self, sql: &str, params: &P) -> Action<'_> {
        self.write_buffer.clear();
        write_parse(&mut self.write_buffer, "", sql, &[]); // unnamed statement
        write_bind(
            &mut self.write_buffer,
            "", // unnamed portal
            "", // unnamed statement
            params,
            &[], // result formats (empty = use default)
        );
        write_describe_portal(&mut self.write_buffer, ""); // get RowDescription
        write_execute(&mut self.write_buffer, "", 0); // unnamed portal, unlimited rows
        write_sync(&mut self.write_buffer);

        self.is_sql_execute = true;
        self.state = State::WaitingParse;
        Action::WritePacket(&self.write_buffer)
    }

    /// Close a prepared statement.
    pub fn close_statement(&mut self, name: &str) -> Action<'_> {
        self.write_buffer.clear();
        write_close_statement(&mut self.write_buffer, name);
        write_sync(&mut self.write_buffer);

        self.state = State::WaitingReady;
        Action::WritePacket(&self.write_buffer)
    }

    /// Process a message from the server.
    ///
    /// The caller should fill buffer_set.read_buffer with the message payload
    /// and set buffer_set.type_byte to the message type.
    pub fn step<'buf>(&'buf mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
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

    fn handle_parse<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::PARSE_COMPLETE {
            return Err(Error::Protocol(format!(
                "Expected ParseComplete, got '{}'",
                type_byte as char
            )));
        }

        ParseComplete::parse(&buffer_set.read_buffer)?;
        // For SQL execute, go directly to WaitingBind (skip describe flow)
        // For prepare, go to WaitingDescribe to get ParameterDescription
        self.state = if self.is_sql_execute {
            State::WaitingBind
        } else {
            State::WaitingDescribe
        };
        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
    }

    fn handle_describe<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
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
        Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
    }

    fn handle_row_desc<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                if let Some(ref mut stmt) = self.prepared_stmt {
                    stmt.row_desc_payload = Some(std::mem::take(&mut buffer_set.read_buffer));
                }
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::NO_DATA => {
                let payload = &buffer_set.read_buffer;
                NoData::parse(payload)?;
                // Statement doesn't return rows
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            _ => Err(Error::Protocol(format!(
                "Expected RowDescription or NoData, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_bind<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::BIND_COMPLETE => {
                BindComplete::parse(payload)?;
                self.state = State::ProcessingRows;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::ROW_DESCRIPTION => {
                // Store column buffer for later use in row callbacks
                self.column_buffer.clear();
                self.column_buffer.extend_from_slice(payload);
                let cols = RowDescription::parse(&self.column_buffer)?;
                self.handler.result_start(cols)?;
                self.state = State::ProcessingRows;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            _ => Err(Error::Protocol(format!(
                "Expected BindComplete, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_rows<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                // Store column buffer for later use in row callbacks
                self.column_buffer.clear();
                self.column_buffer.extend_from_slice(payload);
                let cols = RowDescription::parse(&self.column_buffer)?;
                self.handler.result_start(cols)?;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::NO_DATA => {
                // Statement doesn't return rows (e.g., INSERT without RETURNING)
                NoData::parse(payload)?;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::DATA_ROW => {
                let cols = RowDescription::parse(&self.column_buffer)?;
                let row = DataRow::parse(payload)?;
                self.handler.row(cols, row)?;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.result_end(complete)?;
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::EMPTY_QUERY_RESPONSE => {
                EmptyQueryResponse::parse(payload)?;
                // Portal was created from an empty query string
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::PORTAL_SUSPENDED => {
                PortalSuspended::parse(payload)?;
                // Row limit reached, need to Execute again to get more
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
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

    fn handle_ready<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
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
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            _ => Err(Error::Protocol(format!(
                "Expected ReadyForQuery, got '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_async_message(&self, msg: &RawMessage<'_>) -> Result<Action<'_>> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::Notice(notice.0)))
            }
            msg_type::PARAMETER_STATUS => {
                let param = crate::protocol::backend::auth::ParameterStatus::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::ParameterChanged {
                    name: param.name.to_string(),
                    value: param.value.to_string(),
                }))
            }
            msg_type::NOTIFICATION_RESPONSE => {
                let notification =
                    crate::protocol::backend::auth::NotificationResponse::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::Notification {
                    pid: notification.pid,
                    channel: notification.channel.to_string(),
                    payload: notification.payload.to_string(),
                }))
            }
            _ => Err(Error::Protocol(format!(
                "Unknown async message type: '{}'",
                msg.type_byte as char
            ))),
        }
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
    write_buffer: Vec<u8>,
    needs_parse: bool,
}

impl BindStateMachine {
    /// Create a new bind portal state machine.
    pub fn new() -> Self {
        Self {
            state: BindState::Initial,
            write_buffer: Vec::new(),
            needs_parse: false,
        }
    }

    /// Bind a prepared statement to an unnamed portal.
    ///
    /// Sends Bind + Flush messages.
    pub fn bind_prepared<P: ToParams>(
        &mut self,
        statement_name: &str,
        params: &P,
    ) -> Action<'_> {
        self.write_buffer.clear();
        write_bind(&mut self.write_buffer, "", statement_name, params, &[]);
        write_flush(&mut self.write_buffer);

        self.needs_parse = false;
        self.state = BindState::WaitingBind;
        Action::WritePacket(&self.write_buffer)
    }

    /// Parse raw SQL and bind to an unnamed portal.
    ///
    /// Sends Parse + Bind + Flush messages.
    pub fn bind_sql<P: ToParams>(&mut self, sql: &str, params: &P) -> Action<'_> {
        self.write_buffer.clear();
        write_parse(&mut self.write_buffer, "", sql, &[]); // unnamed statement
        write_bind(&mut self.write_buffer, "", "", params, &[]); // bind to unnamed portal
        write_flush(&mut self.write_buffer);

        self.needs_parse = true;
        self.state = BindState::WaitingParse;
        Action::WritePacket(&self.write_buffer)
    }

    /// Process a message from the server.
    pub fn step(&mut self, buffer_set: &mut BufferSet) -> Result<bool> {
        let type_byte = buffer_set.type_byte;

        // Handle async messages
        if RawMessage::is_async_type(type_byte) {
            return Ok(false); // Not finished, continue
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
                Ok(false) // Continue to BindComplete
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
                Ok(true) // Finished
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }
}

impl Default for BindStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
