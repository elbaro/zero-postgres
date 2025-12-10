//! Extended query protocol state machine.

use crate::error::{Error, Result};
use crate::handler::BinaryHandler;
use crate::protocol::backend::{
    BindComplete, CloseComplete, CommandComplete, DataRow, ErrorResponse, FieldDescriptionTail,
    NoData, ParameterDescription, ParseComplete, PortalSuspended, RawMessage, ReadyForQuery,
    RowDescription, msg_type,
};
use crate::protocol::frontend::{
    write_bind, write_close_statement, write_describe_portal, write_describe_statement,
    write_execute, write_parse, write_sync,
};
use crate::protocol::types::{FormatCode, Oid, TransactionStatus};
use crate::value::ToParams;

use super::action::{Action, AsyncMessage};
use super::simple_query::BufferSet;

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
    /// Statement name
    pub name: String,
    /// Parameter type OIDs
    pub param_oids: Vec<Oid>,
    /// Column descriptions (if the statement returns rows)
    pub columns: Option<Vec<ColumnInfo>>,
}

/// Column information from RowDescription.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub tail: FieldDescriptionTail,
}

impl ColumnInfo {
    /// Table OID (0 if not a table column)
    pub fn table_oid(&self) -> Oid {
        self.tail.table_oid.get()
    }

    /// Column attribute number (0 if not a table column)
    pub fn column_id(&self) -> i16 {
        self.tail.column_id.get()
    }

    /// Data type OID
    pub fn type_oid(&self) -> Oid {
        self.tail.type_oid.get()
    }

    /// Type size (-1 for variable, -2 for null-terminated)
    pub fn type_size(&self) -> i16 {
        self.tail.type_size.get()
    }

    /// Type modifier (type-specific)
    pub fn type_modifier(&self) -> i32 {
        self.tail.type_modifier.get()
    }

    /// Format code (0=text, 1=binary)
    pub fn format(&self) -> FormatCode {
        FormatCode::from_u16(self.tail.format.get())
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
    pub fn prepare(&mut self, name: &str, query: &str, param_oids: &[Oid]) -> Action<'_> {
        self.write_buffer.clear();
        write_parse(&mut self.write_buffer, name, query, param_oids);
        write_describe_statement(&mut self.write_buffer, name);
        write_sync(&mut self.write_buffer);

        self.prepared_stmt = Some(PreparedStatement {
            name: name.to_string(),
            param_oids: Vec::new(),
            columns: None,
        });
        self.state = State::WaitingParse;
        Action::WritePacket(&self.write_buffer)
    }

    /// Execute a prepared statement.
    ///
    /// This sends Bind + Describe + Execute + Sync messages.
    pub fn execute<P: ToParams>(&mut self, statement: &str, params: &P) -> Action<'_> {
        self.write_buffer.clear();
        write_bind(
            &mut self.write_buffer,
            "", // unnamed portal
            statement,
            params,
            &[], // result formats (empty = use default)
        );
        write_describe_portal(&mut self.write_buffer, ""); // get RowDescription
        write_execute(&mut self.write_buffer, "", 0); // unnamed portal, unlimited rows
        write_sync(&mut self.write_buffer);

        self.state = State::WaitingBind;
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
        self.state = State::WaitingDescribe;
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
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                let desc = RowDescription::parse(payload)?;
                if let Some(ref mut stmt) = self.prepared_stmt {
                    stmt.columns = Some(
                        desc.fields()
                            .iter()
                            .map(|f| ColumnInfo {
                                name: f.name.to_string(),
                                tail: *f.tail,
                            })
                            .collect(),
                    );
                }
                self.state = State::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::NO_DATA => {
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

