//! Simple query protocol state machine.

use crate::error::{Error, Result};
use crate::protocol::backend::{
    msg_type, CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, RawMessage,
    ReadyForQuery, RowDescription,
};
use crate::protocol::frontend::write_query;
use crate::protocol::types::TransactionStatus;

use super::action::{Action, AsyncMessage};

/// Control flow for row processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlow {
    /// Continue processing rows
    Continue,
    /// Stop processing (remaining rows will be discarded)
    Stop,
}

/// Handler for simple query results.
pub trait QueryHandler {
    /// Called when column descriptions are received.
    fn columns(&mut self, desc: RowDescription<'_>) -> Result<()>;

    /// Called for each data row.
    ///
    /// Return `ControlFlow::Stop` to stop processing rows early.
    fn row(&mut self, row: DataRow<'_>) -> Result<ControlFlow>;

    /// Called when a command completes.
    fn command_complete(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        let _ = complete;
        Ok(())
    }

    /// Called for empty query response.
    fn empty_query(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Buffer set for state machine operations.
pub struct BufferSet {
    /// Read buffer for incoming messages
    pub read_buffer: Vec<u8>,
    /// Type byte of the last message read
    pub type_byte: u8,
}

impl BufferSet {
    /// Create a new buffer set.
    pub fn new() -> Self {
        Self {
            read_buffer: Vec::with_capacity(8192),
            type_byte: 0,
        }
    }
}

impl Default for BufferSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple query state machine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Initial state - need to send query
    Initial,
    /// Waiting for response
    WaitingResponse,
    /// Processing rows
    ProcessingRows,
    /// Query completed, waiting for ReadyForQuery
    WaitingReady,
    /// Finished
    Finished,
}

/// Simple query protocol state machine.
pub struct SimpleQueryStateMachine<H> {
    state: State,
    handler: H,
    write_buffer: Vec<u8>,
    transaction_status: TransactionStatus,
    skip_rows: bool,
}

impl<H: QueryHandler> SimpleQueryStateMachine<H> {
    /// Create a new simple query state machine.
    pub fn new(handler: H) -> Self {
        Self {
            state: State::Initial,
            handler,
            write_buffer: Vec::new(),
            transaction_status: TransactionStatus::Idle,
            skip_rows: false,
        }
    }

    /// Get the handler.
    pub fn handler(&self) -> &H {
        &self.handler
    }

    /// Get mutable access to the handler.
    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    /// Get the transaction status from the final ReadyForQuery.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Start the query.
    pub fn start(&mut self, query: &str) -> Action<'_> {
        self.write_buffer.clear();
        write_query(&mut self.write_buffer, query);
        self.state = State::WaitingResponse;
        Action::WritePacket(&self.write_buffer)
    }

    /// Process a message from the server.
    ///
    /// The caller should:
    /// 1. Read the message type byte and payload into buffer_set.read_buffer
    /// 2. Set buffer_set.type_byte to the message type
    /// 3. Call this method
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
            // After error, we still need to wait for ReadyForQuery
            self.state = State::WaitingReady;
            return Err(error.into_error());
        }

        match self.state {
            State::WaitingResponse => self.handle_response(buffer_set),
            State::ProcessingRows => self.handle_rows(buffer_set),
            State::WaitingReady => self.handle_ready(buffer_set),
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }

    fn handle_response<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                let desc = RowDescription::parse(payload)?;
                self.handler.columns(desc)?;
                self.state = State::ProcessingRows;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.command_complete(complete)?;
                // More commands may follow in a multi-statement query
                self.state = State::WaitingResponse;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::EMPTY_QUERY_RESPONSE => {
                EmptyQueryResponse::parse(payload)?;
                self.handler.empty_query()?;
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
                "Unexpected message in query response: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_rows<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::DATA_ROW => {
                if !self.skip_rows {
                    let row = DataRow::parse(payload)?;
                    match self.handler.row(row)? {
                        ControlFlow::Continue => {}
                        ControlFlow::Stop => {
                            self.skip_rows = true;
                        }
                    }
                }
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.command_complete(complete)?;
                self.skip_rows = false;
                // More commands may follow
                self.state = State::WaitingResponse;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::READY_FOR_QUERY => {
                let ready = ReadyForQuery::parse(payload)?;
                self.transaction_status = ready.transaction_status().unwrap_or_default();
                self.state = State::Finished;
                Ok(Action::Finished)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected message in row processing: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_ready<'buf>(&mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        if buffer_set.type_byte != msg_type::READY_FOR_QUERY {
            return Err(Error::Protocol(format!(
                "Expected ReadyForQuery, got '{}'",
                buffer_set.type_byte as char
            )));
        }

        let ready = ReadyForQuery::parse(&buffer_set.read_buffer)?;
        self.transaction_status = ready.transaction_status().unwrap_or_default();
        self.state = State::Finished;
        Ok(Action::Finished)
    }

    fn handle_async_message(&self, msg: &RawMessage<'_>) -> Result<Action<'_>> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::Notice(notice.fields)))
            }
            msg_type::PARAMETER_STATUS => {
                let param =
                    crate::protocol::backend::auth::ParameterStatus::parse(msg.payload)?;
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

/// A simple handler that collects all rows into a vector.
#[derive(Debug, Default)]
pub struct CollectHandler {
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    command_tag: Option<String>,
}

impl CollectHandler {
    /// Create a new collect handler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the column names.
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    /// Get the collected rows.
    pub fn rows(&self) -> &[Vec<Option<Vec<u8>>>] {
        &self.rows
    }

    /// Take the collected rows.
    pub fn take_rows(&mut self) -> Vec<Vec<Option<Vec<u8>>>> {
        std::mem::take(&mut self.rows)
    }

    /// Get the command tag from the last command.
    pub fn command_tag(&self) -> Option<&str> {
        self.command_tag.as_deref()
    }
}

impl QueryHandler for CollectHandler {
    fn columns(&mut self, desc: RowDescription<'_>) -> Result<()> {
        self.columns = Some(desc.fields().iter().map(|f| f.name.to_string()).collect());
        Ok(())
    }

    fn row(&mut self, row: DataRow<'_>) -> Result<ControlFlow> {
        let values: Vec<Option<Vec<u8>>> = row.iter().map(|v| v.map(|b| b.to_vec())).collect();
        self.rows.push(values);
        Ok(ControlFlow::Continue)
    }

    fn command_complete(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.command_tag = Some(complete.tag.to_string());
        Ok(())
    }
}

/// A handler that discards all results.
#[derive(Debug, Default)]
pub struct DropHandler {
    rows_affected: Option<u64>,
}

impl DropHandler {
    /// Create a new drop handler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of rows affected (if applicable).
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

impl QueryHandler for DropHandler {
    fn columns(&mut self, _desc: RowDescription<'_>) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, _row: DataRow<'_>) -> Result<ControlFlow> {
        Ok(ControlFlow::Continue)
    }

    fn command_complete(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}
