//! Simple query protocol state machine.

use crate::buffer_set::BufferSet;
use crate::error::{Error, Result};
use crate::handler::TextHandler;
use crate::protocol::backend::{
    CommandComplete, DataRow, EmptyQueryResponse, ErrorResponse, RawMessage, ReadyForQuery,
    RowDescription, msg_type,
};
use crate::protocol::frontend::write_query;
use crate::protocol::types::TransactionStatus;

use super::action::{Action, AsyncMessage};
use super::StateMachine;

/// Simple query state machine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Initial,
    WaitingResponse,
    ProcessingRows,
    WaitingReady,
    Finished,
}

/// Simple query protocol state machine.
pub struct SimpleQueryStateMachine<'a, 'q, H> {
    state: State,
    handler: &'a mut H,
    query: &'q str,
    transaction_status: TransactionStatus,
}

impl<'a, 'q, H: TextHandler> SimpleQueryStateMachine<'a, 'q, H> {
    /// Create a new simple query state machine.
    pub fn new(handler: &'a mut H, query: &'q str) -> Self {
        Self {
            state: State::Initial,
            handler,
            query,
            transaction_status: TransactionStatus::Idle,
        }
    }

    fn handle_response(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::ROW_DESCRIPTION => {
                // Store column buffer for later use in row callbacks
                buffer_set.column_buffer.clear();
                buffer_set.column_buffer.extend_from_slice(payload);
                let cols = RowDescription::parse(&buffer_set.column_buffer)?;
                self.handler.result_start(cols)?;
                self.state = State::ProcessingRows;
                Ok(Action::ReadMessage)
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.result_end(complete)?;
                // More commands may follow in a multi-statement query
                self.state = State::WaitingResponse;
                Ok(Action::ReadMessage)
            }
            msg_type::EMPTY_QUERY_RESPONSE => {
                EmptyQueryResponse::parse(payload)?;
                // Empty query string - silently ignore
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
                "Unexpected message in query response: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_rows(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::DATA_ROW => {
                let cols = RowDescription::parse(&buffer_set.column_buffer)?;
                let row = DataRow::parse(payload)?;
                self.handler.row(cols, row)?;
                Ok(Action::ReadMessage)
            }
            msg_type::COMMAND_COMPLETE => {
                let complete = CommandComplete::parse(payload)?;
                self.handler.result_end(complete)?;
                // More commands may follow
                self.state = State::WaitingResponse;
                Ok(Action::ReadMessage)
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

    fn handle_ready(&mut self, buffer_set: &BufferSet) -> Result<Action> {
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

    fn handle_async_message(&self, msg: &RawMessage<'_>) -> Result<Action> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(AsyncMessage::Notice(notice.0)))
            }
            msg_type::PARAMETER_STATUS => {
                let param = crate::protocol::backend::auth::ParameterStatus::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(AsyncMessage::ParameterChanged {
                    name: param.name.to_string(),
                    value: param.value.to_string(),
                }))
            }
            msg_type::NOTIFICATION_RESPONSE => {
                let notification =
                    crate::protocol::backend::auth::NotificationResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(AsyncMessage::Notification {
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

impl<H: TextHandler> StateMachine for SimpleQueryStateMachine<'_, '_, H> {
    fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Initial state: write query to buffer and request send
        if self.state == State::Initial {
            buffer_set.write_buffer.clear();
            write_query(&mut buffer_set.write_buffer, self.query);
            self.state = State::WaitingResponse;
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

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }
}
