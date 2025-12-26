//! Batch prepare state machine.
//!
//! Used by `prepare_batch` to prepare multiple statements in a single round-trip.

use crate::buffer_set::BufferSet;
use crate::error::{Error, Result};
use crate::protocol::backend::{
    ErrorResponse, NoData, ParameterDescription, ParseComplete, RawMessage, ReadyForQuery, msg_type,
};
use crate::protocol::frontend::{write_describe_statement, write_parse, write_sync};
use crate::protocol::types::TransactionStatus;

use super::action::Action;
use super::extended::PreparedStatement;

/// State for batch prepare flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Initial,
    Processing,
    Finished,
}

/// State machine for batch prepare (Parse + Describe)* + Sync.
///
/// Prepares multiple statements in a single round-trip by sending all
/// Parse and DescribeStatement messages followed by a single Sync.
pub struct BatchPrepareStateMachine {
    state: State,
    /// Statements being prepared
    statements: Vec<PreparedStatement>,
    /// Current statement index we're processing responses for
    current_stmt: usize,
    transaction_status: TransactionStatus,
}

impl BatchPrepareStateMachine {
    /// Create a new batch prepare state machine.
    ///
    /// Writes all Parse + DescribeStatement messages followed by Sync to the buffer.
    pub fn new(buffer_set: &mut BufferSet, queries: &[&str], start_idx: u64) -> Self {
        buffer_set.write_buffer.clear();

        let mut statements = Vec::with_capacity(queries.len());

        for (i, query) in queries.iter().enumerate() {
            let idx = start_idx + i as u64;
            let stmt_name = format!("_zero_s_{}", idx);
            write_parse(&mut buffer_set.write_buffer, &stmt_name, query, &[]);
            write_describe_statement(&mut buffer_set.write_buffer, &stmt_name);
            statements.push(PreparedStatement {
                idx,
                param_oids: Vec::new(),
                row_desc_payload: None,
            });
        }

        write_sync(&mut buffer_set.write_buffer);

        Self {
            state: State::Initial,
            statements,
            current_stmt: 0,
            transaction_status: TransactionStatus::Idle,
        }
    }

    /// Take the prepared statements after completion.
    pub fn take_statements(&mut self) -> Vec<PreparedStatement> {
        std::mem::take(&mut self.statements)
    }

    /// Get the transaction status after completion.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Process input and return the next action.
    pub fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Initial state: write buffer was pre-filled by constructor
        if self.state == State::Initial {
            self.state = State::Processing;
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
            State::Processing => match type_byte {
                msg_type::PARSE_COMPLETE => {
                    ParseComplete::parse(&buffer_set.read_buffer)?;
                    Ok(Action::ReadMessage)
                }
                msg_type::PARAMETER_DESCRIPTION => {
                    let param_desc = ParameterDescription::parse(&buffer_set.read_buffer)?;
                    if self.current_stmt < self.statements.len() {
                        self.statements[self.current_stmt].param_oids = param_desc.oids().to_vec();
                    }
                    Ok(Action::ReadMessage)
                }
                msg_type::ROW_DESCRIPTION => {
                    if self.current_stmt < self.statements.len() {
                        self.statements[self.current_stmt].row_desc_payload =
                            Some(buffer_set.read_buffer.clone());
                    }
                    self.current_stmt += 1;
                    Ok(Action::ReadMessage)
                }
                msg_type::NO_DATA => {
                    NoData::parse(&buffer_set.read_buffer)?;
                    // Statement doesn't return rows
                    self.current_stmt += 1;
                    Ok(Action::ReadMessage)
                }
                msg_type::READY_FOR_QUERY => {
                    let ready = ReadyForQuery::parse(&buffer_set.read_buffer)?;
                    self.transaction_status = ready.transaction_status().unwrap_or_default();
                    self.state = State::Finished;
                    Ok(Action::Finished)
                }
                _ => Err(Error::Protocol(format!(
                    "Unexpected message in batch prepare: '{}'",
                    type_byte as char
                ))),
            },
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }
}
