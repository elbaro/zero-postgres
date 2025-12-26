//! Sans-I/O state machines for PostgreSQL protocol.
//!
//! These state machines handle the protocol logic without performing any I/O.
//! They produce `Action` values that tell the caller what to do next.

pub mod action;
pub mod batch_prepare;
pub mod connection;
pub mod extended;
pub mod simple_query;

pub use action::{Action, AsyncMessage};
pub use connection::ConnectionStateMachine;
pub use extended::ExtendedQueryStateMachine;
pub use simple_query::SimpleQueryStateMachine;

use crate::buffer_set::BufferSet;
use crate::error::Result;
use crate::protocol::types::TransactionStatus;

/// Trait for state machines that can be driven by a connection.
pub trait StateMachine {
    /// Process input and return the next action to perform.
    ///
    /// The driver should:
    /// 1. Call `step()` to get the next action
    /// 2. Perform the action (read/write/tls handshake)
    /// 3. Repeat until `Action::Finished`
    ///
    /// When `Action::Write` is returned, the driver should write
    /// `buffer_set.write_buffer` to the socket.
    fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action>;

    /// Get the transaction status from the final ReadyForQuery.
    fn transaction_status(&self) -> TransactionStatus;
}
