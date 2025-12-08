//! Sans-I/O state machines for PostgreSQL protocol.
//!
//! These state machines handle the protocol logic without performing any I/O.
//! They produce `Action` values that tell the caller what to do next.

pub mod action;
pub mod connection;
pub mod extended;
pub mod simple_query;

pub use action::{Action, AsyncMessage};
pub use connection::{ConnectionState, ConnectionStateMachine};
pub use extended::ExtendedQueryStateMachine;
pub use simple_query::SimpleQueryStateMachine;
