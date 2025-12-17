//! Typed handles for pipeline operations.

use std::marker::PhantomData;

/// Handle for a queued prepare operation.
///
/// Created by [`Pipeline::prepare`](super::Pipeline::prepare).
/// Harvest with [`Pipeline::harvest`](super::Pipeline::harvest) to get a [`PreparedStatement`](crate::state::extended::PreparedStatement).
#[derive(Debug)]
pub struct QueuedPrepare {
    pub(super) seq: usize,
    pub(super) stmt_name: String,
}

/// Handle for a queued portal bind operation.
///
/// Created by [`Pipeline::bind`](super::Pipeline::bind).
/// Harvest with [`Pipeline::harvest`](super::Pipeline::harvest) to confirm the bind completed.
#[derive(Debug)]
pub struct QueuedPortal {
    pub(super) seq: usize,
    pub(super) portal_name: String,
    /// Whether we've already sent an execute for this portal (affects RowDescription expectation)
    pub(super) first_execute_done: std::cell::Cell<bool>,
}

/// Handle for a queued execute operation.
///
/// Created by [`Pipeline::execute`](super::Pipeline::execute) or [`Pipeline::exec`](super::Pipeline::exec).
/// Harvest with [`Pipeline::harvest`](super::Pipeline::harvest) to get the execution result.
#[derive(Debug)]
pub struct QueuedExec<T> {
    pub(super) seq: usize,
    pub(super) _phantom: PhantomData<fn() -> T>,
}

/// Result from executing a query.
#[derive(Debug)]
pub struct ExecResult<T> {
    /// The rows returned by the query.
    pub rows: Vec<T>,
    /// True if the portal was suspended (more rows available).
    /// This happens when `max_rows` was set and reached.
    pub suspended: bool,
}

impl<T> ExecResult<T> {
    /// Create a new execution result.
    pub(super) fn new(rows: Vec<T>, suspended: bool) -> Self {
        Self { rows, suspended }
    }
}
