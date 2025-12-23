//! Shared pipeline types.

use crate::state::extended::PreparedStatement;

/// A ticket for a queued pipeline operation.
///
/// Created by [`Pipeline::exec`].
/// Claim with [`Pipeline::claim_collect`], [`Pipeline::claim_one`], or [`Pipeline::claim_drop`].
#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct Ticket<'a> {
    pub(crate) seq: usize,
    /// Reference to prepared statement (for cached RowDescription), None for raw SQL.
    pub(crate) stmt: Option<&'a PreparedStatement>,
}

/// What response sequence to expect for a queued operation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Expectation {
    /// Parse + Bind + Execute: ParseComplete + BindComplete + RowDescription/NoData + DataRow* + terminal
    ParseBindExecute,
    /// Bind + Execute with cached RowDescription: BindComplete + DataRow* + terminal
    BindExecute,
}
