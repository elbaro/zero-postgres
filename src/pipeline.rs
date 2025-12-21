//! Shared pipeline types.

/// A ticket for a queued pipeline operation.
///
/// Created by [`Pipeline::exec`].
/// Claim with [`Pipeline::claim_collect`], [`Pipeline::claim_one`], or [`Pipeline::claim_drop`].
#[derive(Debug, Clone, Copy)]
#[must_use]
pub struct Ticket {
    pub(crate) seq: usize,
}

/// What response sequence to expect for a queued operation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Expectation {
    /// Parse + Bind + Execute: ParseComplete + BindComplete + RowDescription/NoData + DataRow* + terminal
    ParseBindExecute,
    /// Bind + Execute: BindComplete + RowDescription/NoData + DataRow* + terminal
    BindExecute,
}
