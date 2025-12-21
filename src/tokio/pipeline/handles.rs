//! Typed handles for async pipeline operations.

/// A ticket for a queued pipeline operation.
///
/// Created by [`Pipeline::exec`](super::Pipeline::exec).
/// Claim with [`Pipeline::claim_collect`](super::Pipeline::claim_collect),
/// [`Pipeline::claim_one`](super::Pipeline::claim_one), or
/// [`Pipeline::claim_drop`](super::Pipeline::claim_drop).
#[derive(Debug)]
pub struct Ticket {
    pub(super) seq: usize,
}
