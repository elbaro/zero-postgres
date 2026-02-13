//! Test that missing #[repr(C, packed)] causes a compile error.

use zero_postgres::conversion::ref_row::{LengthPrefixed, I32BE};
use zero_postgres_derive::RefFromRow;

#[derive(RefFromRow)]
struct Invalid {
    value: LengthPrefixed<I32BE>,
}

fn main() {}
