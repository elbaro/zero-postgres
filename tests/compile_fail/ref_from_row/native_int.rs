//! Test that native integer types (not big-endian) cause a compile error.

use zero_postgres::conversion::ref_row::LengthPrefixed;
use zero_postgres_derive::RefFromRow;

#[derive(RefFromRow)]
#[repr(C, packed)]
struct Invalid {
    // Native i64 doesn't implement FixedWireSize - must use I64BE
    value: LengthPrefixed<i64>,
}

fn main() {}
