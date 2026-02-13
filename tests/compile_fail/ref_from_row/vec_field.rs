//! Test that Vec fields cause a compile error.

use zero_postgres_derive::RefFromRow;

#[derive(RefFromRow)]
#[repr(C, packed)]
struct Invalid {
    data: Vec<u8>,
}

fn main() {}
