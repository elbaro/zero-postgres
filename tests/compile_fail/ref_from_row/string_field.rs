//! Test that String fields cause a compile error.

use zero_postgres_derive::RefFromRow;

#[derive(RefFromRow)]
#[repr(C, packed)]
struct Invalid {
    name: String,
}

fn main() {}
