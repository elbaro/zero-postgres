//! Statement reference types for polymorphic exec_* methods.

use crate::state::extended::PreparedStatement;

/// Sealed trait for types that can be used as statement references in exec_* methods.
///
/// This trait is sealed and cannot be implemented outside this crate.
pub trait IntoStatement: private::Sealed {
    /// Returns true if this is raw SQL (needs Parse message).
    fn needs_parse(&self) -> bool;

    /// Get the SQL string if this is raw SQL.
    fn as_sql(&self) -> Option<&str>;

    /// Get the prepared statement if this is a prepared statement reference.
    fn as_prepared(&self) -> Option<&PreparedStatement>;
}

mod private {
    use crate::state::extended::PreparedStatement;

    pub trait Sealed {}

    impl Sealed for &PreparedStatement {}
    impl Sealed for &str {}
    impl Sealed for &&str {}
}

impl IntoStatement for &PreparedStatement {
    fn needs_parse(&self) -> bool {
        false
    }

    fn as_sql(&self) -> Option<&str> {
        None
    }

    fn as_prepared(&self) -> Option<&PreparedStatement> {
        Some(self)
    }
}

impl IntoStatement for &str {
    fn needs_parse(&self) -> bool {
        true
    }

    fn as_sql(&self) -> Option<&str> {
        Some(self)
    }

    fn as_prepared(&self) -> Option<&PreparedStatement> {
        None
    }
}

impl IntoStatement for &&str {
    fn needs_parse(&self) -> bool {
        true
    }

    fn as_sql(&self) -> Option<&str> {
        Some(self)
    }

    fn as_prepared(&self) -> Option<&PreparedStatement> {
        None
    }
}
