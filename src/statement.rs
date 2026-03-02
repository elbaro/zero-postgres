//! Statement reference types for polymorphic exec_* methods.

use crate::state::extended::PreparedStatement;

/// A resolved reference to either raw SQL or a prepared statement.
///
/// Returned by [`IntoStatement::statement_ref`] to allow exhaustive
/// matching at call sites, replacing the old `Option`-returning methods
/// that required `unwrap`.
pub enum StatementRef<'a> {
    Sql(&'a str),
    Prepared(&'a PreparedStatement),
}

/// Sealed trait for types that can be used as statement references in exec_* methods.
///
/// This trait is sealed and cannot be implemented outside this crate.
pub trait IntoStatement: private::Sealed {
    /// Return a [`StatementRef`] discriminating raw SQL from a prepared statement.
    fn statement_ref(&self) -> StatementRef<'_>;
}

mod private {
    use crate::state::extended::PreparedStatement;

    pub trait Sealed {}

    impl Sealed for PreparedStatement {}
    impl Sealed for &PreparedStatement {}
    impl Sealed for str {}
    impl Sealed for &str {}
    impl Sealed for &&str {}
}

impl IntoStatement for &PreparedStatement {
    fn statement_ref(&self) -> StatementRef<'_> {
        StatementRef::Prepared(self)
    }
}

impl IntoStatement for &str {
    fn statement_ref(&self) -> StatementRef<'_> {
        StatementRef::Sql(self)
    }
}

impl IntoStatement for &&str {
    fn statement_ref(&self) -> StatementRef<'_> {
        StatementRef::Sql(self)
    }
}

impl IntoStatement for str {
    fn statement_ref(&self) -> StatementRef<'_> {
        StatementRef::Sql(self)
    }
}

impl IntoStatement for PreparedStatement {
    fn statement_ref(&self) -> StatementRef<'_> {
        StatementRef::Prepared(self)
    }
}
