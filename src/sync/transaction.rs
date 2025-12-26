//! Transaction support for synchronous PostgreSQL connections.

use super::Conn;
use super::named_portal::NamedPortal;
use crate::conversion::ToParams;
use crate::error::{Error, Result};
use crate::statement::IntoStatement;

/// A PostgreSQL transaction for the synchronous connection.
///
/// This struct provides transaction control. The connection is passed
/// to `commit` and `rollback` methods to execute the transaction commands.
pub struct Transaction {
    connection_id: u32,
}

impl Transaction {
    /// Create a new transaction (internal use only).
    pub(crate) fn new(connection_id: u32) -> Self {
        Self { connection_id }
    }

    /// Commit the transaction.
    ///
    /// This consumes the transaction and sends a COMMIT statement to the server.
    /// The connection must be passed as an argument to execute the commit.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidUsage` if the connection is not the same
    /// as the one that started the transaction.
    pub fn commit(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::InvalidUsage(format!(
                "connection mismatch: expected {}, got {}",
                self.connection_id, actual
            )));
        }
        conn.query_drop("COMMIT")?;
        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// This consumes the transaction and sends a ROLLBACK statement to the server.
    /// The connection must be passed as an argument to execute the rollback.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidUsage` if the connection is not the same
    /// as the one that started the transaction.
    pub fn rollback(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::InvalidUsage(format!(
                "connection mismatch: expected {}, got {}",
                self.connection_id, actual
            )));
        }
        conn.query_drop("ROLLBACK")?;
        Ok(())
    }

    /// Create a named portal for iterative row fetching within this transaction.
    ///
    /// Named portals are safe to use within an explicit transaction because
    /// SYNC messages do not destroy them (only COMMIT/ROLLBACK does).
    ///
    /// The statement can be either:
    /// - A `&PreparedStatement` returned from `conn.prepare()`
    /// - A raw SQL `&str` for one-shot execution
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tx = conn.begin()?;
    /// let mut portal = tx.exec_portal(&mut conn, &stmt, ())?;
    ///
    /// while !portal.is_complete() {
    ///     let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 100)?;
    ///     process(rows);
    /// }
    ///
    /// portal.close(&mut conn)?;
    /// tx.commit(&mut conn)?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidUsage` if the connection is not the same
    /// as the one that started the transaction.
    pub fn exec_portal<S: IntoStatement, P: ToParams>(
        &self,
        conn: &mut Conn,
        statement: S,
        params: P,
    ) -> Result<NamedPortal<'_>> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::InvalidUsage(format!(
                "connection mismatch: expected {}, got {}",
                self.connection_id, actual
            )));
        }

        let portal_name = conn.next_portal_name();
        let result = conn.create_named_portal(&portal_name, &statement, &params);

        if let Err(e) = &result {
            if e.is_connection_broken() {
                conn.is_broken = true;
            }
            return Err(result.unwrap_err());
        }

        Ok(NamedPortal::new(portal_name))
    }
}
