//! Transaction support for synchronous PostgreSQL connections.

use super::Conn;
use crate::error::{Error, Result};

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
}
