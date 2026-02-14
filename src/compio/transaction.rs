//! Transaction support for asynchronous PostgreSQL connections.

use super::Conn;
use super::named_portal::NamedPortal;
use crate::conversion::ToParams;
use crate::error::{Error, Result};
use crate::statement::IntoStatement;

/// A PostgreSQL transaction for the asynchronous connection.
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
    pub async fn commit(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::InvalidUsage(format!(
                "connection mismatch: expected {}, got {}",
                self.connection_id, actual
            )));
        }
        conn.query_drop("COMMIT").await?;
        Ok(())
    }

    /// Rollback the transaction.
    pub async fn rollback(self, conn: &mut Conn) -> Result<()> {
        let actual = conn.connection_id();
        if self.connection_id != actual {
            return Err(Error::InvalidUsage(format!(
                "connection mismatch: expected {}, got {}",
                self.connection_id, actual
            )));
        }
        conn.query_drop("ROLLBACK").await?;
        Ok(())
    }

    /// Create a named portal for iterative row fetching within this transaction.
    pub async fn exec_portal_named<S: IntoStatement, P: ToParams>(
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
        let result = conn
            .create_named_portal(&portal_name, &statement, &params)
            .await;

        if let Err(e) = &result {
            if e.is_connection_broken() {
                conn.is_broken = true;
            }
            return Err(result.unwrap_err());
        }

        Ok(NamedPortal::new(portal_name))
    }
}
