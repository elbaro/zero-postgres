//! Named portal for async iterative row fetching.

use crate::conversion::FromRow;
use crate::error::Result;
use crate::handler::{BinaryHandler, CollectHandler};

use super::Conn;

/// Handle to a named portal for async iterative row fetching.
///
/// Created by [`Conn::exec_portal()`]. Use [`execute()`](Self::execute) to retrieve rows in batches.
/// Unlike [`UnnamedPortal`](super::UnnamedPortal), named portals can coexist with other operations
/// on the connection.
///
/// # Example
///
/// ```ignore
/// let mut portal = conn.exec_portal(&stmt, ()).await?;
///
/// while !portal.is_complete() {
///     let rows: Vec<(i32,)> = portal.execute_collect(&mut conn, 100).await?;
///     process(rows);
/// }
///
/// portal.close(&mut conn).await?;
/// ```
pub struct NamedPortal {
    pub(crate) name: String,
    complete: bool,
}

impl NamedPortal {
    /// Create a new named portal.
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            complete: false,
        }
    }

    /// Get the portal name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if portal execution is complete (no more rows available).
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Execute the portal with a handler.
    ///
    /// Fetches up to `max_rows` rows. Pass 0 to fetch all remaining rows.
    /// Updates internal completion status.
    pub async fn execute<H: BinaryHandler>(
        &mut self,
        conn: &mut Conn,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<()> {
        let has_more = conn.lowlevel_execute(&self.name, max_rows, handler).await?;
        self.complete = !has_more;
        Ok(())
    }

    /// Execute the portal and collect typed rows.
    ///
    /// Fetches up to `max_rows` rows. Pass 0 to fetch all remaining rows.
    pub async fn execute_collect<T: for<'a> FromRow<'a>>(
        &mut self,
        conn: &mut Conn,
        max_rows: u32,
    ) -> Result<Vec<T>> {
        let mut handler = CollectHandler::<T>::new();
        self.execute(conn, max_rows, &mut handler).await?;
        Ok(handler.into_rows())
    }

    /// Close the portal and sync.
    ///
    /// This sends Close(Portal) followed by Sync to end the transaction.
    pub async fn close(self, conn: &mut Conn) -> Result<()> {
        conn.lowlevel_close_portal(&self.name).await?;
        conn.lowlevel_sync().await
    }
}
