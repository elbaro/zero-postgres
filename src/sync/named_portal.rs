//! Named portal for iterative row fetching.

use std::marker::PhantomData;

use crate::conversion::FromRow;
use crate::error::Result;
use crate::handler::{BinaryHandler, CollectHandler};

use super::Conn;

/// Handle to a named portal for iterative row fetching.
///
/// Created by [`Transaction::exec_portal()`]. Use [`execute()`](Self::execute) to retrieve rows
/// in batches. The lifetime parameter ties the portal to the transaction that created it,
/// preventing the transaction from being committed/rolled back while the portal is alive.
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
pub struct NamedPortal<'tx> {
    pub(crate) name: String,
    complete: bool,
    _marker: PhantomData<&'tx ()>,
}

impl<'tx> NamedPortal<'tx> {
    /// Create a new named portal.
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            complete: false,
            _marker: PhantomData,
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
    pub fn execute<H: BinaryHandler>(
        &mut self,
        conn: &mut Conn,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<()> {
        let has_more = conn.lowlevel_execute(&self.name, max_rows, handler)?;
        self.complete = !has_more;
        Ok(())
    }

    /// Execute the portal and collect typed rows.
    ///
    /// Fetches up to `max_rows` rows. Pass 0 to fetch all remaining rows.
    pub fn execute_collect<T: for<'a> FromRow<'a>>(
        &mut self,
        conn: &mut Conn,
        max_rows: u32,
    ) -> Result<Vec<T>> {
        let mut handler = CollectHandler::<T>::new();
        self.execute(conn, max_rows, &mut handler)?;
        Ok(handler.into_rows())
    }

    /// Close the portal and sync.
    ///
    /// This sends Close(Portal) followed by Sync to end the transaction.
    pub fn close(self, conn: &mut Conn) -> Result<()> {
        conn.lowlevel_close_portal(&self.name)?;
        conn.lowlevel_sync()
    }
}
