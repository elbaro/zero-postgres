//! Unnamed portal for async iterative row fetching.

use crate::conversion::FromRow;
use crate::error::Result;
use crate::handler::{ExtendedHandler, ForEachHandler};

use super::Conn;

/// Handle to an unnamed portal for async iterative row fetching.
///
/// Created by [`Conn::exec_portal()`]. Use [`exec()`](Self::exec) to retrieve rows in batches.
pub struct UnnamedPortal<'a> {
    pub(crate) conn: &'a mut Conn,
}

impl<'a> UnnamedPortal<'a> {
    /// Execute the portal to fetch up to `max_rows` rows using the provided handler.
    ///
    /// Returns `Ok(true)` if more rows available (PortalSuspended received).
    /// Returns `Ok(false)` if all rows fetched (CommandComplete received).
    pub async fn exec<H: ExtendedHandler>(
        &mut self,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<bool> {
        self.conn.execute_portal_inner("", max_rows, handler).await
    }

    /// Execute the portal and call a closure for each row.
    ///
    /// Returns `Ok(true)` if more rows available (PortalSuspended received).
    /// Returns `Ok(false)` if all rows fetched (CommandComplete received).
    pub async fn exec_foreach<T: for<'b> FromRow<'b>, F: FnMut(T) -> Result<()>>(
        &mut self,
        max_rows: u32,
        f: F,
    ) -> Result<bool> {
        let mut handler = ForEachHandler::<T, F>::new(f);
        self.exec(max_rows, &mut handler).await
    }
}
