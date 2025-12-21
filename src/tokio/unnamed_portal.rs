//! Unnamed portal for async iterative row fetching.

use crate::error::Result;
use crate::handler::BinaryHandler;

use super::Conn;

/// Handle to an unnamed portal for async iterative row fetching.
///
/// Created by [`Conn::exec_iter()`]. Use [`fetch()`](Self::fetch) to retrieve rows in batches.
pub struct UnnamedPortal<'a> {
    pub(crate) conn: &'a mut Conn,
}

impl<'a> UnnamedPortal<'a> {
    /// Fetch up to `max_rows` rows using the provided handler.
    ///
    /// Returns `Ok(true)` if more rows available (PortalSuspended received).
    /// Returns `Ok(false)` if all rows fetched (CommandComplete received).
    pub async fn fetch<H: BinaryHandler>(
        &mut self,
        max_rows: u32,
        handler: &mut H,
    ) -> Result<bool> {
        self.conn.lowlevel_execute("", max_rows, handler).await
    }
}
