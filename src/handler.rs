//! Handlers define what to do with received column definition and row packets.

use crate::conversion::FromRow;
use crate::error::Result;
use crate::protocol::backend::query::{CommandComplete, DataRow, RowDescription};
use crate::state::action::AsyncMessage;

/// Handler for simple query protocol.
///
/// Callback patterns by statement type:
/// - SELECT with rows: `result_start` → `row*` → `result_end`
/// - SELECT with 0 rows: `result_start` → `result_end`
/// - INSERT/UPDATE/DELETE: `result_end` only (with affected row count)
///
/// For multi-statement queries like `"SELECT 1; UPDATE foo SET x=1"`:
/// ```text
/// result_start → row* → result_end   // SELECT 1
/// result_end                          // UPDATE
/// ```
pub trait SimpleHandler {
    /// Called when a result set begins.
    fn result_start(&mut self, cols: RowDescription<'_>) -> Result<()> {
        let _ = cols;
        Ok(())
    }

    /// Called for each data row.
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()>;

    /// Called when a result set ends.
    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        let _ = complete;
        Ok(())
    }
}

/// Handler for extended query protocol.
///
/// Callback patterns by statement type:
/// - SELECT with rows: `result_start` → `row*` → `result_end`
/// - SELECT with 0 rows: `result_start` → `result_end`
/// - INSERT/UPDATE/DELETE: `result_end` only (with affected row count)
pub trait ExtendedHandler {
    /// Called when a result set begins.
    fn result_start(&mut self, cols: RowDescription<'_>) -> Result<()> {
        let _ = cols;
        Ok(())
    }

    /// Called for each data row.
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()>;

    /// Called when a result set ends.
    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        let _ = complete;
        Ok(())
    }
}

/// A handler that discards all results.
#[derive(Debug, Default)]
pub struct DropHandler {
    rows_affected: Option<u64>,
}

impl DropHandler {
    /// Create a new drop handler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of rows affected (if applicable).
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

impl SimpleHandler for DropHandler {
    fn row(&mut self, _cols: RowDescription<'_>, _row: DataRow<'_>) -> Result<()> {
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl ExtendedHandler for DropHandler {
    fn row(&mut self, _cols: RowDescription<'_>, _row: DataRow<'_>) -> Result<()> {
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

/// Handler that collects typed rows.
///
/// # Example
///
/// ```ignore
/// let mut handler: CollectHandler<(i32, String)> = CollectHandler::new();
/// conn.query("SELECT id, name FROM users", &mut handler)?;
/// for (id, name) in handler.into_rows() {
///     println!("{}: {}", id, name);
/// }
/// ```
#[derive(Default)]
pub struct CollectHandler<T> {
    rows: Vec<T>,
}

impl<T> CollectHandler<T> {
    /// Create a new collect handler.
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Get collected rows.
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// Take collected rows.
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }

    /// Get the number of collected rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if no rows were collected.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<T: for<'a> FromRow<'a>> SimpleHandler for CollectHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row_text(cols.fields(), row)?;
        self.rows.push(typed_row);
        Ok(())
    }
}

impl<T: for<'a> FromRow<'a>> ExtendedHandler for CollectHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row_binary(cols.fields(), row)?;
        self.rows.push(typed_row);
        Ok(())
    }
}

/// Handler that collects only the first row.
#[derive(Default)]
pub struct FirstRowHandler<T> {
    row: Option<T>,
}

impl<T> FirstRowHandler<T> {
    /// Create a new first row handler.
    pub fn new() -> Self {
        Self { row: None }
    }

    /// Get the first row if present.
    pub fn get(&self) -> Option<&T> {
        self.row.as_ref()
    }

    /// Take the first row.
    pub fn into_row(self) -> Option<T> {
        self.row
    }
}

impl<T: for<'a> FromRow<'a>> SimpleHandler for FirstRowHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        if self.row.is_none() {
            let typed_row = T::from_row_text(cols.fields(), row)?;
            self.row = Some(typed_row);
        }
        Ok(())
    }
}

impl<T: for<'a> FromRow<'a>> ExtendedHandler for FirstRowHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        if self.row.is_none() {
            let typed_row = T::from_row_binary(cols.fields(), row)?;
            self.row = Some(typed_row);
        }
        Ok(())
    }
}

/// Handler that calls a closure for each row.
pub struct ForEachHandler<T, F> {
    f: F,
    _marker: std::marker::PhantomData<T>,
}

impl<T, F> ForEachHandler<T, F>
where
    F: FnMut(T) -> Result<()>,
{
    /// Create a new foreach handler.
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: for<'a> FromRow<'a>, F: FnMut(T) -> Result<()>> SimpleHandler for ForEachHandler<T, F> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row_text(cols.fields(), row)?;
        (self.f)(typed_row)
    }
}

impl<T: for<'a> FromRow<'a>, F: FnMut(T) -> Result<()>> ExtendedHandler for ForEachHandler<T, F> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row_binary(cols.fields(), row)?;
        (self.f)(typed_row)
    }
}

/// A handler that calls a closure for each row using zero-copy RefFromRow.
///
/// Unlike `ForEachHandler`, this handler uses `RefFromRow` to decode rows
/// as zero-copy references into the buffer. The closure receives a reference
/// to the decoded struct.
///
/// # Requirements
///
/// - The row type must implement `RefFromRow`
/// - All struct fields must use `LengthPrefixed<T>` with big-endian types
/// - All columns must be `NOT NULL`
pub struct ForEachRefHandler<Row, F> {
    f: F,
    _marker: std::marker::PhantomData<Row>,
}

impl<Row, F> ForEachRefHandler<Row, F> {
    pub fn new(f: F) -> Self {
        Self {
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Row, F> ExtendedHandler for ForEachRefHandler<Row, F>
where
    Row: for<'a> crate::conversion::ref_row::RefFromRow<'a>,
    F: for<'a> FnMut(&'a Row) -> Result<()>,
{
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let parsed = Row::ref_from_row_binary(cols.fields(), row)?;
        (self.f)(parsed)
    }
}

/// Handler for asynchronous messages from the server.
///
/// These messages can arrive at any time during query execution:
/// - `Notification` - from LISTEN/NOTIFY
/// - `Notice` - warnings and informational messages
/// - `ParameterChanged` - server parameter updates
///
/// # Example
///
/// ```ignore
/// use zero_postgres::{sync::Conn, AsyncMessage};
///
/// let mut conn = Conn::new(opts)?;
///
/// conn.set_async_message_handler(|msg: &AsyncMessage| {
///     match msg {
///         AsyncMessage::Notification { channel, payload, .. } => {
///             println!("Notification on {}: {}", channel, payload);
///         }
///         AsyncMessage::Notice(err) => {
///             println!("Notice: {:?}", err);
///         }
///         AsyncMessage::ParameterChanged { name, value } => {
///             println!("Parameter {} changed to {}", name, value);
///         }
///     }
/// });
///
/// // Subscribe to a channel
/// conn.query_drop("LISTEN my_channel")?;
///
/// // Notifications will be delivered to the handler during any query
/// conn.query_drop("")?; // empty query to poll for notifications
/// ```
pub trait AsyncMessageHandler: Send {
    /// Handle an asynchronous message from the server.
    fn handle(&mut self, msg: &AsyncMessage);
}

impl<F: FnMut(&AsyncMessage) + Send> AsyncMessageHandler for F {
    fn handle(&mut self, msg: &AsyncMessage) {
        self(msg)
    }
}
