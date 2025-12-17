//! Typed result handlers.

use crate::conversion::FromRow;
use crate::error::Result;
use crate::protocol::backend::query::{CommandComplete, DataRow, RowDescription};

/// Handler for simple query results (text format).
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
pub trait TextHandler {
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

/// Handler for extended query results (binary format).
///
/// Callback patterns by statement type:
/// - SELECT with rows: `result_start` → `row*` → `result_end`
/// - SELECT with 0 rows: `result_start` → `result_end`
/// - INSERT/UPDATE/DELETE: `result_end` only (with affected row count)
pub trait BinaryHandler {
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

impl TextHandler for DropHandler {
    fn row(&mut self, _cols: RowDescription<'_>, _row: DataRow<'_>) -> Result<()> {
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl BinaryHandler for DropHandler {
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

impl<T: for<'a> FromRow<'a>> TextHandler for CollectHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row(cols.fields(), row)?;
        self.rows.push(typed_row);
        Ok(())
    }
}

impl<T: for<'a> FromRow<'a>> BinaryHandler for CollectHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let typed_row = T::from_row(cols.fields(), row)?;
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

impl<T: for<'a> FromRow<'a>> TextHandler for FirstRowHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        if self.row.is_none() {
            let typed_row = T::from_row(cols.fields(), row)?;
            self.row = Some(typed_row);
        }
        Ok(())
    }
}

impl<T: for<'a> FromRow<'a>> BinaryHandler for FirstRowHandler<T> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        if self.row.is_none() {
            let typed_row = T::from_row(cols.fields(), row)?;
            self.row = Some(typed_row);
        }
        Ok(())
    }
}
