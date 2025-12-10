//! Typed result handlers.

use crate::error::Result;
use crate::protocol::backend::query::{
    CommandComplete, DataRow, FieldDescription, FieldDescriptionTail, RowDescription,
};
use crate::row::FromRow;
use crate::state::extended::BinaryHandler;
use crate::state::simple_query::{ControlFlow, TextHandler};

/// Owned copy of field description for storing between callbacks.
#[derive(Debug, Clone)]
pub struct OwnedFieldDescription {
    pub name: String,
    pub tail: FieldDescriptionTail,
}

impl OwnedFieldDescription {
    /// Convert to borrowed FieldDescription for decoding.
    pub fn as_ref(&self) -> FieldDescription<'_> {
        FieldDescription {
            name: &self.name,
            tail: &self.tail,
        }
    }
}

/// Handler that collects typed rows.
///
/// # Example
///
/// ```ignore
/// let mut handler: TypedCollectHandler<(i32, String)> = TypedCollectHandler::new();
/// conn.query("SELECT id, name FROM users", &mut handler)?;
/// for (id, name) in handler.into_rows() {
///     println!("{}: {}", id, name);
/// }
/// ```
pub struct TypedCollectHandler<T> {
    columns: Vec<OwnedFieldDescription>,
    rows: Vec<T>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedCollectHandler<T> {
    /// Create a new typed collect handler.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            _marker: std::marker::PhantomData,
        }
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

impl<T> Default for TypedCollectHandler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: for<'a> FromRow<'a>> TextHandler for TypedCollectHandler<T> {
    fn columns(&mut self, desc: RowDescription<'_>) -> Result<()> {
        self.columns = desc
            .fields()
            .iter()
            .map(|f| OwnedFieldDescription {
                name: f.name.to_string(),
                tail: *f.tail,
            })
            .collect();
        Ok(())
    }

    fn row(&mut self, row: DataRow<'_>) -> Result<ControlFlow> {
        // Convert owned columns to borrowed for decoding
        let cols: Vec<_> = self.columns.iter().map(|c| c.as_ref()).collect();
        let typed_row = T::from_row(&cols, row)?;
        self.rows.push(typed_row);
        Ok(ControlFlow::Continue)
    }

    fn command_complete(&mut self, _complete: CommandComplete<'_>) -> Result<()> {
        Ok(())
    }

    fn empty_query(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<T: for<'a> FromRow<'a>> BinaryHandler for TypedCollectHandler<T> {
    fn columns(&mut self, desc: RowDescription<'_>) -> Result<()> {
        self.columns = desc
            .fields()
            .iter()
            .map(|f| OwnedFieldDescription {
                name: f.name.to_string(),
                tail: *f.tail,
            })
            .collect();
        Ok(())
    }

    fn row(&mut self, row: DataRow<'_>) -> Result<ControlFlow> {
        let cols: Vec<_> = self.columns.iter().map(|c| c.as_ref()).collect();
        let typed_row = T::from_row(&cols, row)?;
        self.rows.push(typed_row);
        Ok(ControlFlow::Continue)
    }

    fn command_complete(&mut self, _complete: CommandComplete<'_>) -> Result<()> {
        Ok(())
    }
}

/// Handler that collects only the first row.
pub struct TypedFirstRowHandler<T> {
    columns: Vec<OwnedFieldDescription>,
    row: Option<T>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedFirstRowHandler<T> {
    /// Create a new first row handler.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            row: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the first row if present.
    pub fn row(&self) -> Option<&T> {
        self.row.as_ref()
    }

    /// Take the first row.
    pub fn into_row(self) -> Option<T> {
        self.row
    }
}

impl<T> Default for TypedFirstRowHandler<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: for<'a> FromRow<'a>> TextHandler for TypedFirstRowHandler<T> {
    fn columns(&mut self, desc: RowDescription<'_>) -> Result<()> {
        self.columns = desc
            .fields()
            .iter()
            .map(|f| OwnedFieldDescription {
                name: f.name.to_string(),
                tail: *f.tail,
            })
            .collect();
        Ok(())
    }

    fn row(&mut self, row: DataRow<'_>) -> Result<ControlFlow> {
        if self.row.is_none() {
            let cols: Vec<_> = self.columns.iter().map(|c| c.as_ref()).collect();
            let typed_row = T::from_row(&cols, row)?;
            self.row = Some(typed_row);
        }
        // Stop after first row
        Ok(ControlFlow::Stop)
    }

    fn command_complete(&mut self, _complete: CommandComplete<'_>) -> Result<()> {
        Ok(())
    }

    fn empty_query(&mut self) -> Result<()> {
        Ok(())
    }
}
