use std::sync::Arc;

use diesel::QueryResult;

use crate::handler::ExtendedHandler;
use crate::protocol::backend::query::{CommandComplete, DataRow, RowDescription};

use super::row::ZeroPgRow;

pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
}

pub struct Cursor {
    columns: Arc<[ColumnInfo]>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    current: usize,
}

impl Cursor {
    pub(in crate::diesel) fn new(
        columns: Arc<[ColumnInfo]>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Self {
        Self {
            columns,
            rows,
            current: 0,
        }
    }
}

impl Iterator for Cursor {
    type Item = QueryResult<ZeroPgRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.rows.len() {
            return None;
        }
        let idx = self.current;
        self.current += 1;
        let values = std::mem::take(&mut self.rows[idx]);
        Some(Ok(ZeroPgRow {
            columns: Arc::clone(&self.columns),
            values,
        }))
    }
}

pub(in crate::diesel) struct CollectRawHandler {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

impl CollectRawHandler {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl ExtendedHandler for CollectRawHandler {
    fn result_start(&mut self, cols: RowDescription<'_>) -> crate::Result<()> {
        self.columns = cols
            .iter()
            .map(|f| ColumnInfo {
                name: f.name.to_owned(),
                type_oid: f.type_oid(),
            })
            .collect();
        Ok(())
    }

    fn row(&mut self, _cols: RowDescription<'_>, row: DataRow<'_>) -> crate::Result<()> {
        let values: Vec<Option<Vec<u8>>> = row.iter().map(|v| v.map(|b| b.to_vec())).collect();
        self.rows.push(values);
        Ok(())
    }

    fn result_end(&mut self, _complete: CommandComplete<'_>) -> crate::Result<()> {
        Ok(())
    }
}
