use std::num::NonZeroU32;
use std::sync::Arc;

use diesel::backend::Backend;
use diesel::pg::{Pg, PgValue, TypeOidLookup};
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

use super::cursor::ColumnInfo;

#[expect(clippy::field_scoped_visibility_modifiers)]
pub struct ZeroPgRow {
    pub(in crate::diesel) columns: Arc<[ColumnInfo]>,
    pub(in crate::diesel) values: Vec<Option<Vec<u8>>>,
}

impl RowSealed for ZeroPgRow {}

impl<'a> Row<'a, Pg> for ZeroPgRow {
    type Field<'f>
        = ZeroPgField<'f>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.columns.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(ZeroPgField {
            col_info: &self.columns[idx],
            value: self.values[idx].as_deref(),
        })
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for ZeroPgRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        (idx < self.columns.len()).then_some(idx)
    }
}

impl<'a> RowIndex<&'a str> for ZeroPgRow {
    fn idx(&self, idx: &'a str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == idx)
    }
}

pub struct ZeroPgField<'a> {
    col_info: &'a ColumnInfo,
    value: Option<&'a [u8]>,
}

impl TypeOidLookup for ZeroPgField<'_> {
    fn lookup(&self) -> NonZeroU32 {
        // OID 0 is invalid in PostgreSQL; all real types have non-zero OIDs.
        #[expect(clippy::unwrap_used)]
        NonZeroU32::new(self.col_info.type_oid).unwrap()
    }
}

impl<'a> Field<'a, Pg> for ZeroPgField<'a> {
    fn field_name(&self) -> Option<&str> {
        Some(&self.col_info.name)
    }

    fn value(&self) -> Option<<Pg as Backend>::RawValue<'_>> {
        self.value.map(|raw| PgValue::new(raw, self))
    }

    fn is_null(&self) -> bool {
        self.value.is_none()
    }
}
