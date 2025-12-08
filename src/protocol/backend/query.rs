//! Query-related backend messages.

use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::error::{Error, Result};
use crate::protocol::codec::{read_cstr, read_i16, read_i32, read_u16, read_u32};
use crate::protocol::types::{FormatCode, Oid};
use zerocopy::byteorder::big_endian::U16 as U16BE;

/// RowDescription message header.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct RowDescriptionHead {
    /// Number of fields in the row
    pub num_fields: U16BE,
}

/// Field description within a RowDescription.
#[derive(Debug, Clone)]
pub struct FieldDescription<'a> {
    /// Field name
    pub name: &'a str,
    /// Table OID (0 if not a table column)
    pub table_oid: Oid,
    /// Column attribute number (0 if not a table column)
    pub column_id: i16,
    /// Data type OID
    pub type_oid: Oid,
    /// Type size (-1 for variable, -2 for null-terminated)
    pub type_size: i16,
    /// Type modifier (type-specific)
    pub type_modifier: i32,
    /// Format code (0=text, 1=binary)
    pub format: FormatCode,
}

/// RowDescription message - describes the columns in a result set.
#[derive(Debug)]
pub struct RowDescription<'a> {
    fields: Vec<FieldDescription<'a>>,
}

impl<'a> RowDescription<'a> {
    /// Parse a RowDescription message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let head = RowDescriptionHead::ref_from_bytes(&payload[..2])
            .map_err(|e| Error::Protocol(format!("RowDescription header: {e:?}")))?;

        let num_fields = head.num_fields.get() as usize;
        let mut fields = Vec::with_capacity(num_fields);
        let mut data = &payload[2..];

        for _ in 0..num_fields {
            let (name, rest) = read_cstr(data)?;
            let (table_oid, rest) = read_u32(rest)?;
            let (column_id, rest) = read_i16(rest)?;
            let (type_oid, rest) = read_u32(rest)?;
            let (type_size, rest) = read_i16(rest)?;
            let (type_modifier, rest) = read_i32(rest)?;
            let (format_code, rest) = read_u16(rest)?;

            fields.push(FieldDescription {
                name,
                table_oid,
                column_id,
                type_oid,
                type_size,
                type_modifier,
                format: FormatCode::from_u16(format_code),
            });

            data = rest;
        }

        Ok(Self { fields })
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Get field descriptions.
    pub fn fields(&self) -> &[FieldDescription<'a>] {
        &self.fields
    }

    /// Iterate over field descriptions.
    pub fn iter(&self) -> impl Iterator<Item = &FieldDescription<'a>> {
        self.fields.iter()
    }
}

/// DataRow message header.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct DataRowHead {
    /// Number of columns
    pub num_columns: U16BE,
}

/// DataRow message - contains a single row of data.
#[derive(Debug, Clone, Copy)]
pub struct DataRow<'a> {
    /// Number of columns
    num_columns: u16,
    /// Column data (after the column count)
    columns_data: &'a [u8],
}

impl<'a> DataRow<'a> {
    /// Parse a DataRow message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let head = DataRowHead::ref_from_bytes(&payload[..2])
            .map_err(|e| Error::Protocol(format!("DataRow header: {e:?}")))?;

        Ok(Self {
            num_columns: head.num_columns.get(),
            columns_data: &payload[2..],
        })
    }

    /// Get the number of columns.
    pub fn len(&self) -> usize {
        self.num_columns as usize
    }

    /// Check if there are no columns.
    pub fn is_empty(&self) -> bool {
        self.num_columns == 0
    }

    /// Create an iterator over column values.
    ///
    /// Each item is `Option<&[u8]>` where `None` represents NULL.
    pub fn iter(&self) -> DataRowIter<'a> {
        DataRowIter {
            remaining: self.columns_data,
        }
    }

    /// Get a column value by index.
    ///
    /// Returns `None` if the column is NULL, `Some(bytes)` otherwise.
    pub fn get(&self, index: usize) -> Option<Option<&'a [u8]>> {
        self.iter().nth(index)
    }
}

/// Iterator over column values in a DataRow.
#[derive(Debug, Clone)]
pub struct DataRowIter<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for DataRowIter<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.len() < 4 {
            return None;
        }

        let len = i32::from_be_bytes([
            self.remaining[0],
            self.remaining[1],
            self.remaining[2],
            self.remaining[3],
        ]);
        self.remaining = &self.remaining[4..];

        if len == -1 {
            // NULL value
            Some(None)
        } else {
            let len = len as usize;
            if self.remaining.len() < len {
                return None;
            }
            let value = &self.remaining[..len];
            self.remaining = &self.remaining[len..];
            Some(Some(value))
        }
    }
}

/// CommandComplete message - indicates successful completion of a command.
#[derive(Debug, Clone, Copy)]
pub struct CommandComplete<'a> {
    /// Command tag (e.g., "SELECT 5", "INSERT 0 1", "UPDATE 10")
    pub tag: &'a str,
}

impl<'a> CommandComplete<'a> {
    /// Parse a CommandComplete message from payload bytes.
    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        let (tag, _) = read_cstr(payload)?;
        Ok(Self { tag })
    }

    /// Parse the number of rows affected from the command tag.
    ///
    /// Returns `Some(count)` for commands like SELECT, INSERT, UPDATE, DELETE.
    /// Returns `None` for other commands or parse failures.
    pub fn rows_affected(&self) -> Option<u64> {
        // Command tags are like:
        // - "SELECT 5"
        // - "INSERT 0 1" (oid, rows)
        // - "UPDATE 10"
        // - "DELETE 3"
        // - "COPY 5"
        let parts: Vec<&str> = self.tag.split_whitespace().collect();

        match parts.as_slice() {
            ["SELECT", count] => count.parse().ok(),
            ["INSERT", _oid, count] => count.parse().ok(),
            ["UPDATE", count] => count.parse().ok(),
            ["DELETE", count] => count.parse().ok(),
            ["COPY", count] => count.parse().ok(),
            ["MOVE", count] => count.parse().ok(),
            ["FETCH", count] => count.parse().ok(),
            _ => None,
        }
    }

    /// Get the command name from the tag.
    pub fn command(&self) -> Option<&str> {
        self.tag.split_whitespace().next()
    }
}

/// EmptyQueryResponse message - response to an empty query string.
#[derive(Debug, Clone, Copy)]
pub struct EmptyQueryResponse;

impl EmptyQueryResponse {
    /// Parse an EmptyQueryResponse message from payload bytes.
    pub fn parse(_payload: &[u8]) -> Result<Self> {
        Ok(Self)
    }
}
