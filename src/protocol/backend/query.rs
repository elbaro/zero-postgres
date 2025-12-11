//! Query-related backend messages.

use std::mem::size_of;

use zerocopy::byteorder::big_endian::{I16 as I16BE, I32 as I32BE, U16 as U16BE, U32 as U32BE};
use zerocopy::{FromBytes, Immutable, KnownLayout};

use crate::error::{Error, Result};
use crate::protocol::codec::read_cstr;
use crate::protocol::types::{FormatCode, Oid};

/// RowDescription message header.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct RowDescriptionHead {
    /// Number of fields in the row
    pub num_fields: U16BE,
}

/// Fixed-size tail of a field description (18 bytes).
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct FieldDescriptionTail {
    /// Table OID (0 if not a table column)
    pub table_oid: U32BE,
    /// Column attribute number (0 if not a table column)
    pub column_id: I16BE,
    /// Data type OID
    pub type_oid: U32BE,
    /// Type size (-1 for variable, -2 for null-terminated)
    pub type_size: I16BE,
    /// Type modifier (type-specific)
    pub type_modifier: I32BE,
    /// Format code (0=text, 1=binary)
    pub format: U16BE,
}

/// Field description within a RowDescription.
#[derive(Debug, Clone, Copy)]
pub struct FieldDescription<'a> {
    /// Field name
    pub name: &'a str,
    /// Fixed-size metadata
    pub tail: &'a FieldDescriptionTail,
}

impl FieldDescription<'_> {
    /// Table OID (0 if not a table column)
    pub fn table_oid(&self) -> Oid {
        self.tail.table_oid.get()
    }

    /// Column attribute number (0 if not a table column)
    pub fn column_id(&self) -> i16 {
        self.tail.column_id.get()
    }

    /// Data type OID
    pub fn type_oid(&self) -> Oid {
        self.tail.type_oid.get()
    }

    /// Type size (-1 for variable, -2 for null-terminated)
    pub fn type_size(&self) -> i16 {
        self.tail.type_size.get()
    }

    /// Type modifier (type-specific)
    pub fn type_modifier(&self) -> i32 {
        self.tail.type_modifier.get()
    }

    /// Format code (0=text, 1=binary)
    pub fn format(&self) -> FormatCode {
        FormatCode::from_u16(self.tail.format.get())
    }
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

        const TAIL_SIZE: usize = size_of::<FieldDescriptionTail>();

        for _ in 0..num_fields {
            let (name, rest) = read_cstr(data)?;
            let tail = FieldDescriptionTail::ref_from_bytes(&rest[..TAIL_SIZE])
                .map_err(|e| Error::Protocol(format!("FieldDescription tail: {e:?}")))?;

            fields.push(FieldDescription { name, tail });

            data = &rest[TAIL_SIZE..];
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
        let len;
        (len, self.remaining) = self.remaining.split_at_checked(4)?;
        let len = i32::from_be_bytes([len[0], len[1], len[2], len[3]]);

        if len == -1 {
            // NULL value
            Some(None)
        } else {
            let len = len as usize;
            if self.remaining.len() < len {
                return None;
            }

            let value;
            (value, self.remaining) = self.remaining.split_at_checked(len)?;
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
