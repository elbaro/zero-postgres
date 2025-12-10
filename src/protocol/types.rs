//! Common PostgreSQL wire protocol types.

/// PostgreSQL Object Identifier (OID)
pub type Oid = u32;

/// Data format code in PostgreSQL protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u16)]
pub enum FormatCode {
    /// Text format (human-readable)
    #[default]
    Text = 0,
    /// Binary format (type-specific packed representation)
    Binary = 1,
}

impl FormatCode {
    /// Create a FormatCode from a raw u16 value.
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => FormatCode::Text,
            1 => FormatCode::Binary,
            _ => FormatCode::Text, // Default to text for unknown values
        }
    }
}

impl From<u16> for FormatCode {
    fn from(value: u16) -> Self {
        Self::from_u16(value)
    }
}

/// Transaction status indicator from ReadyForQuery message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TransactionStatus {
    /// Idle (not in transaction block)
    #[default]
    Idle = b'I',
    /// In transaction block
    InTransaction = b'T',
    /// In failed transaction block (queries will be rejected until rollback)
    Failed = b'E',
}

impl TransactionStatus {
    /// Create a TransactionStatus from a raw byte value.
    pub fn from_byte(value: u8) -> Option<Self> {
        match value {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }

    /// Returns true if currently in a transaction (either active or failed).
    pub fn in_transaction(self) -> bool {
        matches!(self, TransactionStatus::InTransaction | TransactionStatus::Failed)
    }

    /// Returns true if the transaction has failed.
    pub fn is_failed(self) -> bool {
        matches!(self, TransactionStatus::Failed)
    }
}

/// Well-known PostgreSQL type OIDs.
pub mod oid {
    use super::Oid;

    pub const BOOL: Oid = 16;
    pub const BYTEA: Oid = 17;
    pub const NAME: Oid = 19;
    pub const INT8: Oid = 20;
    pub const INT2: Oid = 21;
    pub const INT4: Oid = 23;
    pub const TEXT: Oid = 25;
    pub const FLOAT4: Oid = 700;
    pub const FLOAT8: Oid = 701;
    pub const VARCHAR: Oid = 1043;
    pub const BPCHAR: Oid = 1042;
}

