//! Common PostgreSQL wire protocol types.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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

/// Big-endian 16-bit unsigned integer for zerocopy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct U16BE([u8; 2]);

impl U16BE {
    /// Create a new U16BE from a native u16.
    pub const fn new(value: u16) -> Self {
        Self(value.to_be_bytes())
    }

    /// Get the native u16 value.
    pub const fn get(self) -> u16 {
        u16::from_be_bytes(self.0)
    }
}

impl From<u16> for U16BE {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl From<U16BE> for u16 {
    fn from(value: U16BE) -> Self {
        value.get()
    }
}

/// Big-endian 32-bit unsigned integer for zerocopy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct U32BE([u8; 4]);

impl U32BE {
    /// Create a new U32BE from a native u32.
    pub const fn new(value: u32) -> Self {
        Self(value.to_be_bytes())
    }

    /// Get the native u32 value.
    pub const fn get(self) -> u32 {
        u32::from_be_bytes(self.0)
    }
}

impl From<u32> for U32BE {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<U32BE> for u32 {
    fn from(value: U32BE) -> Self {
        value.get()
    }
}

/// Big-endian 32-bit signed integer for zerocopy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct I32BE([u8; 4]);

impl I32BE {
    /// Create a new I32BE from a native i32.
    pub const fn new(value: i32) -> Self {
        Self(value.to_be_bytes())
    }

    /// Get the native i32 value.
    pub const fn get(self) -> i32 {
        i32::from_be_bytes(self.0)
    }
}

impl From<i32> for I32BE {
    fn from(value: i32) -> Self {
        Self::new(value)
    }
}

impl From<I32BE> for i32 {
    fn from(value: I32BE) -> Self {
        value.get()
    }
}

/// Big-endian 16-bit signed integer for zerocopy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct I16BE([u8; 2]);

impl I16BE {
    /// Create a new I16BE from a native i16.
    pub const fn new(value: i16) -> Self {
        Self(value.to_be_bytes())
    }

    /// Get the native i16 value.
    pub const fn get(self) -> i16 {
        i16::from_be_bytes(self.0)
    }
}

impl From<i16> for I16BE {
    fn from(value: i16) -> Self {
        Self::new(value)
    }
}

impl From<I16BE> for i16 {
    fn from(value: I16BE) -> Self {
        value.get()
    }
}
