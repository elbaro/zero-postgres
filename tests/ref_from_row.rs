//! Tests for RefFromRow zero-copy row decoding.

use zero_postgres::conversion::ref_row::{
    FixedWireSize, I16BE, I32BE, I64BE, LengthPrefixed, U16BE, U32BE, U64BE,
};
use zerocopy::{FromBytes, Immutable, KnownLayout};

/// Test that FixedWireSize is implemented for all expected types.
#[test]
fn fixed_wire_size_primitives() {
    assert_eq!(<i8 as FixedWireSize>::WIRE_SIZE, 1);
    assert_eq!(<u8 as FixedWireSize>::WIRE_SIZE, 1);
    assert_eq!(<I16BE as FixedWireSize>::WIRE_SIZE, 2);
    assert_eq!(<U16BE as FixedWireSize>::WIRE_SIZE, 2);
    assert_eq!(<I32BE as FixedWireSize>::WIRE_SIZE, 4);
    assert_eq!(<U32BE as FixedWireSize>::WIRE_SIZE, 4);
    assert_eq!(<I64BE as FixedWireSize>::WIRE_SIZE, 8);
    assert_eq!(<U64BE as FixedWireSize>::WIRE_SIZE, 8);
}

/// Test LengthPrefixed wire sizes.
#[test]
fn length_prefixed_wire_size() {
    // LengthPrefixed adds 4 bytes for the length prefix
    assert_eq!(<LengthPrefixed<I16BE> as FixedWireSize>::WIRE_SIZE, 6);
    assert_eq!(<LengthPrefixed<I32BE> as FixedWireSize>::WIRE_SIZE, 8);
    assert_eq!(<LengthPrefixed<I64BE> as FixedWireSize>::WIRE_SIZE, 12);
    assert_eq!(<LengthPrefixed<U32BE> as FixedWireSize>::WIRE_SIZE, 8);
}

/// Test zerocopy parsing of big-endian integers.
#[test]
fn big_endian_parsing() {
    // i32 value 0x12345678 in big-endian
    let data: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
    let value: &I32BE = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(value.get(), 0x12345678);

    // i64 value
    let data: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let value: &I64BE = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(value.get(), 0x0102030405060708);
}

/// Test LengthPrefixed parsing.
#[test]
fn length_prefixed_parsing() {
    // Wire format: [len=4 (BE)][value=42 (BE)]
    let mut data = [0u8; 8];
    data[0..4].copy_from_slice(&4_i32.to_be_bytes()); // length = 4
    data[4..8].copy_from_slice(&42_i32.to_be_bytes()); // value = 42

    let prefixed: &LengthPrefixed<I32BE> = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(prefixed.len(), 4);
    assert!(!prefixed.is_null());
    assert_eq!(prefixed.get().get(), 42);
}

/// Test LengthPrefixed with i64.
#[test]
fn length_prefixed_i64() {
    let value: i64 = 0x0102030405060708;
    let mut data = [0u8; 12];
    data[0..4].copy_from_slice(&8_i32.to_be_bytes()); // length = 8
    data[4..12].copy_from_slice(&value.to_be_bytes());

    let prefixed: &LengthPrefixed<I64BE> = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(prefixed.len(), 8);
    assert_eq!(prefixed.get().get(), value);
}

/// Test a packed struct with multiple LengthPrefixed fields.
#[test]
fn packed_struct_with_length_prefixed() {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct TestRow {
        col1: LengthPrefixed<I32BE>, // 4 + 4 = 8 bytes
        col2: LengthPrefixed<I64BE>, // 4 + 8 = 12 bytes
    }

    // Total size: 8 + 12 = 20 bytes
    assert_eq!(std::mem::size_of::<TestRow>(), 20);

    // Create test data simulating PostgreSQL wire format
    let mut data = [0u8; 20];
    // Column 1: INT4 = 42
    data[0..4].copy_from_slice(&4_i32.to_be_bytes());
    data[4..8].copy_from_slice(&42_i32.to_be_bytes());
    // Column 2: INT8 = 12345
    data[8..12].copy_from_slice(&8_i32.to_be_bytes());
    data[12..20].copy_from_slice(&12345_i64.to_be_bytes());

    let row: &TestRow = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(row.col1.len(), 4);
    assert_eq!(row.col1.get().get(), 42);
    assert_eq!(row.col2.len(), 8);
    assert_eq!(row.col2.get().get(), 12345);
}

/// Test packed struct alignment.
#[test]
fn packed_alignment() {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct MixedRow {
        a: LengthPrefixed<I16BE>, // 6 bytes
        b: LengthPrefixed<I64BE>, // 12 bytes
        c: LengthPrefixed<I32BE>, // 8 bytes
    }

    // Total: 6 + 12 + 8 = 26 bytes
    assert_eq!(std::mem::size_of::<MixedRow>(), 26);
    assert_eq!(std::mem::align_of::<MixedRow>(), 1);
}

/// Test unsigned integers with LengthPrefixed.
#[test]
fn unsigned_integers() {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct UnsignedRow {
        a: LengthPrefixed<U32BE>,
        b: LengthPrefixed<U64BE>,
    }

    let mut data = [0u8; 20];
    // Column 1: U32 = 0xDEADBEEF
    data[0..4].copy_from_slice(&4_i32.to_be_bytes());
    data[4..8].copy_from_slice(&0xDEADBEEF_u32.to_be_bytes());
    // Column 2: U64 = 0xCAFEBABEDEADC0DE
    data[8..12].copy_from_slice(&8_i32.to_be_bytes());
    data[12..20].copy_from_slice(&0xCAFEBABEDEADC0DE_u64.to_be_bytes());

    let row: &UnsignedRow = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(row.a.get().get(), 0xDEADBEEF);
    assert_eq!(row.b.get().get(), 0xCAFEBABEDEADC0DE);
}

/// Test negative values.
#[test]
fn negative_values() {
    let mut data = [0u8; 8];
    data[0..4].copy_from_slice(&4_i32.to_be_bytes());
    data[4..8].copy_from_slice(&(-12345_i32).to_be_bytes());

    let prefixed: &LengthPrefixed<I32BE> = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(prefixed.get().get(), -12345);
}

/// Test that zerocopy correctly rejects wrong-sized data.
#[test]
fn size_validation() {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct TestRow {
        a: LengthPrefixed<I32BE>,
        b: LengthPrefixed<I64BE>,
    }

    // Too small
    let data = [0u8; 19];
    <TestRow as FromBytes>::ref_from_bytes(&data).unwrap_err();

    // Correct size
    let data = [0u8; 20];
    <TestRow as FromBytes>::ref_from_bytes(&data).unwrap();
}

/// Test simulating a real PostgreSQL row with multiple columns.
#[test]
fn simulated_row() {
    #[derive(Debug, FromBytes, KnownLayout, Immutable)]
    #[repr(C, packed)]
    struct UserRow {
        id: LengthPrefixed<I64BE>,
        age: LengthPrefixed<I32BE>,
        score: LengthPrefixed<I64BE>,
    }

    // Simulate: SELECT id, age, score FROM users
    // id=1001, age=25, score=9500
    let mut data = [0u8; 32]; // 12 + 8 + 12 = 32

    // id: INT8 = 1001
    data[0..4].copy_from_slice(&8_i32.to_be_bytes());
    data[4..12].copy_from_slice(&1001_i64.to_be_bytes());

    // age: INT4 = 25
    data[12..16].copy_from_slice(&4_i32.to_be_bytes());
    data[16..20].copy_from_slice(&25_i32.to_be_bytes());

    // score: INT8 = 9500
    data[20..24].copy_from_slice(&8_i32.to_be_bytes());
    data[24..32].copy_from_slice(&9500_i64.to_be_bytes());

    let row: &UserRow = FromBytes::ref_from_bytes(&data).unwrap();
    assert_eq!(row.id.get().get(), 1001);
    assert_eq!(row.age.get().get(), 25);
    assert_eq!(row.score.get().get(), 9500);
}
