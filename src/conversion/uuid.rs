//! UUID type implementation (uuid crate).

use crate::error::{Error, Result};
use crate::protocol::types::{Oid, oid};

use super::{FromWireValue, ToWireValue};

impl FromWireValue<'_> for uuid::Uuid {
    fn from_text(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::UUID {
            return Err(Error::Decode(format!("cannot decode oid {} as UUID", oid)));
        }
        let s = simdutf8::compat::from_utf8(bytes)
            .map_err(|e| Error::Decode(format!("invalid UTF-8: {}", e)))?;
        uuid::Uuid::parse_str(s).map_err(|e| Error::Decode(format!("invalid UUID: {}", e)))
    }

    fn from_binary(oid: Oid, bytes: &[u8]) -> Result<Self> {
        if oid != oid::UUID {
            return Err(Error::Decode(format!("cannot decode oid {} as UUID", oid)));
        }
        uuid::Uuid::from_slice(bytes).map_err(|e| Error::Decode(format!("invalid UUID: {}", e)))
    }
}

impl ToWireValue for uuid::Uuid {
    fn natural_oid(&self) -> Oid {
        oid::UUID
    }

    fn encode(&self, target_oid: Oid, buf: &mut Vec<u8>) -> Result<()> {
        match target_oid {
            oid::UUID => {
                buf.extend_from_slice(&16_i32.to_be_bytes());
                buf.extend_from_slice(self.as_bytes());
                Ok(())
            }
            _ => Err(Error::type_mismatch(self.natural_oid(), target_oid)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_text() {
        let uuid_str = b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        let uuid = uuid::Uuid::from_text(oid::UUID, uuid_str).unwrap();
        assert_eq!(uuid.to_string(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    }

    #[test]
    fn test_uuid_binary() {
        let bytes: [u8; 16] = [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11,
        ];
        let uuid = uuid::Uuid::from_binary(oid::UUID, &bytes).unwrap();
        assert_eq!(uuid.to_string(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    }

    #[test]
    fn test_uuid_roundtrip() {
        let original = uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
        let mut buf = Vec::new();
        original.encode(original.natural_oid(), &mut buf).unwrap();
        // Skip the 4-byte length prefix
        let decoded = uuid::Uuid::from_binary(oid::UUID, &buf[4..]).unwrap();
        assert_eq!(original, decoded);
    }
}
