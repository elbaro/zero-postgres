//! Extended query protocol messages.

use crate::conversion::ToParams;
use crate::protocol::codec::MessageBuilder;
use crate::protocol::types::{FormatCode, Oid};

/// Write a Parse message to create a prepared statement.
///
/// - `name`: Statement name (empty string for unnamed statement)
/// - `query`: SQL query with $1, $2, ... placeholders
/// - `param_oids`: Parameter type OIDs (0 = let server infer)
pub fn write_parse(buf: &mut Vec<u8>, name: &str, query: &str, param_oids: &[Oid]) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::PARSE);
    msg.write_cstr(name);
    msg.write_cstr(query);
    msg.write_i16(param_oids.len() as i16);
    for &oid in param_oids {
        msg.write_i32(oid as i32);
    }
    msg.finish();
}

/// Write a Bind message to create a portal from a prepared statement.
///
/// - `portal`: Portal name (empty string for unnamed portal)
/// - `statement`: Statement name
/// - `params`: Parameter values (tuple of ToValue types)
/// - `result_formats`: Format codes for results
pub fn write_bind<P: ToParams>(
    buf: &mut Vec<u8>,
    portal: &str,
    statement: &str,
    params: &P,
    result_formats: &[FormatCode],
) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::BIND);

    // Portal and statement names
    msg.write_cstr(portal);
    msg.write_cstr(statement);

    // Parameter format codes - all binary (1)
    let param_count = params.param_count();
    msg.write_i16(param_count as i16);
    for _ in 0..param_count {
        msg.write_i16(FormatCode::Binary as i16);
    }

    // Parameter values (count + length-prefixed data)
    msg.write_i16(param_count as i16);
    params.to_binary(msg.buf());

    // Result format codes
    msg.write_i16(result_formats.len() as i16);
    for &fmt in result_formats {
        msg.write_i16(fmt as i16);
    }

    msg.finish();
}

/// Write an Execute message to run a portal.
///
/// - `portal`: Portal name
/// - `max_rows`: Maximum number of rows to return (0 = unlimited)
pub fn write_execute(buf: &mut Vec<u8>, portal: &str, max_rows: u32) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::EXECUTE);
    msg.write_cstr(portal);
    msg.write_i32(max_rows as i32);
    msg.finish();
}

/// Write a Describe message to get metadata.
///
/// - `describe_type`: 'S' for statement, 'P' for portal
/// - `name`: Statement or portal name
pub fn write_describe(buf: &mut Vec<u8>, describe_type: u8, name: &str) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::DESCRIBE);
    msg.write_u8(describe_type);
    msg.write_cstr(name);
    msg.finish();
}

/// Write a Describe message for a statement.
pub fn write_describe_statement(buf: &mut Vec<u8>, name: &str) {
    write_describe(buf, b'S', name);
}

/// Write a Describe message for a portal.
pub fn write_describe_portal(buf: &mut Vec<u8>, name: &str) {
    write_describe(buf, b'P', name);
}

/// Write a Close message to release a statement or portal.
///
/// - `close_type`: 'S' for statement, 'P' for portal
/// - `name`: Statement or portal name
pub fn write_close(buf: &mut Vec<u8>, close_type: u8, name: &str) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::CLOSE);
    msg.write_u8(close_type);
    msg.write_cstr(name);
    msg.finish();
}

/// Write a Close message for a statement.
pub fn write_close_statement(buf: &mut Vec<u8>, name: &str) {
    write_close(buf, b'S', name);
}

/// Write a Close message for a portal.
pub fn write_close_portal(buf: &mut Vec<u8>, name: &str) {
    write_close(buf, b'P', name);
}

/// Write a Sync message.
///
/// This ends an extended query sequence and causes:
/// - Implicit COMMIT if successful and not in explicit transaction
/// - Implicit ROLLBACK if failed and not in explicit transaction
/// - Server responds with ReadyForQuery
pub fn write_sync(buf: &mut Vec<u8>) {
    let msg = MessageBuilder::new(buf, super::msg_type::SYNC);
    msg.finish();
}

/// Write a Flush message.
///
/// Forces the server to send all pending responses without waiting for Sync.
/// Useful for pipelining when you need intermediate results.
pub fn write_flush(buf: &mut Vec<u8>) {
    let msg = MessageBuilder::new(buf, super::msg_type::FLUSH);
    msg.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let mut buf = Vec::new();
        write_parse(&mut buf, "stmt1", "SELECT $1::int", &[0]);

        assert_eq!(buf[0], b'P');

        // Verify length field
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len as usize, buf.len() - 1);
    }

    #[test]
    fn test_sync() {
        let mut buf = Vec::new();
        write_sync(&mut buf);

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'S');
        assert_eq!(&buf[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_flush() {
        let mut buf = Vec::new();
        write_flush(&mut buf);

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'H');
        assert_eq!(&buf[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_execute() {
        let mut buf = Vec::new();
        write_execute(&mut buf, "", 0);

        assert_eq!(buf[0], b'E');
        // Length: 4 + 1 (empty string + null) + 4 (max_rows) = 9
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 9);
    }
}
