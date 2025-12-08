//! Simple query protocol messages.

use crate::protocol::codec::MessageBuilder;

/// Write a Query message.
///
/// The query string may contain multiple SQL statements separated by semicolons.
pub fn write_query(buf: &mut Vec<u8>, query: &str) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::QUERY);
    msg.write_cstr(query);
    msg.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query() {
        let mut buf = Vec::new();
        write_query(&mut buf, "SELECT 1");

        assert_eq!(buf[0], b'Q');

        // Length should be 4 (length field) + 9 (query + null terminator)
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 13);

        // Query string should be "SELECT 1\0"
        assert_eq!(&buf[5..14], b"SELECT 1\0");
    }
}
