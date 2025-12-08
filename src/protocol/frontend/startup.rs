//! Startup and termination messages.

use crate::protocol::codec::MessageBuilder;

/// Protocol version 3.0 (0x00030000)
pub const PROTOCOL_VERSION_3_0: i32 = 196608;

/// Protocol version 3.2 (0x00030002)
pub const PROTOCOL_VERSION_3_2: i32 = 196610;

/// SSL request code
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// GSSAPI encryption request code
pub const GSSENC_REQUEST_CODE: i32 = 80877104;

/// Cancel request code
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

/// Write an SSLRequest message.
///
/// This is sent before StartupMessage to request TLS encryption.
/// Server responds with single byte: 'S' (accepted) or 'N' (rejected).
pub fn write_ssl_request(buf: &mut Vec<u8>) {
    let mut msg = MessageBuilder::new_startup(buf);
    msg.write_i32(SSL_REQUEST_CODE);
    msg.finish();
}

/// Write a GSSENCRequest message.
///
/// This is sent before StartupMessage to request GSSAPI encryption.
/// Server responds with single byte: 'G' (accepted) or 'N' (rejected).
pub fn write_gssenc_request(buf: &mut Vec<u8>) {
    let mut msg = MessageBuilder::new_startup(buf);
    msg.write_i32(GSSENC_REQUEST_CODE);
    msg.finish();
}

/// Write a StartupMessage.
///
/// Parameters is a list of (name, value) pairs.
/// Required: "user" - database username
/// Optional: "database", "options", "replication", "client_encoding", etc.
pub fn write_startup(buf: &mut Vec<u8>, params: &[(&str, &str)]) {
    write_startup_with_version(buf, PROTOCOL_VERSION_3_0, params);
}

/// Write a StartupMessage with a specific protocol version.
pub fn write_startup_with_version(buf: &mut Vec<u8>, version: i32, params: &[(&str, &str)]) {
    let mut msg = MessageBuilder::new_startup(buf);
    msg.write_i32(version);

    for (name, value) in params {
        msg.write_cstr(name);
        msg.write_cstr(value);
    }

    // Terminator
    msg.write_u8(0);
    msg.finish();
}

/// Write a CancelRequest message.
///
/// This is sent on a NEW connection to cancel a query running on another connection.
/// The server closes the connection immediately with no response.
pub fn write_cancel_request(buf: &mut Vec<u8>, pid: u32, secret_key: u32) {
    let mut msg = MessageBuilder::new_startup(buf);
    msg.write_i32(CANCEL_REQUEST_CODE);
    msg.write_i32(pid as i32);
    msg.write_i32(secret_key as i32);
    msg.finish();
}

/// Write a Terminate message.
///
/// Sent to cleanly close the connection.
pub fn write_terminate(buf: &mut Vec<u8>) {
    let msg = MessageBuilder::new(buf, super::msg_type::TERMINATE);
    msg.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_request() {
        let mut buf = Vec::new();
        write_ssl_request(&mut buf);

        assert_eq!(buf.len(), 8);
        assert_eq!(&buf[0..4], &8_i32.to_be_bytes());
        assert_eq!(&buf[4..8], &SSL_REQUEST_CODE.to_be_bytes());
    }

    #[test]
    fn test_startup() {
        let mut buf = Vec::new();
        write_startup(&mut buf, &[("user", "postgres"), ("database", "test")]);

        // Check length is at start
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len as usize, buf.len());

        // Check protocol version
        let version = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(version, PROTOCOL_VERSION_3_0);
    }

    #[test]
    fn test_terminate() {
        let mut buf = Vec::new();
        write_terminate(&mut buf);

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'X');
        assert_eq!(&buf[1..5], &4_i32.to_be_bytes());
    }
}
