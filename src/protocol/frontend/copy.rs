//! COPY protocol frontend messages.

use crate::protocol::codec::MessageBuilder;

/// Write a CopyData message.
pub fn write_copy_data(buf: &mut Vec<u8>, data: &[u8]) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::COPY_DATA);
    msg.write_bytes(data);
    msg.finish();
}

/// Write a CopyDone message.
pub fn write_copy_done(buf: &mut Vec<u8>) {
    let msg = MessageBuilder::new(buf, super::msg_type::COPY_DONE);
    msg.finish();
}

/// Write a CopyFail message.
pub fn write_copy_fail(buf: &mut Vec<u8>, error_message: &str) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::COPY_FAIL);
    msg.write_cstr(error_message);
    msg.finish();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_copy_data() {
        let mut buf = Vec::new();
        write_copy_data(&mut buf, b"hello\tworld\n");

        assert_eq!(buf[0], b'd');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len as usize, buf.len() - 1);
    }

    #[test]
    fn test_copy_done() {
        let mut buf = Vec::new();
        write_copy_done(&mut buf);

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'c');
        assert_eq!(&buf[1..5], &4_i32.to_be_bytes());
    }

    #[test]
    fn test_copy_fail() {
        let mut buf = Vec::new();
        write_copy_fail(&mut buf, "error occurred");

        assert_eq!(buf[0], b'f');
    }
}
