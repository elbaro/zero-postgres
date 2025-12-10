//! Action types for state machine I/O requests.

use crate::error::ServerError;

/// Action requested by a state machine.
///
/// The caller should perform the requested I/O and then call the
/// appropriate method to continue the state machine.
#[derive(Debug)]
pub enum Action<'buf> {
    /// State machine needs to read a packet from the server.
    ///
    /// The caller should:
    /// 1. Read the message type byte (1 byte)
    /// 2. Read the length (4 bytes, big-endian i32)
    /// 3. Read (length - 4) bytes of payload into the provided buffer
    /// 4. Call the state machine's `step()` method again
    NeedPacket(&'buf mut Vec<u8>),

    /// State machine needs to write a packet to the server.
    ///
    /// The caller should write all the bytes to the socket and then
    /// call the state machine's appropriate continue method.
    WritePacket(&'buf [u8]),

    /// An asynchronous message was received.
    ///
    /// The caller should handle this message and then call the
    /// state machine's `step()` method again.
    AsyncMessage(AsyncMessage),

    /// The state machine has finished successfully.
    Finished,
}

/// Asynchronous message from the server.
///
/// These can arrive at any time during query execution.
#[derive(Debug, Clone)]
pub enum AsyncMessage {
    /// Notification from LISTEN/NOTIFY.
    Notification {
        /// PID of the notifying backend process
        pid: u32,
        /// Channel name
        channel: String,
        /// Notification payload
        payload: String,
    },

    /// Non-fatal notice/warning from server.
    Notice(ServerError),

    /// Server parameter value changed.
    ParameterChanged {
        /// Parameter name
        name: String,
        /// New value
        value: String,
    },
}
