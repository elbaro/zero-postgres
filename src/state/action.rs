//! Action types for state machine I/O requests.

use crate::error::ServerError;

/// Action requested by a state machine.
///
/// The caller should perform the requested I/O and then call the
/// appropriate method to continue the state machine.
#[derive(Debug)]
pub enum Action {
    /// Write `buffer_set.write_buffer` to the server, then read a single byte.
    ///
    /// Used for SSL negotiation: write SSL request, then read response ('S' or 'N').
    WriteAndReadByte,

    /// Read a PostgreSQL message from the server.
    ///
    /// The caller should:
    /// 1. Read the message type byte (1 byte)
    /// 2. Read the length (4 bytes, big-endian i32)
    /// 3. Read (length - 4) bytes of payload into the buffer set
    /// 4. Call the state machine's `step()` method again
    ReadMessage,

    /// Write `buffer_set.write_buffer` to the server.
    ///
    /// The caller should write all bytes to the socket and flush,
    /// then call `step()` again.
    Write,

    /// Write `buffer_set.write_buffer` to the server, then read a message.
    ///
    /// Used for query operations: write query, then read response.
    WriteAndReadMessage,

    /// Perform TLS handshake.
    ///
    /// After successful handshake, call `step()` again.
    TlsHandshake,

    /// An asynchronous message was received.
    ///
    /// The caller should handle the message, read the next message,
    /// then call `step()` again.
    HandleAsyncMessageAndReadMessage(AsyncMessage),

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
