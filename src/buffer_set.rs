//! Buffer set for state machine operations.

/// Buffer set for state machine operations.
pub struct BufferSet {
    /// Read buffer for incoming messages
    pub read_buffer: Vec<u8>,
    /// Type byte of the last message read
    pub type_byte: u8,
}

impl BufferSet {
    /// Create a new buffer set.
    pub fn new() -> Self {
        Self {
            read_buffer: Vec::with_capacity(8192),
            type_byte: 0,
        }
    }
}

impl Default for BufferSet {
    fn default() -> Self {
        Self::new()
    }
}
