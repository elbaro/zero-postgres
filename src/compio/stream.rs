//! Async stream abstraction for compio with read buffering.

use compio::buf::{BufResult, IntoInner, IoBuf, IoBufMut};
use compio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
#[cfg(unix)]
use compio::net::UnixStream;

#[cfg(feature = "compio-tls")]
use compio::tls::TlsStream;

const READ_BUF_CAPACITY: usize = 8192;

enum StreamInner {
    Tcp(TcpStream),
    #[cfg(feature = "compio-tls")]
    Tls(TlsStream<TcpStream>),
    #[cfg(unix)]
    Unix(UnixStream),
}

/// Buffered async stream for compio.
///
/// Wraps the raw socket with a userspace read buffer to amortize io_uring
/// submissions. Without buffering, every `read_u8` / `read_exact(4)` /
/// `read_exact(payload)` would each be a separate io_uring submission
/// (3 per PostgreSQL message). With buffering, a single read fills the
/// buffer and subsequent message parses are served from memory.
pub struct Stream {
    inner: StreamInner,
    read_buf: Vec<u8>,
    read_pos: usize,
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self {
            inner: StreamInner::Tcp(stream),
            read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
            read_pos: 0,
        }
    }

    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> Self {
        Self {
            inner: StreamInner::Unix(stream),
            read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
            read_pos: 0,
        }
    }

    /// Upgrade a TCP stream to TLS.
    ///
    /// Returns an error if this is not a TCP stream or if the TLS handshake fails.
    #[cfg(feature = "compio-tls")]
    pub async fn upgrade_to_tls(self, host: &str) -> Result<Self, crate::error::Error> {
        match self.inner {
            StreamInner::Tcp(tcp_stream) => {
                let native_connector =
                    compio::native_tls::TlsConnector::new().map_err(crate::error::Error::Tls)?;
                let connector = compio::tls::TlsConnector::from(native_connector);
                let tls_stream = connector.connect(host, tcp_stream).await?;
                // Start with a fresh read buffer after TLS handshake;
                // any pre-TLS buffered bytes are invalid in the TLS context.
                Ok(Self {
                    inner: StreamInner::Tls(tls_stream),
                    read_buf: Vec::with_capacity(READ_BUF_CAPACITY),
                    read_pos: 0,
                })
            }
            StreamInner::Tls(_) => Err(crate::error::Error::InvalidUsage(
                "Stream is already TLS".into(),
            )),
            #[cfg(unix)]
            StreamInner::Unix(_) => Err(crate::error::Error::InvalidUsage(
                "Cannot upgrade Unix socket to TLS".into(),
            )),
        }
    }

    // --- Buffered read internals ---

    /// Bytes available in the read buffer.
    fn available(&self) -> usize {
        self.read_buf.len() - self.read_pos
    }

    /// Compact the buffer and read more data from the socket.
    async fn fill_buf(&mut self) -> std::io::Result<()> {
        // Compact: move unconsumed data to front
        if self.read_pos > 0 {
            let valid = self.available();
            self.read_buf
                .copy_within(self.read_pos..self.read_pos + valid, 0);
            self.read_buf.truncate(valid);
            self.read_pos = 0;
        }

        // Read from socket. compio reads from buf.len() to buf.capacity(),
        // so with len=valid and cap=8192 it fills the remaining space.
        let buf = std::mem::take(&mut self.read_buf);
        let BufResult(result, buf) = self.read_raw(buf).await;
        self.read_buf = buf;
        let n = result?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        Ok(())
    }

    /// Ensure at least `n` bytes are available in the read buffer.
    async fn ensure(&mut self, n: usize) -> std::io::Result<()> {
        while self.available() < n {
            self.fill_buf().await?;
        }
        Ok(())
    }

    // --- Public read API ---

    pub async fn read_u8(&mut self) -> std::io::Result<u8> {
        self.ensure(1).await?;
        let byte = self.read_buf[self.read_pos];
        self.read_pos += 1;
        Ok(byte)
    }

    /// Read a PostgreSQL message into the buffer set.
    ///
    /// Reads the 5-byte header (type byte + 4-byte length) from the read
    /// buffer, then copies the payload. For small messages the payload is
    /// served entirely from the buffer with no additional syscalls.
    pub async fn read_message(
        &mut self,
        buffer_set: &mut crate::buffer_set::BufferSet,
    ) -> std::io::Result<()> {
        // Read 5-byte header from buffer
        self.ensure(5).await?;
        buffer_set.type_byte = self.read_buf[self.read_pos];
        let (len_bytes, _) = self.read_buf[self.read_pos + 1..]
            .split_first_chunk::<4>()
            .ok_or_else(|| std::io::Error::other("protocol: header shorter than 5 bytes"))?;
        let length = u32::from_be_bytes(*len_bytes) as usize;
        self.read_pos += 5;

        let payload_len = length.saturating_sub(4);
        if payload_len == 0 {
            buffer_set.read_buffer.clear();
            return Ok(());
        }

        buffer_set.read_buffer.clear();
        buffer_set.read_buffer.reserve(payload_len);

        // Copy what we already have from the read buffer
        let from_buf = self.available().min(payload_len);
        buffer_set
            .read_buffer
            .extend_from_slice(&self.read_buf[self.read_pos..self.read_pos + from_buf]);
        self.read_pos += from_buf;

        let remaining = payload_len - from_buf;
        if remaining > 0 {
            // Large payload: read the rest directly from the socket
            let buf = std::mem::take(&mut buffer_set.read_buffer);
            let BufResult(res, slice) = self.read_exact_raw(buf.slice(from_buf..payload_len)).await;
            buffer_set.read_buffer = slice.into_inner();
            res?;
        }

        Ok(())
    }

    // --- Raw (unbuffered) I/O ---

    /// Read whatever is available from the socket (non-blocking style).
    async fn read_raw(&mut self, buf: Vec<u8>) -> BufResult<usize, Vec<u8>> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.read(buf).await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.read(buf).await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.read(buf).await,
        }
    }

    /// Read exactly the requested bytes from the socket.
    async fn read_exact_raw<B: IoBufMut>(&mut self, buf: B) -> BufResult<(), B> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.read_exact(buf).await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.read_exact(buf).await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.read_exact(buf).await,
        }
    }

    // --- Write API (pass-through, no buffering needed) ---

    pub async fn write_all_owned(&mut self, buf: Vec<u8>) -> BufResult<(), Vec<u8>> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.write_all(buf).await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.write_all(buf).await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.write_all(buf).await,
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.inner {
            StreamInner::Tcp(r) => r.flush().await,
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(r) => r.flush().await,
            #[cfg(unix)]
            StreamInner::Unix(r) => r.flush().await,
        }
    }

    // --- Misc ---

    pub fn is_tcp_loopback(&self) -> bool {
        match &self.inner {
            StreamInner::Tcp(r) => r
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "compio-tls")]
            StreamInner::Tls(_) => false,
            #[cfg(unix)]
            StreamInner::Unix(_) => false,
        }
    }
}
