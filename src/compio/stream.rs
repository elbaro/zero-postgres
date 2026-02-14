//! Async stream abstraction for compio.

use compio::buf::{BufResult, IntoInner, IoBuf, IoBufMut};
use compio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
#[cfg(unix)]
use compio::net::UnixStream;

#[cfg(feature = "compio-tls")]
use compio_tls::TlsStream;

pub enum Stream {
    Tcp(TcpStream),
    #[cfg(feature = "compio-tls")]
    Tls(TlsStream<TcpStream>),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self::Tcp(stream)
    }

    #[cfg(unix)]
    pub fn unix(stream: UnixStream) -> Self {
        Self::Unix(stream)
    }

    /// Upgrade a TCP stream to TLS.
    ///
    /// Returns an error if this is not a TCP stream or if the TLS handshake fails.
    #[cfg(feature = "compio-tls")]
    pub async fn upgrade_to_tls(self, host: &str) -> Result<Self, crate::error::Error> {
        match self {
            Stream::Tcp(tcp_stream) => {
                let native_connector =
                    native_tls::TlsConnector::new().map_err(crate::error::Error::Tls)?;
                let connector = compio_tls::TlsConnector::from(native_connector);
                let tls_stream = connector.connect(host, tcp_stream).await?;
                Ok(Stream::Tls(tls_stream))
            }
            Stream::Tls(_) => Err(crate::error::Error::InvalidUsage(
                "Stream is already TLS".into(),
            )),
            #[cfg(unix)]
            Stream::Unix(_) => Err(crate::error::Error::InvalidUsage(
                "Cannot upgrade Unix socket to TLS".into(),
            )),
        }
    }

    pub async fn read_u8(&mut self) -> std::io::Result<u8> {
        match self {
            Stream::Tcp(r) => r.read_u8().await,
            #[cfg(feature = "compio-tls")]
            Stream::Tls(r) => r.read_u8().await,
            #[cfg(unix)]
            Stream::Unix(r) => r.read_u8().await,
        }
    }

    /// Read a PostgreSQL message into the buffer set.
    ///
    /// Compio's `read_exact` fills from `buf.len()` to `buf.capacity()`,
    /// so buffers must have len=0 with the desired capacity, and we use
    /// `.slice(0..n)` to read exactly n bytes when capacity may exceed n.
    pub async fn read_message(
        &mut self,
        buffer_set: &mut crate::buffer_set::BufferSet,
    ) -> std::io::Result<()> {
        buffer_set.type_byte = self.read_u8().await?;

        // len=0, cap=4: compio reads exactly 4 bytes
        let length_buf = Vec::with_capacity(4);
        let BufResult(length_res, length_buf) = self.read_exact(length_buf).await;
        length_res?;

        let length =
            u32::from_be_bytes([length_buf[0], length_buf[1], length_buf[2], length_buf[3]])
                as usize;

        let payload_len = length.saturating_sub(4);
        if payload_len == 0 {
            buffer_set.read_buffer.clear();
            return Ok(());
        }

        // Reuse the pooled buffer. After clear(), len=0 but capacity may
        // exceed payload_len, so slice to read exactly payload_len bytes.
        buffer_set.read_buffer.clear();
        buffer_set.read_buffer.reserve(payload_len);
        let read_buf = std::mem::take(&mut buffer_set.read_buffer);
        let BufResult(payload_res, slice) = self.read_exact(read_buf.slice(0..payload_len)).await;
        buffer_set.read_buffer = slice.into_inner();
        payload_res?;

        Ok(())
    }

    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> BufResult<(), B> {
        match self {
            Stream::Tcp(r) => r.read_exact(buf).await,
            #[cfg(feature = "compio-tls")]
            Stream::Tls(r) => r.read_exact(buf).await,
            #[cfg(unix)]
            Stream::Unix(r) => r.read_exact(buf).await,
        }
    }

    pub async fn write_all_owned(&mut self, buf: Vec<u8>) -> BufResult<(), Vec<u8>> {
        match self {
            Stream::Tcp(r) => r.write_all(buf).await,
            #[cfg(feature = "compio-tls")]
            Stream::Tls(r) => r.write_all(buf).await,
            #[cfg(unix)]
            Stream::Unix(r) => r.write_all(buf).await,
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.flush().await,
            #[cfg(feature = "compio-tls")]
            Stream::Tls(r) => r.flush().await,
            #[cfg(unix)]
            Stream::Unix(r) => r.flush().await,
        }
    }

    pub fn is_tcp_loopback(&self) -> bool {
        match self {
            Self::Tcp(r) => r
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "compio-tls")]
            Self::Tls(_) => false,
            #[cfg(unix)]
            Self::Unix(_) => false,
        }
    }
}
