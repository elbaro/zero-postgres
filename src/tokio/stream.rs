//! Async stream abstraction for tokio.

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::UnixStream;

#[cfg(feature = "tokio-tls")]
use tokio_native_tls::TlsStream;

pub enum Stream {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "tokio-tls")]
    Tls(BufReader<TlsStream<TcpStream>>),
    Unix(BufReader<UnixStream>),
}

impl Stream {
    pub fn tcp(stream: TcpStream) -> Self {
        Self::Tcp(BufReader::new(stream))
    }

    pub fn unix(stream: UnixStream) -> Self {
        Self::Unix(BufReader::new(stream))
    }

    /// Upgrade a TCP stream to TLS.
    ///
    /// Returns an error if this is not a TCP stream or if the TLS handshake fails.
    #[cfg(feature = "tokio-tls")]
    pub async fn upgrade_to_tls(self, host: &str) -> Result<Self, crate::error::Error> {
        match self {
            Stream::Tcp(buf_reader) => {
                let tcp_stream = buf_reader.into_inner();
                let connector = tokio_native_tls::TlsConnector::from(native_tls::TlsConnector::new()?);
                let tls_stream = connector.connect(host, tcp_stream).await.map_err(|e| {
                    crate::error::Error::Tls(e.into())
                })?;
                Ok(Stream::Tls(BufReader::new(tls_stream)))
            }
            Stream::Tls(_) => Err(crate::error::Error::InvalidUsage(
                "Stream is already TLS".into(),
            )),
            Stream::Unix(_) => Err(crate::error::Error::InvalidUsage(
                "Cannot upgrade Unix socket to TLS".into(),
            )),
        }
    }

    pub async fn read_u8(&mut self) -> std::io::Result<u8> {
        match self {
            Stream::Tcp(r) => r.read_u8().await,
            #[cfg(feature = "tokio-tls")]
            Stream::Tls(r) => r.read_u8().await,
            Stream::Unix(r) => r.read_u8().await,
        }
    }

    /// Read a PostgreSQL message into the buffer set.
    pub async fn read_message(&mut self, buffer_set: &mut crate::buffer_set::BufferSet) -> std::io::Result<()> {
        buffer_set.type_byte = self.read_u8().await?;

        let mut length_bytes = [0u8; 4];
        self.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;

        let payload_len = length.saturating_sub(4);
        buffer_set.read_buffer.clear();
        buffer_set.read_buffer.resize(payload_len, 0);
        self.read_exact(&mut buffer_set.read_buffer).await?;

        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.read_exact(buf).await.map(|_| ()),
            #[cfg(feature = "tokio-tls")]
            Stream::Tls(r) => r.read_exact(buf).await.map(|_| ()),
            Stream::Unix(r) => r.read_exact(buf).await.map(|_| ()),
        }
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.get_mut().write_all(buf).await,
            #[cfg(feature = "tokio-tls")]
            Stream::Tls(r) => r.get_mut().write_all(buf).await,
            Stream::Unix(r) => r.get_mut().write_all(buf).await,
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.get_mut().flush().await,
            #[cfg(feature = "tokio-tls")]
            Stream::Tls(r) => r.get_mut().flush().await,
            Stream::Unix(r) => r.get_mut().flush().await,
        }
    }

    pub fn is_tcp_loopback(&self) -> bool {
        match self {
            Self::Tcp(r) => r
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(r) => r
                .get_ref() // &TlsStream<TcpStream> (tokio_native_tls)
                .get_ref() // &native_tls::TlsStream<AllowStd<TcpStream>>
                .get_ref() // &AllowStd<TcpStream>
                .get_ref() // &TcpStream
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            Self::Unix(_) => false,
        }
    }
}
