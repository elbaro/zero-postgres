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

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
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
