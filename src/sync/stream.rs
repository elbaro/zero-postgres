use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;

#[cfg(feature = "sync-tls")]
use native_tls::TlsStream;

pub enum Stream {
    Tcp(BufReader<TcpStream>),
    #[cfg(feature = "sync-tls")]
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

    pub fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.read_exact(buf),
            #[cfg(feature = "sync-tls")]
            Stream::Tls(r) => r.read_exact(buf),
            Stream::Unix(r) => r.read_exact(buf),
        }
    }

    pub fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.get_mut().write_all(buf),
            #[cfg(feature = "sync-tls")]
            Stream::Tls(r) => r.get_mut().write_all(buf),
            Stream::Unix(r) => r.get_mut().write_all(buf),
        }
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.get_mut().flush(),
            #[cfg(feature = "sync-tls")]
            Stream::Tls(r) => r.get_mut().flush(),
            Stream::Unix(r) => r.get_mut().flush(),
        }
    }
}
