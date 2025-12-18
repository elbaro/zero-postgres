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

    /// Upgrade a TCP stream to TLS.
    ///
    /// Returns an error if this is not a TCP stream or if the TLS handshake fails.
    #[cfg(feature = "sync-tls")]
    pub fn upgrade_to_tls(self, host: &str) -> Result<Self, crate::error::Error> {
        match self {
            Stream::Tcp(buf_reader) => {
                let tcp_stream = buf_reader.into_inner();
                let connector = native_tls::TlsConnector::new()?;
                let tls_stream = connector.connect(host, tcp_stream).map_err(|e| match e {
                    native_tls::HandshakeError::Failure(e) => crate::error::Error::Tls(e),
                    native_tls::HandshakeError::WouldBlock(_) => {
                        crate::error::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "TLS handshake would block",
                        ))
                    }
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

    pub fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut buf = [0u8; 1];
        let n = match self {
            Stream::Tcp(r) => r.read(&mut buf),
            #[cfg(feature = "sync-tls")]
            Stream::Tls(r) => r.read(&mut buf),
            Stream::Unix(r) => r.read(&mut buf),
        }?;
        if n == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        Ok(buf[0])
    }

    /// Read a PostgreSQL message into the buffer set.
    pub fn read_message(
        &mut self,
        buffer_set: &mut crate::buffer_set::BufferSet,
    ) -> std::io::Result<()> {
        buffer_set.type_byte = self.read_u8()?;

        let mut length_bytes = [0u8; 4];
        self.read_exact(&mut length_bytes)?;
        let length = u32::from_be_bytes(length_bytes) as usize;

        let payload_len = length.saturating_sub(4);
        buffer_set.read_buffer.clear();
        buffer_set.read_buffer.resize(payload_len, 0);
        self.read_exact(&mut buffer_set.read_buffer)?;

        Ok(())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
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

    /// TlsStream writing is buffered
    pub fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Tcp(r) => r.get_mut().flush(),
            #[cfg(feature = "sync-tls")]
            Stream::Tls(r) => r.get_mut().flush(),
            Stream::Unix(r) => r.get_mut().flush(),
        }
    }

    pub fn is_tcp_loopback(&self) -> bool {
        match self {
            Self::Tcp(r) => r
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            #[cfg(feature = "sync-tls")]
            Self::Tls(r) => r
                .get_ref()
                .get_ref()
                .peer_addr()
                .map(|addr| addr.ip().is_loopback())
                .unwrap_or(false),
            Self::Unix(_) => false,
        }
    }
}
