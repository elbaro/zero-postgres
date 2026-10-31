//! Connection options.

use std::sync::Arc;

use url::Url;

use crate::buffer_pool::{BufferPool, GLOBAL_BUFFER_POOL};
use crate::error::Error;

/// TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Don't use TLS (Insecure)
    Disable,
    /// Try TLS, fall back to unencrypted if the server does not support TLS (Insecure)
    #[default]
    Prefer,
    /// Require TLS (Recommended)
    Require,
}

/// Connection options for PostgreSQL.
#[derive(Debug, Clone)]
pub struct Opts {
    /// Hostname or IP address.
    ///
    /// Default: `""`
    pub host: String,

    /// Port number for the PostgreSQL server.
    ///
    /// Default: `5432`
    pub port: u16,

    /// Unix socket path.
    ///
    /// Default: `None`
    pub socket: Option<String>,

    /// Username for authentication.
    ///
    /// Default: `""`
    pub user: String,

    /// Database name to use.
    ///
    /// Default: `None`
    pub database: Option<String>,

    /// Password for authentication.
    ///
    /// Default: `None`
    pub password: Option<String>,

    /// Application name to report to the server.
    ///
    /// Default: `None`
    pub application_name: Option<String>,

    /// TLS connection mode.
    ///
    /// Default: `TlsMode::Prefer`
    pub tls_mode: TlsMode,

    /// rustls client config for TLS connections.
    ///
    /// Set this to trust a private CA or to present a client certificate.
    /// Build the config with [`crate::rustls`] so that the rustls versions match,
    /// and with the aws-lc-rs provider, which this crate uses:
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    /// use zero_postgres::rustls::pki_types::{pem::PemObject, CertificateDer};
    /// use zero_postgres::rustls::{crypto::aws_lc_rs, ClientConfig, RootCertStore};
    ///
    /// let mut roots = RootCertStore::empty();
    /// for cert in CertificateDer::pem_file_iter("/etc/myapp/db-ca.pem")? {
    ///     roots.add(cert?)?;
    /// }
    /// let config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
    ///     .with_safe_default_protocol_versions()?
    ///     .with_root_certificates(roots)
    ///     .with_no_client_auth();
    ///
    /// let mut opts = zero_postgres::Opts::default();
    /// opts.tls_config = Some(Arc::new(config));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `None` uses the platform certificate verifier, built once and shared by
    /// all connections.
    ///
    /// Default: `None`
    #[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
    pub tls_config: Option<Arc<rustls::ClientConfig>>,

    /// Additional connection parameters.
    ///
    /// Default: `[]`
    pub params: Vec<(String, String)>,

    /// When connected via TCP to loopback, upgrade to Unix socket for better performance.
    ///
    /// Default: `true`
    pub upgrade_to_unix_socket: bool,

    /// Maximum number of idle connections in the pool.
    ///
    /// Default: `100`
    pub pool_max_idle_conn: usize,

    /// Maximum number of concurrent connections (None = unlimited).
    ///
    /// Default: `None`
    pub pool_max_concurrency: Option<usize>,

    /// Buffer pool for reusing buffers across connections.
    ///
    /// Default: `GLOBAL_BUFFER_POOL`
    pub buffer_pool: Arc<BufferPool>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5432,
            socket: None,
            user: String::new(),
            database: None,
            password: None,
            application_name: None,
            tls_mode: TlsMode::Prefer,
            #[cfg(any(feature = "sync-tls", feature = "tokio-tls", feature = "compio-tls"))]
            tls_config: None,
            params: Vec::new(),
            upgrade_to_unix_socket: true,
            pool_max_idle_conn: 100,
            pool_max_concurrency: None,
            buffer_pool: Arc::clone(&GLOBAL_BUFFER_POOL),
        }
    }
}

impl TryFrom<&Url> for Opts {
    type Error = Error;

    /// Parse a PostgreSQL connection URL.
    ///
    /// Format: `(pg|postgres|postgresql)://[user[:password]@]host[:port][/database][?param1=value1&param2=value2&..]`
    ///
    /// The user, password and database are percent-decoded, so characters such as
    /// `/`, `#` and `?` can be written as `%2F`, `%23` and `%3F`. A literal `%`
    /// followed by two hex digits must be written as `%25`.
    ///
    /// Supported query parameters:
    /// - `tls_mode`: disable, prefer, require
    /// - `application_name`: application name
    /// - `upgrade_to_unix_socket`: true/True/1/yes/on or false/False/0/no/off
    /// - `pool_max_idle_conn`: maximum idle connections (positive integer)
    /// - `pool_max_concurrency`: maximum concurrent connections (positive integer)
    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if !["pg", "postgres", "postgresql"].contains(&url.scheme()) {
            return Err(Error::InvalidUsage(format!(
                "Invalid scheme: expected 'pg://', 'postgres://', or 'postgresql://', got '{}://'",
                url.scheme()
            )));
        }

        let mut opts = Opts {
            host: url.host_str().unwrap_or("localhost").to_string(),
            port: url.port().unwrap_or(5432),
            user: percent_decode(url.username())?,
            password: url.password().map(percent_decode).transpose()?,
            database: match url.path().strip_prefix('/') {
                Some(s) if !s.is_empty() => Some(percent_decode(s)?),
                _ => None,
            },
            ..Opts::default()
        };

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "tls_mode" => {
                    opts.tls_mode = match value.as_ref() {
                        "disable" => TlsMode::Disable,
                        "prefer" => TlsMode::Prefer,
                        "require" => TlsMode::Require,
                        _ => {
                            return Err(Error::InvalidUsage(format!(
                                "Invalid tls_mode: expected one of ['disable', 'prefer', 'require'], got {}",
                                value
                            )));
                        }
                    };
                }
                "application_name" => {
                    opts.application_name = Some(value.to_string());
                }
                "upgrade_to_unix_socket" => {
                    opts.upgrade_to_unix_socket = match value.as_ref() {
                        "true" | "True" | "1" | "yes" | "on" => true,
                        "false" | "False" | "0" | "no" | "off" => false,
                        _ => {
                            return Err(Error::InvalidUsage(format!(
                                "Invalid upgrade_to_unix_socket parameter: {}",
                                value
                            )));
                        }
                    };
                }
                "pool_max_idle_conn" => {
                    opts.pool_max_idle_conn = value.parse().map_err(|_unhelpful_err| {
                        Error::InvalidUsage(format!("Invalid pool_max_idle_conn: {}", value))
                    })?;
                }
                "pool_max_concurrency" => {
                    opts.pool_max_concurrency = Some(value.parse().map_err(|_unhelpful_err| {
                        Error::InvalidUsage(format!("Invalid pool_max_concurrency: {}", value))
                    })?);
                }
                _ => {
                    opts.params.push((key.to_string(), value.to_string()));
                }
            }
        }

        Ok(opts)
    }
}

impl TryFrom<&str> for Opts {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s).map_err(|e| Error::InvalidUsage(format!("Invalid URL: {}", e)))?;
        Self::try_from(&url)
    }
}

fn percent_decode(s: &str) -> Result<String, Error> {
    percent_encoding::percent_decode_str(s)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|e| Error::InvalidUsage(format!("Invalid UTF-8 in URL: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_encoded_credentials_and_database() {
        let opts = Opts::try_from("postgres://al%40ice:p%40ss%2Fw%3Frd@localhost/my%20db").unwrap();
        assert_eq!(opts.user, "al@ice");
        assert_eq!(opts.password.as_deref(), Some("p@ss/w?rd"));
        assert_eq!(opts.database.as_deref(), Some("my db"));
    }

    #[test]
    fn decodes_raw_at_sign_in_password() {
        let opts = Opts::try_from("postgres://alice:p@ss@localhost/db").unwrap();
        assert_eq!(opts.password.as_deref(), Some("p@ss"));
        assert_eq!(opts.host, "localhost");
    }

    #[test]
    fn keeps_plain_values() {
        let opts = Opts::try_from("postgres://alice:secret@localhost/db").unwrap();
        assert_eq!(opts.user, "alice");
        assert_eq!(opts.password.as_deref(), Some("secret"));
        assert_eq!(opts.database.as_deref(), Some("db"));
    }

    #[test]
    fn rejects_invalid_utf8() {
        Opts::try_from("postgres://alice:%FF@localhost/db").unwrap_err();
    }
}
