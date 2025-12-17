//! Connection options.

use no_panic::no_panic;
use url::Url;

use crate::error::Error;

/// SSL connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Don't use SSL
    Disable,
    /// Try SSL, fall back to unencrypted if not supported
    #[default]
    Prefer,
    /// Require SSL connection
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

    /// SSL connection mode.
    ///
    /// Default: `SslMode::Prefer`
    pub ssl_mode: SslMode,

    /// Additional connection parameters.
    ///
    /// Default: `[]`
    pub params: Vec<(String, String)>,

    /// When connected via TCP to loopback, upgrade to Unix socket for better performance.
    ///
    /// Default: `true`
    pub prefer_unix_socket: bool,

    /// Maximum number of idle connections in the pool.
    ///
    /// Default: `100`
    pub pool_max_idle_conn: usize,

    /// Maximum number of concurrent connections (None = unlimited).
    ///
    /// Default: `None`
    pub pool_max_concurrency: Option<usize>,
}

impl Default for Opts {
    #[no_panic]
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 5432,
            socket: None,
            user: String::new(),
            database: None,
            password: None,
            application_name: None,
            ssl_mode: SslMode::Prefer,
            params: Vec::new(),
            prefer_unix_socket: true,
            pool_max_idle_conn: 100,
            pool_max_concurrency: None,
        }
    }
}

impl TryFrom<&Url> for Opts {
    type Error = Error;

    /// Parse a PostgreSQL connection URL.
    ///
    /// Format: `postgres://[user[:password]@]host[:port][/database][?param1=value1&param2=value2&..]`
    ///
    /// Supported query parameters:
    /// - `sslmode`: disable, prefer, require
    /// - `application_name`: application name
    /// - `prefer_unix_socket`: true/True/1/yes/on or false/False/0/no/off
    /// - `pool_max_idle_conn`: maximum idle connections (positive integer)
    /// - `pool_max_concurrency`: maximum concurrent connections (positive integer)
    #[no_panic]
    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if !["postgres", "pg"].contains(&url.scheme()) {
            return Err(Error::InvalidUsage(format!(
                "Invalid scheme: expected 'postgres://' or 'pg://', got '{}://'",
                url.scheme()
            )));
        }

        let mut opts = Opts {
            host: url.host_str().unwrap_or("localhost").to_string(),
            port: url.port().unwrap_or(5432),
            user: url.username().to_string(),
            password: url.password().map(|s| s.to_string()),
            database: url.path().strip_prefix('/').and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            }),
            ..Opts::default()
        };

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sslmode" => {
                    opts.ssl_mode = match value.as_ref() {
                        "disable" => SslMode::Disable,
                        "prefer" => SslMode::Prefer,
                        "require" => SslMode::Require,
                        _ => {
                            return Err(Error::InvalidUsage(format!(
                                "Invalid sslmode: expected one of ['disable', 'prefer', 'require'], got {}",
                                value
                            )));
                        }
                    };
                }
                "application_name" => {
                    opts.application_name = Some(value.to_string());
                }
                "prefer_unix_socket" => {
                    opts.prefer_unix_socket = match value.as_ref() {
                        "true" | "True" | "1" | "yes" | "on" => true,
                        "false" | "False" | "0" | "no" | "off" => false,
                        _ => {
                            return Err(Error::InvalidUsage(format!(
                                "Invalid prefer_unix_socket: {}",
                                value
                            )));
                        }
                    };
                }
                "pool_max_idle_conn" => {
                    opts.pool_max_idle_conn = value.parse().map_err(|_| {
                        Error::InvalidUsage(format!("Invalid pool_max_idle_conn: {}", value))
                    })?;
                }
                "pool_max_concurrency" => {
                    opts.pool_max_concurrency = Some(value.parse().map_err(|_| {
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

    #[no_panic]
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s).map_err(|e| Error::InvalidUsage(format!("Invalid URL: {}", e)))?;
        Self::try_from(&url)
    }
}
