//! Connection options.

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
    pub host: String,
    pub port: u16,
    pub socket: Option<String>,
    pub user: String,
    pub database: Option<String>,
    pub password: Option<String>,
    pub application_name: Option<String>,
    pub ssl_mode: SslMode,
    pub params: Vec<(String, String)>,
    /// If connected via TCP to loopback, upgrade to Unix socket for better performance.
    pub upgrade_to_unix_socket: bool,
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
            ssl_mode: SslMode::Prefer,
            params: Vec::new(),
            upgrade_to_unix_socket: true,
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
    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        if url.scheme() != "postgres" && url.scheme() != "pg" {
            return Err(Error::InvalidUsage(format!(
                "Invalid scheme: expected 'postgres' or 'pg', got '{}'",
                url.scheme()
            )));
        }

        let mut opts = Opts::default();

        opts.host = url.host_str().unwrap_or("localhost").to_string();
        opts.port = url.port().unwrap_or(5432);
        opts.user = url.username().to_string();
        opts.password = url.password().map(|s| s.to_string());
        opts.database = url.path().strip_prefix('/').and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sslmode" => {
                    opts.ssl_mode = match value.as_ref() {
                        "disable" => SslMode::Disable,
                        "prefer" => SslMode::Prefer,
                        "require" => SslMode::Require,
                        _ => {
                            return Err(Error::InvalidUsage(format!("Invalid sslmode: {}", value)));
                        }
                    };
                }
                "application_name" => {
                    opts.application_name = Some(value.to_string());
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
