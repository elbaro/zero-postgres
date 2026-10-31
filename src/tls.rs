use std::sync::{Arc, OnceLock};

use rustls::ClientConfig;
use rustls_platform_verifier::BuilderVerifierExt;

use crate::error::{Error, Result};
use crate::opts::Opts;

/// Returns `opts.tls_config`, or the shared platform config when it is unset.
pub fn client_config(opts: &Opts) -> Result<Arc<ClientConfig>> {
    match &opts.tls_config {
        Some(config) => Ok(Arc::clone(config)),
        None => platform_client_config(),
    }
}

/// Returns the rustls client config shared by all TLS connections without
/// `Opts::tls_config`.
///
/// The platform verifier is built on the first call and reused afterwards.
fn platform_client_config() -> Result<Arc<ClientConfig>> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(Arc::clone(config));
    }
    let config = Arc::new(build_client_config()?);
    Ok(Arc::clone(CONFIG.get_or_init(|| config)))
}

fn build_client_config() -> Result<ClientConfig> {
    // An explicit provider avoids a panic when both aws-lc-rs and ring are
    // enabled in the dependency graph and no process default is installed.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_platform_verifier()?
        .with_no_client_auth())
}

#[cfg(any(feature = "sync-tls", feature = "tokio-tls"))]
pub fn server_name(host: &str) -> Result<rustls::pki_types::ServerName<'static>> {
    rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| Error::InvalidUsage(format!("invalid TLS server name {host:?}: {e}")))
}
