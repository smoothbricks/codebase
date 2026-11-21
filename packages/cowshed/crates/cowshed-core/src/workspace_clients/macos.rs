//! Where macOS keeps the root bundle its OpenSSL-built clients verify against.

pub const SYSTEM_TRUST_BUNDLE: &str = "/etc/ssl/cert.pem";
