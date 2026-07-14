use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use zeroize::Zeroizing;

use crate::{
    interfaces::{
        CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
    },
    policy::CanonicalTarget,
};

/// Reads scoped gateway credentials from macOS generic-password items.
///
/// Items use service `dev.cowshed.gateway` and account
/// `v1|<repo_id>|<protocol>|<base64url-exact-origin>`. The password is a
/// versioned JSON record containing the same binding, methods, normalized path
/// prefixes, header name, and secret header value. Lookup occurs only after the
/// gateway actor has admitted the exact request.
#[derive(Clone, Debug, Default)]
pub struct KeychainCredentialProvider;

impl KeychainCredentialProvider {
    pub const SERVICE: &'static str = "dev.cowshed.gateway";

    pub fn new() -> Self {
        Self
    }

    pub fn account_for(query: &CredentialQuery) -> String {
        format!(
            "v1|{}|{}|{}",
            query.repo_id,
            protocol_tag(query.protocol),
            URL_SAFE_NO_PAD.encode(query.origin.as_bytes())
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredCredential {
    version: u16,
    repo_id: String,
    protocol: String,
    origin: String,
    methods: Vec<String>,
    path_prefixes: Vec<String>,
    header_name: String,
    header_value: String,
}

fn decode_record(bytes: Vec<u8>) -> Result<CredentialRecord, CredentialError> {
    let bytes = Zeroizing::new(bytes);
    let parsed: StoredCredential = serde_json::from_slice(&bytes).map_err(|error| {
        CredentialError::Unavailable(format!("invalid Keychain record: {error}"))
    })?;
    if parsed.version != 1 {
        return Err(CredentialError::Unavailable(
            "unsupported Keychain credential version".to_owned(),
        ));
    }
    let protocol = parse_protocol(&parsed.protocol).ok_or_else(|| {
        CredentialError::Unavailable("invalid Keychain credential protocol".to_owned())
    })?;
    let header_name = http::HeaderName::from_bytes(parsed.header_name.as_bytes())
        .map_err(|_| CredentialError::InvalidHeader)?;
    Ok(CredentialRecord {
        repo_id: parsed.repo_id,
        protocol,
        origin: parsed.origin,
        methods: parsed.methods.into_iter().collect(),
        path_prefixes: parsed.path_prefixes,
        header_name,
        header_value: Zeroizing::new(parsed.header_value),
    })
}

#[async_trait::async_trait]
impl CredentialProvider for KeychainCredentialProvider {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        let url = url::Url::parse(&query.origin).map_err(|_| CredentialError::ScopeMismatch)?;
        if url.scheme() != "https"
            || CanonicalTarget::from_url(&url).is_err()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(CredentialError::ScopeMismatch);
        }
        lookup_platform(Self::account_for(query)).await
    }
}

#[cfg(target_os = "macos")]
async fn lookup_platform(account: String) -> Result<Option<CredentialRecord>, CredentialError> {
    tokio::task::spawn_blocking(move || {
        match security_framework::passwords::get_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
        ) {
            Ok(bytes) => decode_record(bytes).map(Some),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(None)
            }
            Err(error) => Err(CredentialError::Unavailable(format!(
                "macOS Keychain lookup failed with OSStatus {}",
                error.code()
            ))),
        }
    })
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

#[cfg(not(target_os = "macos"))]
async fn lookup_platform(_account: String) -> Result<Option<CredentialRecord>, CredentialError> {
    Err(CredentialError::Unavailable(
        "macOS Keychain credential provider is unavailable on this platform".to_owned(),
    ))
}

const fn protocol_tag(protocol: CredentialProtocol) -> &'static str {
    match protocol {
        CredentialProtocol::Generic => "generic",
        CredentialProtocol::Npm => "npm",
        CredentialProtocol::Cargo => "cargo",
        CredentialProtocol::Go => "go",
    }
}

fn parse_protocol(value: &str) -> Option<CredentialProtocol> {
    match value {
        "generic" => Some(CredentialProtocol::Generic),
        "npm" => Some(CredentialProtocol::Npm),
        "cargo" => Some(CredentialProtocol::Cargo),
        "go" => Some(CredentialProtocol::Go),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_record_is_strict_and_secret_is_not_debugged() {
        let bytes = br#"{
            "version":1,
            "repoId":"repo",
            "protocol":"generic",
            "origin":"https://example.test:443",
            "methods":["GET"],
            "pathPrefixes":["/v1"],
            "headerName":"authorization",
            "headerValue":"Bearer secret"
        }"#
        .to_vec();
        let record = decode_record(bytes).expect("decode record");
        assert_eq!(record.repo_id, "repo");
        assert!(!format!("{record:?}").contains("Bearer secret"));
    }
}
