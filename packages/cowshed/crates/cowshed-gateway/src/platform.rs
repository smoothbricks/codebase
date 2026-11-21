//! Scoped gateway credentials: what a record is, and the platform store that holds it.
//!
//! The record is stated here once for every platform: its binding, its JSON form, the rules a
//! usable one meets and the account key that names it. Where records are kept differs by
//! platform and lives in that platform's file: `macos.rs` keeps them as Keychain generic
//! passwords, `linux.rs` as files in the user's credential directory. Each file defines the
//! provider the gateway reads through and the three enrolment functions, behind the same
//! signatures, so nothing here or above this module branches on the platform.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use cowshed_gateway_types::{CanonicalTarget, normalize_path};

use crate::interfaces::{CredentialError, CredentialProtocol, CredentialQuery, CredentialRecord};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use platform::{remove_scoped_credential, scoped_credential_scope, store_scoped_credential};
// The provider each platform alone has, which only that platform's own callers name
// (`macos.rs`: `KeychainCredentialProvider`; `linux.rs`: `SystemdCredentialProvider`). A
// platform file's public surface is its provider and the three functions above, nothing else.
pub use platform::*;

#[derive(Deserialize, Serialize)]
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

const STORED_CREDENTIAL_VERSION: u16 = 1;

/// What the operator asked to enrol, before it becomes a stored record.
///
/// The same shape the gateway reads back, so enrolment cannot invent a record the lookup path
/// would reject: [`validate_scope`] is the single description of a usable binding, applied when
/// writing and again when reading.
pub struct ScopedCredential {
    pub repo_id: String,
    pub protocol: CredentialProtocol,
    pub origin: String,
    pub methods: Vec<String>,
    pub path_prefixes: Vec<String>,
    pub header_name: String,
    pub header_value: Zeroizing<String>,
}

impl ScopedCredential {
    /// The store key for this binding — the same account the provider looks up.
    pub fn account(&self) -> String {
        account_key(&self.repo_id, self.protocol, &self.origin)
    }

    /// The stored record as the platform store holds it: the bytes enrolment writes and
    /// [`decode_record`] reads back, the same on every platform.
    fn to_json(&self) -> Result<Zeroizing<String>, CredentialError> {
        let stored = StoredCredential {
            version: STORED_CREDENTIAL_VERSION,
            repo_id: self.repo_id.clone(),
            protocol: protocol_tag(self.protocol).to_owned(),
            origin: self.origin.clone(),
            methods: self.methods.clone(),
            path_prefixes: self.path_prefixes.clone(),
            header_name: self.header_name.clone(),
            header_value: self.header_value.to_string(),
        };
        serde_json::to_string(&stored)
            .map(Zeroizing::new)
            .map_err(|error| {
                CredentialError::Unavailable(format!("scoped credential is not encodable: {error}"))
            })
    }
}

/// Every refusal an enrolment or a stored record can earn, in the operator's terms.
///
/// Stated once and applied twice — writing and reading — so a record that exists is a record the
/// gateway can use. A binding that admits everything is refused here rather than at the moment a
/// request would have carried the secret somewhere nobody chose.
pub fn validate_scope(credential: &ScopedCredential) -> Result<(), CredentialError> {
    validate_origin(&credential.origin)?;
    if credential.repo_id.is_empty() {
        return Err(CredentialError::ScopeMismatch);
    }
    if credential.methods.is_empty()
        || credential
            .methods
            .iter()
            .any(|method| !matches!(method.as_str(), "GET" | "HEAD"))
    {
        return Err(CredentialError::ScopeMismatch);
    }
    if credential.path_prefixes.is_empty() {
        return Err(CredentialError::ScopeMismatch);
    }
    for prefix in &credential.path_prefixes {
        validate_prefix(prefix)?;
    }
    let header_name = http::HeaderName::from_bytes(credential.header_name.as_bytes())
        .map_err(|_| CredentialError::InvalidHeader)?;
    if matches!(
        header_name.as_str(),
        "proxy-authorization" | "cookie" | "set-cookie"
    ) {
        return Err(CredentialError::InvalidHeader);
    }
    if credential.header_value.is_empty()
        || http::HeaderValue::from_str(credential.header_value.as_str()).is_err()
    {
        return Err(CredentialError::InvalidHeader);
    }
    Ok(())
}

/// A stored prefix must already be canonical.
///
/// The request side matches a prefix against RAW request bytes, so a prefix carrying its own
/// escapes would be compared against something it does not equal — silently narrower or wider
/// than the operator wrote. Refusing it at both ends keeps "what was enrolled" and "what is
/// enforced" the same sentence. A bare `/` is refused too: that is not a scope.
fn validate_prefix(prefix: &str) -> Result<(), CredentialError> {
    let normalized = normalize_path(prefix).map_err(|_| CredentialError::ScopeMismatch)?;
    if normalized != prefix || prefix == "/" {
        return Err(CredentialError::ScopeMismatch);
    }
    Ok(())
}

/// Decode a stored record, holding it to the same rules enrolment applied.
///
/// A record is trusted host state, but "trusted" is not "well-formed": an item written by hand,
/// or by an older tool, can carry a prefix that no request will ever equal or a method the
/// egress grant cannot admit. Refusing it here names the problem while an operator is looking,
/// instead of leaving a credential that silently never attaches.
fn decode_record(bytes: Vec<u8>) -> Result<CredentialRecord, CredentialError> {
    let bytes = Zeroizing::new(bytes);
    let parsed: StoredCredential = serde_json::from_slice(&bytes).map_err(|error| {
        CredentialError::Unavailable(format!("invalid scoped credential record: {error}"))
    })?;
    if parsed.version != STORED_CREDENTIAL_VERSION {
        return Err(CredentialError::Unavailable(
            "unsupported scoped credential version".to_owned(),
        ));
    }
    let protocol = parse_protocol(&parsed.protocol).ok_or_else(|| {
        CredentialError::Unavailable("invalid scoped credential protocol".to_owned())
    })?;
    let credential = ScopedCredential {
        repo_id: parsed.repo_id,
        protocol,
        origin: parsed.origin,
        methods: parsed.methods,
        path_prefixes: parsed.path_prefixes,
        header_name: parsed.header_name,
        header_value: Zeroizing::new(parsed.header_value),
    };
    validate_scope(&credential)?;
    let header_name = http::HeaderName::from_bytes(credential.header_name.as_bytes())
        .map_err(|_| CredentialError::InvalidHeader)?;
    Ok(CredentialRecord {
        repo_id: credential.repo_id,
        protocol: credential.protocol,
        origin: credential.origin,
        methods: credential.methods.into_iter().collect(),
        path_prefixes: credential.path_prefixes,
        header_name,
        header_value: credential.header_value,
    })
}

fn account_for(query: &CredentialQuery) -> String {
    account_key(&query.repo_id, query.protocol, &query.origin)
}

fn account_key(repo_id: &str, protocol: CredentialProtocol, origin: &str) -> String {
    format!(
        "v1|{}|{}|{}",
        repo_id,
        protocol_tag(protocol),
        URL_SAFE_NO_PAD.encode(origin.as_bytes())
    )
}

fn validate_query(query: &CredentialQuery) -> Result<(), CredentialError> {
    validate_origin(&query.origin)
}

/// An origin is a bare `https://host:port`, and nothing else.
///
/// The gateway builds the lookup origin from the admitted target, and a record matches it by
/// string equality. The check is therefore the equality itself: the text must be exactly what
/// [`CanonicalTarget::origin`] would produce, explicit port included. A host with the port left
/// implicit, a path, a query, a fragment, or embedded credentials all fail that comparison, so
/// what would otherwise be a credential that silently never matches becomes a refusal an
/// operator sees while enrolling it.
fn validate_origin(origin: &str) -> Result<(), CredentialError> {
    let url = url::Url::parse(origin).map_err(|_| CredentialError::ScopeMismatch)?;
    if url.scheme() != "https" {
        return Err(CredentialError::ScopeMismatch);
    }
    let target = CanonicalTarget::from_url(&url).map_err(|_| CredentialError::ScopeMismatch)?;
    if target.origin() != origin {
        return Err(CredentialError::ScopeMismatch);
    }
    Ok(())
}

/// Whether a binding was there, without reading the secret back out.
///
/// What `rm` answers: an absent binding is already the requested state, and saying which of the
/// two happened is the whole difference between "removed" and "was not enrolled".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialPresence {
    Installed,
    Absent,
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

    /// One usable binding, shared with the platform files' own tests.
    pub(super) fn enrollable() -> ScopedCredential {
        ScopedCredential {
            repo_id: "owner/repo".to_owned(),
            protocol: CredentialProtocol::Generic,
            origin: "https://registry.test:443".to_owned(),
            methods: vec!["GET".to_owned(), "HEAD".to_owned()],
            path_prefixes: vec!["/api/packages/owner/npm/".to_owned()],
            header_name: "authorization".to_owned(),
            header_value: Zeroizing::new("Bearer host-held".to_owned()),
        }
    }

    #[test]
    fn an_enrollable_binding_keys_itself_by_repo_protocol_and_exact_origin() {
        let credential = enrollable();
        validate_scope(&credential).expect("a usable binding");
        assert_eq!(
            credential.account(),
            format!(
                "v1|owner/repo|generic|{}",
                URL_SAFE_NO_PAD.encode(b"https://registry.test:443")
            )
        );
        // What enrolment writes is what a lookup decodes. The two halves of the record format
        // have one owner, and this is what holds them to being one.
        let encoded = credential.to_json().expect("encodable");
        let decoded =
            decode_record(encoded.as_bytes().to_vec()).expect("the writer's own bytes decode");
        assert_eq!(decoded.origin, credential.origin);
        assert_eq!(decoded.path_prefixes, credential.path_prefixes);
        assert_eq!(decoded.header_name.as_str(), credential.header_name);
    }

    #[test]
    fn a_binding_no_request_could_ever_match_is_refused_at_enrolment() {
        // The gateway builds the lookup origin from the admitted target, which always carries an
        // explicit port; anything else can only ever fail to match.
        for origin in [
            "https://registry.test",
            "https://registry.test:443/api/packages/owner/npm/",
            "http://registry.test:80",
            "https://user:pass@registry.test:443",
            "https://registry.test:443?x=1",
        ] {
            let mut credential = enrollable();
            credential.origin = origin.to_owned();
            assert!(
                validate_scope(&credential).is_err(),
                "{origin} must be refused"
            );
        }
    }

    #[test]
    fn a_scope_that_admits_everything_or_cannot_be_matched_literally_is_refused() {
        for prefixes in [
            vec![],
            vec!["/".to_owned()],
            vec!["api/packages".to_owned()],
            vec!["/api/%70ackages/owner/npm/".to_owned()],
            vec!["/api/../packages/".to_owned()],
        ] {
            let mut credential = enrollable();
            credential.path_prefixes = prefixes.clone();
            assert!(
                validate_scope(&credential).is_err(),
                "{prefixes:?} must be refused"
            );
        }
    }

    #[test]
    fn only_the_methods_an_intercept_grant_admits_are_enrollable() {
        for methods in [vec![], vec!["POST".to_owned()], vec!["get".to_owned()]] {
            let mut credential = enrollable();
            credential.methods = methods.clone();
            assert!(
                validate_scope(&credential).is_err(),
                "{methods:?} must be refused"
            );
        }
    }

    #[test]
    fn a_header_that_would_be_stripped_or_forged_is_refused() {
        for header in [
            "proxy-authorization",
            "cookie",
            "set-cookie",
            "not a header",
        ] {
            let mut credential = enrollable();
            credential.header_name = header.to_owned();
            assert!(
                validate_scope(&credential).is_err(),
                "{header} must be refused"
            );
        }
        let mut empty = enrollable();
        empty.header_value = Zeroizing::new(String::new());
        assert!(validate_scope(&empty).is_err());
    }

    #[test]
    fn a_stored_record_is_held_to_the_same_rules_as_an_enrolment() {
        // A prefix that is not canonical is compared against raw request bytes it cannot equal:
        // refused on read, so it can never become a credential that silently never attaches.
        let bytes = br#"{
            "version":1,
            "repoId":"repo",
            "protocol":"generic",
            "origin":"https://example.test:443",
            "methods":["GET"],
            "pathPrefixes":["/v1/%70kg"],
            "headerName":"authorization",
            "headerValue":"Bearer secret"
        }"#
        .to_vec();
        assert!(decode_record(bytes).is_err());
        let host_wide = br#"{
            "version":1,
            "repoId":"repo",
            "protocol":"generic",
            "origin":"https://example.test:443",
            "methods":["GET"],
            "pathPrefixes":["/"],
            "headerName":"authorization",
            "headerValue":"Bearer secret"
        }"#
        .to_vec();
        assert!(decode_record(host_wide).is_err());
    }
}
