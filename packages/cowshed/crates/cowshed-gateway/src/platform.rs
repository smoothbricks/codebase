#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use cowshed_gateway_types::{CanonicalTarget, normalize_path};

use crate::interfaces::{
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
};

/// Reads scoped gateway credentials from macOS generic-password items.
///
/// Items use service `dev.cowshed.gateway` and account
/// `v1|<repo_id>|<protocol>|<base64url-exact-origin>`. The password is a
/// versioned JSON record containing the same binding, methods, normalized path
/// prefixes, header name, and secret header value. Lookup occurs only after the
/// gateway actor has admitted the exact request.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Default)]
pub struct KeychainCredentialProvider;

#[cfg(target_os = "macos")]
impl KeychainCredentialProvider {
    pub const SERVICE: &'static str = "dev.cowshed.gateway";

    pub fn new() -> Self {
        Self
    }

    pub fn account_for(query: &CredentialQuery) -> String {
        account_for(query)
    }
}

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

    /// The stored record as the platform store holds it.
    ///
    /// Encoding exists only where a record is written, which is macOS: the Linux provider reads
    /// what the service manager placed in `$CREDENTIALS_DIRECTORY` and has no writable side, so
    /// this is gated exactly like the enrolment that calls it. Decoding stays shared — both
    /// platforms read the same format.
    #[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[async_trait::async_trait]
impl CredentialProvider for KeychainCredentialProvider {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        validate_query(query)?;
        lookup_keychain(Self::account_for(query)).await
    }
}

#[cfg(target_os = "macos")]
async fn lookup_keychain(account: String) -> Result<Option<CredentialRecord>, CredentialError> {
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

/// Whether an enrolled binding is present, without reading the secret back out.
///
/// `status` needs to say "a credential is installed for this origin" and nothing more. Handing
/// the secret to a reporting path just so it can be discarded is how secrets end up in output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialPresence {
    Installed,
    Absent,
}

/// Install or replace one scoped credential in the host's platform store.
///
/// Enrolment is a trusted-host operation: the secret arrives from the operator's environment,
/// travels in a `Zeroizing` buffer, and is handed to the platform store without passing through
/// argv, a temporary file, or any workspace-reachable path. Replacing an existing binding is
/// deliberate — rotation is the common case and a second command to delete first would only
/// create a window with no credential at all.
#[cfg(target_os = "macos")]
pub async fn store_scoped_credential(credential: ScopedCredential) -> Result<(), CredentialError> {
    validate_scope(&credential)?;
    let account = credential.account();
    let payload = credential.to_json()?;
    tokio::task::spawn_blocking(move || {
        security_framework::passwords::set_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
            payload.as_bytes(),
        )
        .map_err(|error| {
            CredentialError::Unavailable(format!(
                "macOS Keychain write failed with OSStatus {}",
                error.code()
            ))
        })
    })
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

/// Remove one scoped credential. An absent binding is already the requested state.
#[cfg(target_os = "macos")]
pub async fn remove_scoped_credential(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    let account = account_key(repo_id, protocol, origin);
    tokio::task::spawn_blocking(
        move || match security_framework::passwords::delete_generic_password(
            KeychainCredentialProvider::SERVICE,
            &account,
        ) {
            Ok(()) => Ok(CredentialPresence::Installed),
            Err(error) if error.code() == security_framework_sys::base::errSecItemNotFound => {
                Ok(CredentialPresence::Absent)
            }
            Err(error) => Err(CredentialError::Unavailable(format!(
                "macOS Keychain delete failed with OSStatus {}",
                error.code()
            ))),
        },
    )
    .await
    .map_err(|error| CredentialError::Unavailable(format!("Keychain task failed: {error}")))?
}

/// Is a usable binding installed for this origin? The secret is never returned.
#[cfg(target_os = "macos")]
pub async fn scoped_credential_presence(
    repo_id: &str,
    protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    let account = account_key(repo_id, protocol, origin);
    Ok(match lookup_keychain(account).await? {
        Some(_) => CredentialPresence::Installed,
        None => CredentialPresence::Absent,
    })
}

/// Enrolment is not available where the store is read-only by construction.
///
/// The Linux provider reads what the service manager placed in `$CREDENTIALS_DIRECTORY`; there
/// is no writable side to it. Saying so, with the mechanism named, is the honest answer — a
/// pretend success would leave an operator believing a credential exists.
#[cfg(not(target_os = "macos"))]
pub async fn store_scoped_credential(credential: ScopedCredential) -> Result<(), CredentialError> {
    validate_scope(&credential)?;
    Err(CredentialError::Unavailable(READ_ONLY_STORE.to_owned()))
}

#[cfg(not(target_os = "macos"))]
pub async fn remove_scoped_credential(
    _repo_id: &str,
    _protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    Err(CredentialError::Unavailable(READ_ONLY_STORE.to_owned()))
}

#[cfg(not(target_os = "macos"))]
pub async fn scoped_credential_presence(
    _repo_id: &str,
    _protocol: CredentialProtocol,
    origin: &str,
) -> Result<CredentialPresence, CredentialError> {
    validate_origin(origin)?;
    Err(CredentialError::Unavailable(READ_ONLY_STORE.to_owned()))
}

#[cfg(not(target_os = "macos"))]
const READ_ONLY_STORE: &str = "this platform's gateway credential store is read-only: the service manager supplies records \
     through $CREDENTIALS_DIRECTORY, so enrol the credential in that unit's configuration";

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
pub struct SystemdCredentialProvider {
    directory: PathBuf,
    authorized_uid: u32,
}

#[cfg(target_os = "linux")]
impl SystemdCredentialProvider {
    pub fn from_environment() -> Result<Self, CredentialError> {
        let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
            .map(PathBuf::from)
            .ok_or_else(|| {
                CredentialError::Unavailable(
                    "CREDENTIALS_DIRECTORY is required for Linux production credentials".to_owned(),
                )
            })?;
        Self::new(directory, unsafe { libc::geteuid() })
    }

    pub fn new(directory: PathBuf, authorized_uid: u32) -> Result<Self, CredentialError> {
        validate_directory(&directory, authorized_uid)?;
        Ok(Self {
            directory,
            authorized_uid,
        })
    }

    pub fn account_for(query: &CredentialQuery) -> String {
        account_for(query)
    }
}

#[cfg(target_os = "linux")]
#[async_trait::async_trait]
impl CredentialProvider for SystemdCredentialProvider {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        validate_query(query)?;
        let path = self.directory.join(Self::account_for(query));
        let directory = self.directory.clone();
        let authorized_uid = self.authorized_uid;
        tokio::task::spawn_blocking(move || {
            validate_directory(&directory, authorized_uid)?;
            read_systemd_credential(&path, authorized_uid)
        })
        .await
        .map_err(|error| {
            CredentialError::Unavailable(format!("systemd credential task failed: {error}"))
        })?
    }
}

#[cfg(target_os = "linux")]
fn validate_directory(path: &Path, authorized_uid: u32) -> Result<(), CredentialError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if !path.is_absolute() {
        return Err(CredentialError::Unavailable(
            "systemd credential directory must be absolute".to_owned(),
        ));
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| {
        CredentialError::Unavailable("systemd credential directory is unavailable".to_owned())
    })?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != authorized_uid
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(CredentialError::Unavailable(
            "systemd credential directory has insecure ownership or mode".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_systemd_credential(
    path: &Path,
    authorized_uid: u32,
) -> Result<Option<CredentialRecord>, CredentialError> {
    use std::{
        io::Read as _,
        os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    };

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(CredentialError::Unavailable(
                "systemd credential file is unavailable".to_owned(),
            ));
        }
    };
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != authorized_uid
        || !matches!(metadata.permissions().mode() & 0o777, 0o400 | 0o600)
        || metadata.len() > 1024 * 1024
    {
        return Err(CredentialError::Unavailable(
            "systemd credential file has invalid type, ownership, mode, or size".to_owned(),
        ));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| {
            CredentialError::Unavailable("systemd credential file could not be opened".to_owned())
        })?;
    let opened = file.metadata().map_err(|_| {
        CredentialError::Unavailable("systemd credential file metadata failed".to_owned())
    })?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(CredentialError::Unavailable(
            "systemd credential file changed during lookup".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            CredentialError::Unavailable("systemd credential file read failed".to_owned())
        })?;
    if bytes.len() > 1024 * 1024 {
        return Err(CredentialError::Unavailable(
            "systemd credential file exceeds 1 MiB".to_owned(),
        ));
    }
    decode_record(bytes).map(Some)
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

    fn enrollable() -> ScopedCredential {
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
        // have one owner, and this is what holds them to being one. Encoding only exists where
        // a record can be written, so the round trip is asserted there.
        #[cfg(target_os = "macos")]
        {
            let encoded = credential.to_json().expect("encodable");
            let decoded =
                decode_record(encoded.as_bytes().to_vec()).expect("the writer's own bytes decode");
            assert_eq!(decoded.origin, credential.origin);
            assert_eq!(decoded.path_prefixes, credential.path_prefixes);
            assert_eq!(decoded.header_name.as_str(), credential.header_name);
        }
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
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn systemd_credentials_require_scoped_owned_strict_files() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory =
            std::env::temp_dir().join(format!("cowshed-systemd-creds-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("directory");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .expect("directory mode");
        let provider =
            SystemdCredentialProvider::new(directory.clone(), unsafe { libc::geteuid() })
                .expect("provider");
        let query = CredentialQuery {
            workspace_id: "ws".to_owned(),
            repo_id: "repo".to_owned(),
            protocol: CredentialProtocol::Generic,
            origin: "https://example.test:443".to_owned(),
            method: http::Method::GET,
            path: "/v1/resource".to_owned(),
        };
        let path = directory.join(SystemdCredentialProvider::account_for(&query));
        std::fs::write(
            &path,
            br#"{"version":1,"repoId":"repo","protocol":"generic","origin":"https://example.test:443","methods":["GET"],"pathPrefixes":["/v1"],"headerName":"authorization","headerValue":"Bearer secret"}"#,
        )
        .expect("credential");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
            .expect("credential mode");
        let record = provider
            .lookup(&query)
            .await
            .expect("lookup")
            .expect("record");
        assert!(record.validate_for(&query));
        assert!(!format!("{record:?}").contains("Bearer secret"));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("bad mode");
        assert!(matches!(
            provider.lookup(&query).await,
            Err(CredentialError::Unavailable(_))
        ));
        let _ = std::fs::remove_dir_all(directory);
    }
}
