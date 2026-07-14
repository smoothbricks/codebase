#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

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
        CredentialError::Unavailable(format!("invalid scoped credential record: {error}"))
    })?;
    if parsed.version != 1 {
        return Err(CredentialError::Unavailable(
            "unsupported scoped credential version".to_owned(),
        ));
    }
    let protocol = parse_protocol(&parsed.protocol).ok_or_else(|| {
        CredentialError::Unavailable("invalid scoped credential protocol".to_owned())
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

fn account_for(query: &CredentialQuery) -> String {
    format!(
        "v1|{}|{}|{}",
        query.repo_id,
        protocol_tag(query.protocol),
        URL_SAFE_NO_PAD.encode(query.origin.as_bytes())
    )
}

fn validate_query(query: &CredentialQuery) -> Result<(), CredentialError> {
    let url = url::Url::parse(&query.origin).map_err(|_| CredentialError::ScopeMismatch)?;
    if url.scheme() != "https"
        || CanonicalTarget::from_url(&url).is_err()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
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
    let mut file = std::fs::OpenOptions::new()
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
