use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use cowshed_gateway::TOKEN_BYTES;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
    PKCS_ECDSA_P256_SHA256,
};
use thiserror::Error;
use x509_parser::prelude::{FromDer, X509Certificate};
use zeroize::{Zeroize, Zeroizing};

use crate::metadata::{
    Platform, PortBlock, WorkspaceIncarnation, WorkspaceName, write_atomic_bytes,
};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::storage::lifecycle::LifecycleWorkspace;
use crate::workspace_environment::{WorkspaceEnvironmentError, write_workspace_environment};

pub const CA_CERTIFICATE_PATH: &str = ".cowshed/ca.pem";
pub const WORKSPACE_TOKEN_PATH: &str = ".cowshed/token";
const CREDENTIAL_DIRECTORY: &str = ".cowshed";
// base64url without padding: four characters per three bytes, rounded up. Derived rather than
// restated so the token's on-disk width cannot drift from the byte count the gateway defines.
const TOKEN_ENCODED_BYTES: usize = (TOKEN_BYTES * 4).div_ceil(3);
const MAX_CERTIFICATE_PEM_BYTES: u64 = 64 * 1024;
const MAX_PRIVATE_KEY_PEM_BYTES: u64 = 64 * 1024;

/// Fully validated gateway credential material for one exact workspace incarnation.
///
/// Secret bytes are zeroized on drop, cannot be cloned or serialized, and are always redacted
/// from diagnostics.
pub struct GatewayWorkspaceCredentials {
    token: Zeroizing<String>,
    certificate_pem: Zeroizing<String>,
    private_key_pem: Zeroizing<String>,
}

impl GatewayWorkspaceCredentials {
    pub fn token(&self) -> &str {
        self.token.as_str()
    }

    pub fn certificate_pem(&self) -> &str {
        self.certificate_pem.as_str()
    }

    pub fn private_key_pem(&self) -> &str {
        self.private_key_pem.as_str()
    }
}

impl fmt::Debug for GatewayWorkspaceCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayWorkspaceCredentials")
            .field("token", &"[REDACTED]")
            .field("certificate_pem", &"[REDACTED]")
            .field("private_key_pem", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum WorkspaceCredentialError {
    #[error("workspace credential I/O failed while {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("workspace credential publication failed for {path}")]
    Publication { path: PathBuf },
    #[error("workspace credential generation failed during {0}")]
    Generation(&'static str),
    #[error("invalid workspace credential asset {kind} at {path}")]
    InvalidAsset { kind: &'static str, path: PathBuf },
    #[error(transparent)]
    Environment(#[from] WorkspaceEnvironmentError),
}

pub fn mint_workspace_credentials(
    workspace: &LifecycleWorkspace,
    mount_point: &Path,
    workspace_mount: &Path,
    platform: Platform,
    port_block: Option<PortBlock>,
    private_key_path: &Path,
) -> Result<(), WorkspaceCredentialError> {
    let credential_directory = ensure_credential_directory(mount_point)?;
    let certificate_path = mount_point.join(CA_CERTIFICATE_PATH);
    let token_path = mount_point.join(WORKSPACE_TOKEN_PATH);

    let signing_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
        .map_err(|_| WorkspaceCredentialError::Generation("P-256 key generation"))?;
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|_| WorkspaceCredentialError::Generation("CA parameters"))?;
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::OrganizationName, "cowshed");
    distinguished_name.push(
        DnType::CommonName,
        credential_subject(workspace.repo(), workspace.name(), workspace.incarnation()),
    );
    params.distinguished_name = distinguished_name;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let certificate = params
        .self_signed(&signing_key)
        .map_err(|_| WorkspaceCredentialError::Generation("self-signed CA certificate"))?;

    let private_key = Zeroizing::new(signing_key.serialize_pem());
    let certificate_pem = certificate.pem();
    let mut token_bytes = Zeroizing::new([0_u8; TOKEN_BYTES]);
    getrandom::fill(&mut token_bytes[..])
        .map_err(|_| WorkspaceCredentialError::Generation("workspace token"))?;
    let token = Zeroizing::new(URL_SAFE_NO_PAD.encode(&token_bytes[..]));

    publish_asset(private_key_path, private_key.as_bytes())?;
    publish_asset(&certificate_path, certificate_pem.as_bytes())?;
    publish_asset(&token_path, token.as_bytes())?;
    write_workspace_environment(mount_point, workspace_mount, &token, platform, port_block)?;
    sync_directory(&credential_directory, "syncing credential directory")?;

    // A certificate minted a line above carries the current identity, so the tightest possible
    // owned set is the honest one to read it back with.
    validate_workspace_credentials(
        &OwnedRepoIds::sole(workspace.repo().clone()),
        workspace,
        mount_point,
        private_key_path,
    )
}

pub fn validate_private_key(path: &Path) -> Result<(), WorkspaceCredentialError> {
    validate_mode_and_type(path, "private key")?;
    let bytes = Zeroizing::new(read_asset(path, "reading private key")?);
    let pem = std::str::from_utf8(bytes.as_ref()).map_err(|_| invalid("private key", path))?;
    KeyPair::from_pem_and_sign_algo(pem, &PKCS_ECDSA_P256_SHA256)
        .map_err(|_| invalid("private key", path))?;
    Ok(())
}

pub fn validate_public_workspace_assets(
    owned_repo_ids: &OwnedRepoIds,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    mount_point: &Path,
) -> Result<(), WorkspaceCredentialError> {
    let certificate_path = mount_point.join(CA_CERTIFICATE_PATH);
    validate_certificate(&certificate_path, |certificate| {
        validate_certificate_identity(
            certificate,
            owned_repo_ids,
            workspace,
            incarnation,
            &certificate_path,
        )
    })?;
    validate_token(&mount_point.join(WORKSPACE_TOKEN_PATH))
}

pub fn validate_workspace_credentials(
    owned_repo_ids: &OwnedRepoIds,
    workspace: &LifecycleWorkspace,
    mount_point: &Path,
    private_key_path: &Path,
) -> Result<(), WorkspaceCredentialError> {
    validate_mode_and_type(private_key_path, "private key")?;
    let key_bytes = Zeroizing::new(read_asset(private_key_path, "reading private key")?);
    let key_pem = std::str::from_utf8(key_bytes.as_ref())
        .map_err(|_| invalid("private key", private_key_path))?;
    let signing_key = KeyPair::from_pem_and_sign_algo(key_pem, &PKCS_ECDSA_P256_SHA256)
        .map_err(|_| invalid("private key", private_key_path))?;

    let certificate_path = mount_point.join(CA_CERTIFICATE_PATH);
    validate_certificate(&certificate_path, |certificate| {
        validate_certificate_identity(
            certificate,
            owned_repo_ids,
            workspace.name(),
            workspace.incarnation(),
            &certificate_path,
        )?;
        if certificate.public_key().subject_public_key.data.as_ref() != signing_key.public_key_raw()
        {
            return Err(invalid("certificate/key pair", &certificate_path));
        }
        Ok(())
    })?;
    validate_token(&mount_point.join(WORKSPACE_TOKEN_PATH))
}

/// Validate identity, file type, permissions, certificate/key pairing, and token encoding before
/// returning the exact bounded credential contents needed by the gateway.
pub fn read_gateway_workspace_credentials(
    owned_repo_ids: &OwnedRepoIds,
    workspace: &LifecycleWorkspace,
    mount_point: &Path,
    private_key_path: &Path,
) -> Result<GatewayWorkspaceCredentials, WorkspaceCredentialError> {
    validate_workspace_credentials(owned_repo_ids, workspace, mount_point, private_key_path)?;

    let token_path = mount_point.join(WORKSPACE_TOKEN_PATH);
    let certificate_path = mount_point.join(CA_CERTIFICATE_PATH);
    let token = read_bounded_utf8(
        &token_path,
        "reading gateway workspace token",
        TOKEN_ENCODED_BYTES as u64,
        "workspace token",
    )?;
    let certificate_pem = read_bounded_utf8(
        &certificate_path,
        "reading gateway CA certificate",
        MAX_CERTIFICATE_PEM_BYTES,
        "CA certificate",
    )?;
    let private_key_pem = read_bounded_utf8(
        private_key_path,
        "reading gateway private key",
        MAX_PRIVATE_KEY_PEM_BYTES,
        "private key",
    )?;

    Ok(GatewayWorkspaceCredentials {
        token,
        certificate_pem,
        private_key_pem,
    })
}

fn credential_subject(
    repo: &RepoId,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
) -> String {
    format!(
        "cowshed:{}:{}:{}",
        repo.as_str(),
        workspace.as_str(),
        incarnation.as_str()
    )
}

fn ensure_credential_directory(mount_point: &Path) -> Result<PathBuf, WorkspaceCredentialError> {
    let directory = mount_point.join(CREDENTIAL_DIRECTORY);
    match fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(invalid("credential directory", &directory)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&directory).map_err(|source| {
                io_failure("creating credential directory", &directory, source)
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).map_err(
                    |source| io_failure("setting credential directory mode", &directory, source),
                )?;
            }
            sync_directory(mount_point, "publishing credential directory")?;
        }
        Err(source) => {
            return Err(io_failure(
                "inspecting credential directory",
                &directory,
                source,
            ));
        }
    }
    Ok(directory)
}

fn publish_asset(path: &Path, bytes: &[u8]) -> Result<(), WorkspaceCredentialError> {
    write_atomic_bytes(path, bytes).map_err(|_| WorkspaceCredentialError::Publication {
        path: path.to_owned(),
    })
}

fn validate_certificate<T>(
    path: &Path,
    validate: impl FnOnce(&X509Certificate<'_>) -> Result<T, WorkspaceCredentialError>,
) -> Result<T, WorkspaceCredentialError> {
    validate_mode_and_type(path, "CA certificate")?;
    let bytes = read_asset(path, "reading CA certificate")?;
    let (pem_remainder, pem) =
        x509_parser::pem::parse_x509_pem(&bytes).map_err(|_| invalid("CA certificate", path))?;
    if !pem_remainder.is_empty() || pem.label != "CERTIFICATE" {
        return Err(invalid("CA certificate", path));
    }
    let (remainder, certificate) =
        X509Certificate::from_der(&pem.contents).map_err(|_| invalid("CA certificate", path))?;
    if !remainder.is_empty() || certificate.subject() != certificate.issuer() {
        return Err(invalid("self-signed CA certificate", path));
    }
    certificate
        .verify_signature(None)
        .map_err(|_| invalid("self-signed CA certificate", path))?;
    let constraints = certificate
        .basic_constraints()
        .map_err(|_| invalid("CA certificate", path))?
        .ok_or_else(|| invalid("CA certificate", path))?;
    if !constraints.value.ca {
        return Err(invalid("CA certificate", path));
    }
    validate(&certificate)
}

/// The certificate subject is the one identity stamp sealed inside a workspace image that cannot
/// be corrected without re-minting, and re-minting needs the image mounted — which is exactly what
/// reading the subject gates. Splitting the subject back into its three fields and asking the
/// binding whether it owns the repository named there breaks that cycle on the single axis an
/// identity change moves. The workspace name and its random incarnation still have to match
/// exactly, so a credential remains pinned to one workspace of one project.
fn validate_certificate_identity(
    certificate: &X509Certificate<'_>,
    owned_repo_ids: &OwnedRepoIds,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    path: &Path,
) -> Result<(), WorkspaceCredentialError> {
    let mut common_names = certificate.subject().iter_common_name();
    let common_name = common_names
        .next()
        .and_then(|attribute| attribute.as_str().ok())
        .ok_or_else(|| invalid("CA certificate identity", path))?;
    if common_names.next().is_some() {
        return Err(invalid("CA certificate identity", path));
    }
    let (repo, subject_workspace, subject_incarnation) = parse_credential_subject(common_name)
        .ok_or_else(|| invalid("CA certificate identity", path))?;
    if subject_workspace != workspace.as_str()
        || subject_incarnation != incarnation.as_str()
        || !RepoId::parse(repo).is_ok_and(|repo| owned_repo_ids.accepts(&repo))
    {
        return Err(invalid("CA certificate identity", path));
    }
    Ok(())
}

/// The three fields [`credential_subject`] encodes, split back out. `None` for any subject that is
/// not exactly `cowshed:<owner/repo>:<workspace>:<incarnation>`, so a subject with a stray
/// separator or a missing field is refused rather than partially believed.
fn parse_credential_subject(subject: &str) -> Option<(&str, &str, &str)> {
    let mut fields = subject.split(':');
    let prefix = fields.next()?;
    let repo = fields.next()?;
    let workspace = fields.next()?;
    let incarnation = fields.next()?;
    if prefix != "cowshed" || fields.next().is_some() {
        return None;
    }
    Some((repo, workspace, incarnation))
}

fn validate_token(path: &Path) -> Result<(), WorkspaceCredentialError> {
    validate_mode_and_type(path, "workspace token")?;
    let encoded = Zeroizing::new(read_asset(path, "reading workspace token")?);
    if encoded.len() != TOKEN_ENCODED_BYTES || encoded.contains(&b'=') {
        return Err(invalid("workspace token", path));
    }
    let decoded = Zeroizing::new(
        URL_SAFE_NO_PAD
            .decode(&encoded[..])
            .map_err(|_| invalid("workspace token", path))?,
    );
    if decoded.len() != TOKEN_BYTES {
        return Err(invalid("workspace token", path));
    }
    Ok(())
}

fn validate_mode_and_type(path: &Path, kind: &'static str) -> Result<(), WorkspaceCredentialError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_failure("inspecting credential asset", path, source))?;
    if !metadata.file_type().is_file() {
        return Err(invalid(kind, path));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(invalid(kind, path));
        }
    }
    Ok(())
}

fn read_asset(path: &Path, operation: &'static str) -> Result<Vec<u8>, WorkspaceCredentialError> {
    fs::read(path).map_err(|source| io_failure(operation, path, source))
}
fn read_bounded_utf8(
    path: &Path,
    operation: &'static str,
    maximum: u64,
    kind: &'static str,
) -> Result<Zeroizing<String>, WorkspaceCredentialError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|source| io_failure(operation, path, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| io_failure(operation, path, source))?;
    if !metadata.file_type().is_file() {
        return Err(invalid(kind, path));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(invalid(kind, path));
        }
    }
    if metadata.len() > maximum {
        return Err(invalid(kind, path));
    }

    let capacity = usize::try_from(metadata.len()).map_err(|_| invalid(kind, path))?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(capacity));
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_failure(operation, path, source))?;
    if bytes.len() as u64 > maximum {
        return Err(invalid(kind, path));
    }
    // `FromUtf8Error` owns the secret bytes and does not zeroize on drop.
    String::from_utf8(std::mem::take(&mut *bytes)).map_or_else(
        |error| {
            let mut leaked = error.into_bytes();
            leaked.zeroize();
            Err(invalid(kind, path))
        },
        |text| Ok(Zeroizing::new(text)),
    )
}

fn sync_directory(path: &Path, operation: &'static str) -> Result<(), WorkspaceCredentialError> {
    crate::fsio::sync_directory(path).map_err(|source| io_failure(operation, path, source))
}

fn io_failure(operation: &'static str, path: &Path, source: io::Error) -> WorkspaceCredentialError {
    WorkspaceCredentialError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn invalid(kind: &'static str, path: &Path) -> WorkspaceCredentialError {
    WorkspaceCredentialError::InvalidAsset {
        kind,
        path: path.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use crate::workspace_environment::{GO_ENV, PORT_BASE_ENV, WORKSPACE_TOKEN_ENV};

    use super::*;
    use crate::metadata::{ImageFormat, WorkspaceRole};
    use crate::storage::lifecycle::Revision;

    /// The tightest owned set for a workspace: exactly the identity it answers to.
    fn sole(workspace: &LifecycleWorkspace) -> OwnedRepoIds {
        OwnedRepoIds::sole(workspace.repo().clone())
    }

    fn workspace(incarnation: &str) -> LifecycleWorkspace {
        LifecycleWorkspace::new(
            RepoId::parse("acme/widget").expect("repo"),
            WorkspaceName::session("raven").expect("workspace"),
            WorkspaceIncarnation::new(incarnation).expect("incarnation"),
            Revision::new(7),
            Revision::new(3),
            WorkspaceRole::Workspace,
            ImageFormat::Sparse,
        )
        .expect("lifecycle workspace")
    }

    fn mint_credentials(
        workspace: &LifecycleWorkspace,
        mount: &Path,
        private_key: &Path,
    ) -> Result<(), WorkspaceCredentialError> {
        mint_workspace_credentials(
            workspace,
            mount,
            mount,
            Platform::Macos,
            Some(PortBlock::new(40_960, 16).expect("port block")),
            private_key,
        )
    }

    fn test_root(case: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-workspace-credentials-{case}-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).expect("test root");
        root
    }

    #[test]
    fn token_rotation_rewrites_the_in_image_environment() {
        let root = test_root("environment-rotation");
        let image_mount = root.join("image");
        fs::create_dir(&image_mount).expect("image mount");
        let canonical_mount = Path::new("/Users/test/.cowshed/mnt/acme/widget/raven");
        let key_path = root.join("raven.ca.key");
        let workspace = workspace("00112233445566778899aabbccddeeff");
        let port_block = crate::metadata::PortBlock::new(40_960, 16).expect("port block");

        mint_workspace_credentials(
            &workspace,
            &image_mount,
            canonical_mount,
            crate::metadata::Platform::Macos,
            Some(port_block),
            &key_path,
        )
        .expect("first mint");
        let first_token =
            fs::read_to_string(image_mount.join(WORKSPACE_TOKEN_PATH)).expect("token");
        let first_environment =
            fs::read_to_string(image_mount.join(".cowshed/env")).expect("environment");
        assert_eq!(
            first_environment,
            format!(
                "export {GO_ENV}=/Users/test/.cowshed/mnt/acme/widget/raven/.cowshed/cache/go/env\nexport {WORKSPACE_TOKEN_ENV}={first_token}\nexport {PORT_BASE_ENV}=40960\n"
            )
        );

        mint_workspace_credentials(
            &workspace,
            &image_mount,
            canonical_mount,
            crate::metadata::Platform::Macos,
            Some(port_block),
            &key_path,
        )
        .expect("rotated mint");
        let second_token =
            fs::read_to_string(image_mount.join(WORKSPACE_TOKEN_PATH)).expect("rotated token");
        let second_environment =
            fs::read_to_string(image_mount.join(".cowshed/env")).expect("rotated environment");
        assert_ne!(second_token, first_token);
        assert_ne!(second_environment, first_environment);
        assert!(
            second_environment
                .contains(&format!("export COWSHED_WORKSPACE_TOKEN={second_token}\n"))
        );
        assert!(!second_environment.contains(&first_token));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn linux_environment_omits_the_port_export() {
        let root = test_root("linux-environment");
        let image_mount = root.join("image");
        fs::create_dir(&image_mount).expect("image mount");
        let key_path = root.join("raven.ca.key");

        mint_workspace_credentials(
            &workspace("00112233445566778899aabbccddeeff"),
            &image_mount,
            Path::new("/home/test/.cowshed/mnt/acme/widget/raven"),
            crate::metadata::Platform::Linux,
            None,
            &key_path,
        )
        .expect("mint Linux credentials");
        let environment =
            fs::read_to_string(image_mount.join(".cowshed/env")).expect("environment");
        assert_eq!(environment.lines().count(), 2);
        assert!(!environment.contains("COWSHED_PORT_BASE"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn minted_assets_are_private_p256_ca_and_exact_base64url_token() {
        let root = test_root("contract");
        let mount = root.join("mount");
        fs::create_dir(&mount).expect("mount");
        let key_path = root.join("staged.sparseimage.ca.key");
        let workspace = workspace("00112233445566778899aabbccddeeff");

        mint_credentials(&workspace, &mount, &key_path).expect("mint credentials");
        validate_workspace_credentials(&sole(&workspace), &workspace, &mount, &key_path)
            .expect("validate credentials");

        for path in [
            key_path.clone(),
            mount.join(CA_CERTIFICATE_PATH),
            mount.join(WORKSPACE_TOKEN_PATH),
        ] {
            let metadata = fs::symlink_metadata(path).expect("asset metadata");
            assert!(metadata.file_type().is_file());
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        let token = fs::read(mount.join(WORKSPACE_TOKEN_PATH)).expect("token");
        assert_eq!(token.len(), TOKEN_ENCODED_BYTES);
        assert!(!token.contains(&b'='));
        assert_eq!(URL_SAFE_NO_PAD.decode(token).expect("base64url").len(), 32);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn each_mint_rotates_authority_and_rejects_crossed_key_certificate_pairs() {
        let root = test_root("rotation");
        let first_mount = root.join("first");
        let second_mount = root.join("second");
        fs::create_dir(&first_mount).expect("first mount");
        fs::create_dir(&second_mount).expect("second mount");
        let first_key = root.join("first.ca.key");
        let second_key = root.join("second.ca.key");
        let first_workspace = workspace("00112233445566778899aabbccddeeff");
        let second_workspace = workspace("ffeeddccbbaa99887766554433221100");

        mint_credentials(&first_workspace, &first_mount, &first_key).expect("first credentials");
        mint_credentials(&second_workspace, &second_mount, &second_key)
            .expect("second credentials");
        assert_ne!(
            fs::read(&first_key).expect("first key"),
            fs::read(&second_key).expect("second key")
        );
        assert_ne!(
            fs::read(first_mount.join(WORKSPACE_TOKEN_PATH)).expect("first token"),
            fs::read(second_mount.join(WORKSPACE_TOKEN_PATH)).expect("second token")
        );
        assert!(
            validate_workspace_credentials(
                &sole(&first_workspace),
                &first_workspace,
                &first_mount,
                &second_key
            )
            .is_err(),
            "a key from another workspace must not validate"
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn credential_directory_symlink_is_rejected_without_writing_secrets() {
        let root = test_root("symlink");
        let mount = root.join("mount");
        let outside = root.join("outside");
        fs::create_dir(&mount).expect("mount");
        fs::create_dir(&outside).expect("outside");
        symlink(&outside, mount.join(CREDENTIAL_DIRECTORY)).expect("symlink");
        let key_path = root.join("staged.ca.key");

        let error = mint_credentials(
            &workspace("00112233445566778899aabbccddeeff"),
            &mount,
            &key_path,
        )
        .expect_err("symlink must fail");
        assert!(matches!(
            error,
            WorkspaceCredentialError::InvalidAsset {
                kind: "credential directory",
                ..
            }
        ));
        assert!(!key_path.exists());
        assert_eq!(fs::read_dir(outside).expect("outside").count(), 0);

        fs::remove_dir_all(root).expect("cleanup");
    }
    #[test]
    fn gateway_read_returns_exact_redacted_material_after_full_validation() {
        let root = test_root("gateway-read");
        let mount = root.join("mount");
        fs::create_dir(&mount).expect("mount");
        let key_path = root.join("workspace.ca.key");
        let workspace = workspace("00112233445566778899aabbccddeeff");
        mint_credentials(&workspace, &mount, &key_path).expect("mint credentials");

        let expected_token =
            String::from_utf8(fs::read(mount.join(WORKSPACE_TOKEN_PATH)).unwrap()).unwrap();
        let expected_certificate =
            String::from_utf8(fs::read(mount.join(CA_CERTIFICATE_PATH)).unwrap()).unwrap();
        let expected_key = String::from_utf8(fs::read(&key_path).unwrap()).unwrap();
        let credentials =
            read_gateway_workspace_credentials(&sole(&workspace), &workspace, &mount, &key_path)
                .expect("validated gateway credentials");
        assert_eq!(credentials.token(), expected_token);
        assert_eq!(credentials.certificate_pem(), expected_certificate);
        assert_eq!(credentials.private_key_pem(), expected_key);
        let debug = format!("{credentials:?}");
        assert!(!debug.contains(&expected_token));
        assert!(!debug.contains("BEGIN PRIVATE KEY"));
        assert!(debug.contains("[REDACTED]"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn gateway_read_rejects_mode_symlink_pair_and_token_failures() {
        let root = test_root("gateway-read-invalid");
        let subject = workspace("00112233445566778899aabbccddeeff");

        let mode_mount = root.join("mode");
        fs::create_dir(&mode_mount).unwrap();
        let mode_key = root.join("mode.ca.key");
        mint_credentials(&subject, &mode_mount, &mode_key).unwrap();
        fs::set_permissions(&mode_key, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            read_gateway_workspace_credentials(&sole(&subject), &subject, &mode_mount, &mode_key)
                .is_err()
        );

        let symlink_mount = root.join("symlink");
        fs::create_dir(&symlink_mount).unwrap();
        let symlink_key = root.join("symlink.ca.key");
        mint_credentials(&subject, &symlink_mount, &symlink_key).unwrap();
        let token_path = symlink_mount.join(WORKSPACE_TOKEN_PATH);
        let outside_token = root.join("outside-token");
        fs::rename(&token_path, &outside_token).unwrap();
        symlink(&outside_token, &token_path).unwrap();
        assert!(
            read_gateway_workspace_credentials(
                &sole(&subject),
                &subject,
                &symlink_mount,
                &symlink_key
            )
            .is_err()
        );

        let pair_mount = root.join("pair");
        let other_mount = root.join("other");
        fs::create_dir(&pair_mount).unwrap();
        fs::create_dir(&other_mount).unwrap();
        let pair_key = root.join("pair.ca.key");
        let other_key = root.join("other.ca.key");
        mint_credentials(&subject, &pair_mount, &pair_key).unwrap();
        mint_credentials(
            &workspace("ffeeddccbbaa99887766554433221100"),
            &other_mount,
            &other_key,
        )
        .unwrap();
        assert!(
            read_gateway_workspace_credentials(&sole(&subject), &subject, &pair_mount, &other_key)
                .is_err()
        );

        let token_mount = root.join("token");
        fs::create_dir(&token_mount).unwrap();
        let token_key = root.join("token.ca.key");
        mint_credentials(&subject, &token_mount, &token_key).unwrap();
        fs::write(token_mount.join(WORKSPACE_TOKEN_PATH), b"not-a-valid-token").unwrap();
        assert!(
            read_gateway_workspace_credentials(&sole(&subject), &subject, &token_mount, &token_key)
                .is_err()
        );

        fs::remove_dir_all(root).expect("cleanup");
    }
}
