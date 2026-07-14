use crate::repository::RepoId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const METADATA_VERSION: u32 = 1;
pub const PORT_BLOCK_SIZE: u16 = 16;

#[derive(Debug)]
pub enum MetadataError {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    InvalidWorkspaceName(String),
    ReservedSessionName,
    InvalidWorkspaceIncarnation(String),
    UnsupportedVersion {
        kind: &'static str,
        version: u32,
    },
    WorkspaceRoleMismatch {
        workspace: String,
        role: WorkspaceRole,
    },
    ImageFormatMismatch {
        path: PathBuf,
        format: ImageFormat,
        actual_extension: Option<String>,
    },
    UnsupportedImageExtension {
        path: PathBuf,
        actual_extension: Option<String>,
    },
    InvalidPortBlock {
        base: u16,
        size: u16,
    },
    InvalidPath(PathBuf),
    TemporaryFileExhausted(PathBuf),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "metadata I/O failed for {}: {source}", path.display())
            }
            Self::Json { path, source } => {
                write!(f, "invalid metadata JSON in {}: {source}", path.display())
            }
            Self::InvalidWorkspaceName(name) => write!(f, "invalid workspace name {name:?}"),
            Self::ReservedSessionName => {
                f.write_str("workspace name \"main\" is reserved and cannot name a session")
            }
            Self::InvalidWorkspaceIncarnation(value) => {
                write!(
                    f,
                    "invalid workspace incarnation {value:?}: expected 32 lowercase hexadecimal characters"
                )
            }
            Self::UnsupportedVersion { kind, version } => {
                write!(f, "unsupported {kind} metadata version {version}")
            }
            Self::WorkspaceRoleMismatch { workspace, role } => {
                write!(
                    f,
                    "workspace {workspace:?} does not agree with role {role:?}"
                )
            }
            Self::ImageFormatMismatch {
                path,
                format,
                actual_extension,
            } => write!(
                f,
                "image {} has extension {:?}, which does not agree with imageFormat {:?}",
                path.display(),
                actual_extension,
                format
            ),
            Self::UnsupportedImageExtension {
                path,
                actual_extension,
            } => write!(
                f,
                "image {} has unsupported extension {:?}; expected .asif or .sparseimage",
                path.display(),
                actual_extension
            ),
            Self::InvalidPortBlock { base, size } => {
                write!(f, "invalid port block {{ base: {base}, size: {size} }}")
            }
            Self::InvalidPath(path) => {
                write!(f, "metadata path has no file name: {}", path.display())
            }
            Self::TemporaryFileExhausted(path) => {
                write!(
                    f,
                    "could not allocate a temporary file beside {}",
                    path.display()
                )
            }
        }
    }
}

impl Error for MetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn io_error(path: &Path, source: io::Error) -> MetadataError {
    MetadataError::Io {
        path: path.to_owned(),
        source,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageFormat {
    Asif,
    Sparse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AttachTool {
    DiskutilImage,
    Hdiutil,
}

impl ImageFormat {
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Asif => "asif",
            Self::Sparse => "sparseimage",
        }
    }

    pub const fn image_extension(self) -> &'static str {
        match self {
            Self::Asif => ".asif",
            Self::Sparse => ".sparseimage",
        }
    }

    pub const fn attach_tool(self) -> AttachTool {
        match self {
            Self::Asif => AttachTool::DiskutilImage,
            Self::Sparse => AttachTool::Hdiutil,
        }
    }

    pub fn from_image_path(path: &Path) -> Result<Self, MetadataError> {
        match path.extension().and_then(OsStr::to_str) {
            Some("asif") => Ok(Self::Asif),
            Some("sparseimage") => Ok(Self::Sparse),
            extension => Err(MetadataError::UnsupportedImageExtension {
                path: path.to_owned(),
                actual_extension: extension.map(str::to_owned),
            }),
        }
    }

    pub fn validate_path(self, path: &Path) -> Result<(), MetadataError> {
        let actual = path.extension().and_then(OsStr::to_str);
        if actual == Some(self.extension()) {
            Ok(())
        } else {
            Err(MetadataError::ImageFormatMismatch {
                path: path.to_owned(),
                format: self,
                actual_extension: actual.map(str::to_owned),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkspaceName(String);

impl WorkspaceName {
    pub fn new(value: impl Into<String>) -> Result<Self, MetadataError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let valid = (1..=64).contains(&bytes.len())
            && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
            && bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
        if valid {
            Ok(Self(value))
        } else {
            Err(MetadataError::InvalidWorkspaceName(value))
        }
    }

    pub fn session(value: impl Into<String>) -> Result<Self, MetadataError> {
        let name = Self::new(value)?;
        if name.as_str() == "main" {
            Err(MetadataError::ReservedSessionName)
        } else {
            Ok(name)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_main(&self) -> bool {
        self.0 == "main"
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for WorkspaceName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkspaceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkspaceIncarnation(String);

impl WorkspaceIncarnation {
    pub fn new(value: impl Into<String>) -> Result<Self, MetadataError> {
        let value = value.into();
        if value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err(MetadataError::InvalidWorkspaceIncarnation(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkspaceIncarnation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for WorkspaceIncarnation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for WorkspaceIncarnation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceRole {
    Main,
    Workspace,
}

fn validate_role_name(role: WorkspaceRole, name: &WorkspaceName) -> Result<(), MetadataError> {
    if matches!(
        (role, name.is_main()),
        (WorkspaceRole::Main, true) | (WorkspaceRole::Workspace, false)
    ) {
        Ok(())
    } else {
        Err(MetadataError::WorkspaceRoleMismatch {
            workspace: name.to_string(),
            role,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceMarker {
    pub version: u32,
    pub repo_id: RepoId,
    pub project_root: PathBuf,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub role: WorkspaceRole,
    pub image_format: ImageFormat,
    pub base_commit: String,
    pub created_at: String,
    pub forked_from: Option<WorkspaceName>,
    pub created_trace: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceMarkerWire {
    version: u32,
    repo_id: RepoId,
    project_root: PathBuf,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    role: WorkspaceRole,
    image_format: ImageFormat,
    base_commit: String,
    created_at: String,
    forked_from: Option<WorkspaceName>,
    created_trace: String,
}

impl<'de> Deserialize<'de> for WorkspaceMarker {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkspaceMarkerWire::deserialize(deserializer)?;
        let marker = Self {
            version: wire.version,
            repo_id: wire.repo_id,
            project_root: wire.project_root,
            workspace: wire.workspace,
            workspace_incarnation: wire.workspace_incarnation,
            role: wire.role,
            image_format: wire.image_format,
            base_commit: wire.base_commit,
            created_at: wire.created_at,
            forked_from: wire.forked_from,
            created_trace: wire.created_trace,
        };
        marker.validate().map_err(serde::de::Error::custom)?;
        Ok(marker)
    }
}

impl WorkspaceMarker {
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.version != METADATA_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "workspace marker",
                version: self.version,
            });
        }
        validate_role_name(self.role, &self.workspace)?;
        if let Some(source) = &self.forked_from
            && source.is_main()
        {
            return Err(MetadataError::ReservedSessionName);
        }
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, MetadataError> {
        let marker: Self = read_json(path)?;
        marker.validate()?;
        Ok(marker)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Macos,
    Linux,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortBlock {
    pub(crate) base: u16,
    pub(crate) size: u16,
}

impl PortBlock {
    pub fn new(base: u16, size: u16) -> Result<Self, MetadataError> {
        if size == PORT_BLOCK_SIZE && base.checked_add(size - 1).is_some() {
            Ok(Self { base, size })
        } else {
            Err(MetadataError::InvalidPortBlock { base, size })
        }
    }

    pub const fn base(self) -> u16 {
        self.base
    }

    pub const fn size(self) -> u16 {
        self.size
    }

    pub fn validate(self) -> Result<(), MetadataError> {
        Self::new(self.base, self.size).map(|_| ())
    }

    pub fn ports(self) -> Result<RangeInclusive<u16>, MetadataError> {
        self.validate()?;
        Ok(self.base..=self.base + (self.size - 1))
    }
}

impl<'de> Deserialize<'de> for PortBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            base: u16,
            size: u16,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.base, wire.size).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EgressMode {
    #[default]
    Intercept,
    Opaque,
}

fn is_intercept(mode: &EgressMode) -> bool {
    *mode == EgressMode::Intercept
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressRule {
    pub host: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "is_intercept")]
    pub mode: EgressMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonate: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RepoRule(pub String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SimVerb {
    #[serde(rename = "openurl")]
    OpenUrl,
    #[serde(rename = "install")]
    Install,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrantSet {
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_block: Option<PortBlock>,
    #[serde(default)]
    pub read: Vec<PathBuf>,
    #[serde(default)]
    pub write: Vec<PathBuf>,
    #[serde(default)]
    pub egress: Vec<EgressRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<RepoRule>,
    #[serde(default)]
    pub sim: Vec<SimVerb>,
}

impl GrantSet {
    pub fn closed_baseline(port_block: Option<PortBlock>) -> Result<Self, MetadataError> {
        if let Some(block) = port_block {
            block.validate()?;
        }
        Ok(Self {
            port_block,
            ..Self::default()
        })
    }

    pub fn validate(&self, platform: Platform) -> Result<(), MetadataError> {
        match (platform, self.port_block) {
            (Platform::Macos, Some(block)) => block.validate(),
            (Platform::Linux, None) => Ok(()),
            (_, Some(block)) => Err(MetadataError::InvalidPortBlock {
                base: block.base,
                size: block.size,
            }),
            (Platform::Macos, None) => Err(MetadataError::InvalidPortBlock { base: 0, size: 0 }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceInfoSnapshot {
    pub project_root: PathBuf,
    pub role: WorkspaceRole,
    pub base_commit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<WorkspaceName>,
    pub captured_at: String,
    pub stale: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PublicationState {
    Active,
    PendingFence,
}

fn active_publication_state() -> PublicationState {
    PublicationState::Active
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetachedWorkspaceMetadata {
    pub version: u32,
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub image_format: ImageFormat,
    pub platform: Platform,
    pub publication_state: PublicationState,
    pub updated_at: String,
    #[serde(flatten)]
    pub grants: GrantSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_snapshot: Option<WorkspaceInfoSnapshot>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DetachedWorkspaceMetadataWire {
    version: u32,
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    image_format: ImageFormat,
    platform: Platform,
    #[serde(default = "active_publication_state")]
    publication_state: PublicationState,
    updated_at: String,
    revision: u64,
    #[serde(default)]
    port_block: Option<PortBlock>,
    #[serde(default)]
    read: Vec<PathBuf>,
    #[serde(default)]
    write: Vec<PathBuf>,
    #[serde(default)]
    egress: Vec<EgressRule>,
    #[serde(default)]
    repos: Vec<RepoRule>,
    #[serde(default)]
    sim: Vec<SimVerb>,
    #[serde(default)]
    info_snapshot: Option<WorkspaceInfoSnapshot>,
}

impl<'de> Deserialize<'de> for DetachedWorkspaceMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DetachedWorkspaceMetadataWire::deserialize(deserializer)?;
        let metadata = Self {
            version: wire.version,
            repo_id: wire.repo_id,
            workspace: wire.workspace,
            workspace_incarnation: wire.workspace_incarnation,
            image_format: wire.image_format,
            platform: wire.platform,
            publication_state: wire.publication_state,
            updated_at: wire.updated_at,
            grants: GrantSet {
                revision: wire.revision,
                port_block: wire.port_block,
                read: wire.read,
                write: wire.write,
                egress: wire.egress,
                repos: wire.repos,
                sim: wire.sim,
            },
            info_snapshot: wire.info_snapshot,
        };
        metadata
            .validate_deserialized()
            .map_err(serde::de::Error::custom)?;
        Ok(metadata)
    }
}

impl DetachedWorkspaceMetadata {
    fn validate_deserialized(&self) -> Result<(), MetadataError> {
        if self.version != METADATA_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "detached workspace",
                version: self.version,
            });
        }
        self.grants.validate(self.platform)
    }

    pub fn validate(&self, image_path: &Path) -> Result<(), MetadataError> {
        self.validate_deserialized()?;
        self.image_format.validate_path(image_path)?;
        Ok(())
    }

    pub fn read_for_image(image_path: &Path) -> Result<Self, MetadataError> {
        let metadata: Self = read_json(&sidecar_path(image_path))?;
        metadata.validate(image_path)?;
        Ok(metadata)
    }

    pub fn write_for_image(&self, image_path: &Path) -> Result<(), MetadataError> {
        self.validate(image_path)?;
        write_json(&sidecar_path(image_path), self)
    }
}

pub fn sidecar_path(image_path: &Path) -> PathBuf {
    let mut path: OsString = image_path.as_os_str().to_owned();
    path.push(".grants.json");
    PathBuf::from(path)
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, MetadataError> {
    let file = File::open(path).map_err(|source| io_error(path, source))?;
    serde_json::from_reader(BufReader::new(file)).map_err(|source| MetadataError::Json {
        path: path.to_owned(),
        source,
    })
}

pub fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<(), MetadataError> {
    write_json_with_nonce(path, value, atomic_nonce())
}

/// Atomically publishes private bytes with mode `0600`, without following a temporary-file
/// symlink, and fsyncs both the file and its parent directory before returning.
pub fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), MetadataError> {
    write_atomic_with_nonce(path, atomic_nonce(), |writer| {
        writer
            .write_all(bytes)
            .map_err(|source| io_error(path, source))
    })
}

fn atomic_nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn write_json_with_nonce<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
    nonce: u128,
) -> Result<(), MetadataError> {
    write_atomic_with_nonce(path, nonce, |writer| {
        serde_json::to_writer_pretty(&mut *writer, value).map_err(|source| {
            MetadataError::Json {
                path: path.to_owned(),
                source,
            }
        })?;
        writer
            .write_all(b"\n")
            .map_err(|source| io_error(path, source))
    })
}

fn write_atomic_with_nonce(
    path: &Path,
    nonce: u128,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<(), MetadataError>,
) -> Result<(), MetadataError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| MetadataError::InvalidPath(path.to_owned()))?;
    let mut opened = None;
    for attempt in 0..128_u8 {
        let mut temp_name = file_name.to_os_string();
        temp_name.push(format!(".tmp.{}.{nonce}.{attempt}", std::process::id()));
        let temp_path = parent.join(temp_name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        match options.open(&temp_path) {
            Ok(file) => {
                opened = Some((temp_path, file));
                break;
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(io_error(&temp_path, source)),
        }
    }

    let (temp_path, file) =
        opened.ok_or_else(|| MetadataError::TemporaryFileExhausted(path.to_owned()))?;
    let mut cleanup = TempCleanup {
        path: temp_path.clone(),
        armed: true,
    };
    {
        let mut writer = BufWriter::new(file);
        write(&mut writer)?;
        writer
            .flush()
            .map_err(|source| io_error(&temp_path, source))?;
        #[cfg(unix)]
        writer
            .get_ref()
            .set_permissions({
                use std::os::unix::fs::PermissionsExt;
                fs::Permissions::from_mode(0o600)
            })
            .map_err(|source| io_error(&temp_path, source))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|source| io_error(&temp_path, source))?;
    }

    fs::rename(&temp_path, path).map_err(|source| io_error(path, source))?;
    cleanup.armed = false;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error(parent, source))?;
    Ok(())
}

struct TempCleanup {
    path: PathBuf,
    armed: bool,
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    const LEGACY_V1_SIDECAR: &str = r#"{
  "version": 1,
  "repoId": "acme/widget",
  "workspace": "raven",
  "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
  "imageFormat": "asif",
  "platform": "macos",
  "updatedAt": "2026-07-11T12:34:56Z",
  "revision": 7,
  "portBlock": { "base": 40976, "size": 16 },
  "read": ["/project/shared-fixtures"],
  "write": ["/project/artifacts/raven"],
  "egress": [
    { "host": "registry.npmjs.org" },
    { "host": "pinned.example.com", "mode": "opaque" }
  ],
  "sim": ["openurl"],
  "infoSnapshot": {
    "projectRoot": "/project",
    "role": "workspace",
    "baseCommit": "8f31c2d",
    "branch": "raven",
    "createdAt": "2026-07-11T12:00:00Z",
    "capturedAt": "2026-07-11T12:34:00Z",
    "stale": true
  }
}"#;

    fn temp_directory(test: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cowshed-metadata-{test}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn frozen_sidecar_json() -> serde_json::Value {
        json!({
            "version": 1,
            "repoId": "acme/widget",
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "publicationState": "active",
            "imageFormat": "asif",
            "platform": "macos",
            "updatedAt": "2026-07-11T12:34:56Z",
            "revision": 7,
            "portBlock": { "base": 40976, "size": 16 },
            "read": ["/project/shared-fixtures"],
            "write": ["/project/artifacts/raven"],
            "egress": [
                { "host": "registry.npmjs.org" },
                { "host": "pinned.example.com", "mode": "opaque" }
            ],
            "sim": ["openurl"],
            "infoSnapshot": {
                "projectRoot": "/project",
                "role": "workspace",
                "baseCommit": "8f31c2d",
                "branch": "raven",
                "createdAt": "2026-07-11T12:00:00Z",
                "capturedAt": "2026-07-11T12:34:00Z",
                "stale": true
            }
        })
    }

    fn marker_from_json() -> WorkspaceMarker {
        serde_json::from_value(json!({
            "version": 1,
            "repoId": "acme/widget",
            "projectRoot": "/project",
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "imageFormat": "asif",
            "baseCommit": "8f31c2d",
            "createdAt": "2026-07-11T12:00:00Z",
            "forkedFrom": null,
            "createdTrace": "4bf92f"
        }))
        .unwrap()
    }

    #[test]
    fn detached_metadata_round_trip_preserves_frozen_spelling() {
        let expected = frozen_sidecar_json();
        let metadata: DetachedWorkspaceMetadata = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(&metadata).unwrap(), expected);
    }

    #[test]
    fn legacy_v1_sidecar_without_publication_state_reopens_as_active() {
        let directory = temp_directory("legacy-v1-sidecar");
        let image = directory.join("raven.asif");
        fs::write(sidecar_path(&image), LEGACY_V1_SIDECAR).unwrap();

        let metadata = DetachedWorkspaceMetadata::read_for_image(&image).unwrap();
        assert_eq!(metadata.publication_state, PublicationState::Active);

        let mut expected: serde_json::Value = serde_json::from_str(LEGACY_V1_SIDECAR).unwrap();
        expected["publicationState"] = json!("active");
        assert_eq!(serde_json::to_value(metadata).unwrap(), expected);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_malformed_publication_state_is_rejected() {
        let mut malformed = frozen_sidecar_json();
        malformed["publicationState"] = json!("published");

        let error = serde_json::from_value::<DetachedWorkspaceMetadata>(malformed).unwrap_err();
        assert!(
            error.to_string().contains("unknown variant `published`"),
            "{error}"
        );
    }

    #[test]
    fn marker_round_trip_preserves_frozen_spelling() {
        let expected = json!({
            "version": 1,
            "repoId": "acme/widget",
            "projectRoot": "/project",
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "imageFormat": "sparse",
            "baseCommit": "8f31c2d",
            "createdAt": "2026-07-11T12:00:00Z",
            "forkedFrom": null,
            "createdTrace": "4bf92f"
        });
        let marker: WorkspaceMarker = serde_json::from_value(expected.clone()).unwrap();
        marker.validate().unwrap();
        assert_eq!(serde_json::to_value(marker).unwrap(), expected);
    }

    #[test]
    fn marker_deserialization_rejects_inconsistent_invariants_and_unknown_fields() {
        let valid = serde_json::to_value(marker_from_json()).unwrap();
        for (field, value) in [
            ("version", json!(METADATA_VERSION + 1)),
            ("role", json!("main")),
            ("forkedFrom", json!("main")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(serde_json::from_value::<WorkspaceMarker>(invalid).is_err());
        }

        let mut unknown = valid;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<WorkspaceMarker>(unknown).is_err());
    }

    #[test]
    fn detached_deserialization_rejects_inconsistent_invariants_and_unknown_fields() {
        let valid = frozen_sidecar_json();
        for (field, value) in [
            ("version", json!(METADATA_VERSION + 1)),
            ("platform", json!("linux")),
            ("portBlock", json!({ "base": 40976, "size": 15 })),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(serde_json::from_value::<DetachedWorkspaceMetadata>(invalid).is_err());
        }

        let mut macos_without_port = valid.clone();
        macos_without_port
            .as_object_mut()
            .unwrap()
            .remove("portBlock");
        assert!(serde_json::from_value::<DetachedWorkspaceMetadata>(macos_without_port).is_err());

        let mut unknown = valid;
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<DetachedWorkspaceMetadata>(unknown).is_err());
    }

    proptest! {
        #[test]
        fn valid_marker_schemas_round_trip_across_roles(
            main in any::<bool>(),
            sparse in any::<bool>(),
            session_suffix in 0_u16..=u16::MAX,
        ) {
            let (workspace, role) = if main {
                ("main".to_owned(), "main")
            } else {
                (format!("session-{session_suffix}"), "workspace")
            };
            let expected = json!({
                "version": METADATA_VERSION,
                "repoId": "acme/widget",
                "projectRoot": "/project",
                "workspace": workspace,
                "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                "role": role,
                "imageFormat": if sparse { "sparse" } else { "asif" },
                "baseCommit": "8f31c2d",
                "createdAt": "2026-07-11T12:00:00Z",
                "forkedFrom": null,
                "createdTrace": "4bf92f"
            });

            let marker: WorkspaceMarker = serde_json::from_value(expected.clone()).unwrap();
            prop_assert_eq!(serde_json::to_value(marker).unwrap(), expected);
        }

        #[test]
        fn valid_sidecar_schemas_round_trip_across_platforms(
            macos in any::<bool>(),
            sparse in any::<bool>(),
            base in 0_u16..=(u16::MAX - PORT_BLOCK_SIZE + 1),
        ) {
            let mut expected = frozen_sidecar_json();
            expected["imageFormat"] = json!(if sparse { "sparse" } else { "asif" });
            if macos {
                expected["platform"] = json!("macos");
                expected["portBlock"] = json!({ "base": base, "size": PORT_BLOCK_SIZE });
            } else {
                expected["platform"] = json!("linux");
                expected.as_object_mut().unwrap().remove("portBlock");
            }

            let metadata: DetachedWorkspaceMetadata =
                serde_json::from_value(expected.clone()).unwrap();
            prop_assert_eq!(serde_json::to_value(metadata).unwrap(), expected);
        }

        #[test]
        fn public_metadata_deserializers_reject_every_unsupported_version(
            version in any::<u32>().prop_filter("version 1 is supported", |version| {
                *version != METADATA_VERSION
            }),
        ) {
            let mut marker = serde_json::to_value(marker_from_json()).unwrap();
            marker["version"] = json!(version);
            prop_assert!(serde_json::from_value::<WorkspaceMarker>(marker).is_err());

            let mut sidecar = frozen_sidecar_json();
            sidecar["version"] = json!(version);
            prop_assert!(
                serde_json::from_value::<DetachedWorkspaceMetadata>(sidecar).is_err()
            );
        }
    }

    #[test]
    fn formats_and_extensions_must_agree() {
        assert_eq!(ImageFormat::Asif.attach_tool(), AttachTool::DiskutilImage);
        assert_eq!(ImageFormat::Sparse.attach_tool(), AttachTool::Hdiutil);
        ImageFormat::Asif
            .validate_path(Path::new("raven.asif"))
            .unwrap();
        ImageFormat::Sparse
            .validate_path(Path::new("raven.sparseimage"))
            .unwrap();
        assert!(matches!(
            ImageFormat::Asif.validate_path(Path::new("raven.sparseimage")),
            Err(MetadataError::ImageFormatMismatch { .. })
        ));
        assert!(matches!(
            ImageFormat::Sparse.validate_path(Path::new("raven.asif")),
            Err(MetadataError::ImageFormatMismatch { .. })
        ));
        assert!(
            ImageFormat::Asif
                .validate_path(Path::new("raven.img"))
                .is_err()
        );
    }

    #[test]
    fn detached_metadata_rejects_crossed_extension_before_use() {
        let directory = temp_directory("format-mismatch");
        let image = directory.join("raven.sparseimage");
        write_json(&sidecar_path(&image), &frozen_sidecar_json()).unwrap();
        assert!(matches!(
            DetachedWorkspaceMetadata::read_for_image(&image),
            Err(MetadataError::ImageFormatMismatch {
                format: ImageFormat::Asif,
                ..
            })
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn workspace_names_enforce_grammar_and_reserve_main_for_sessions() {
        assert!(WorkspaceName::new("a").is_ok());
        assert!(WorkspaceName::new(format!("a{}", "-".repeat(63))).is_ok());
        for invalid in ["", "A", "-raven", "raven_2", "raven/2"] {
            assert!(WorkspaceName::new(invalid).is_err(), "accepted {invalid:?}");
        }
        assert!(WorkspaceName::new(format!("a{}", "b".repeat(64))).is_err());
        assert!(matches!(
            WorkspaceName::session("main"),
            Err(MetadataError::ReservedSessionName)
        ));
        assert!(WorkspaceName::session("raven").is_ok());
    }

    #[test]
    fn atomic_write_round_trips_with_no_temp_residue() {
        let directory = temp_directory("atomic");
        let path = directory.join("metadata.json");
        let value = json!({ "repoId": "acme/widget", "revision": 7 });
        write_json(&path, &value).unwrap();
        let actual: serde_json::Value = read_json(&path).unwrap();
        assert_eq!(actual, value);
        let entries: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![OsString::from("metadata.json")]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_serialization_removes_temp_file() {
        struct Fails;
        impl Serialize for Fails {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                Err(serde::ser::Error::custom("intentional"))
            }
        }

        let directory = temp_directory("cleanup");
        let path = directory.join("metadata.json");
        assert!(write_json(&path, &Fails).is_err());
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn metadata_errors_expose_stable_messages_and_causes() {
        let io_error = MetadataError::Io {
            path: PathBuf::from("/metadata.json"),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "disk unavailable"),
        };
        assert_eq!(
            io_error.to_string(),
            "metadata I/O failed for /metadata.json: disk unavailable"
        );
        assert_eq!(
            io_error
                .source()
                .expect("I/O errors retain their cause")
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let json_error = MetadataError::Json {
            path: PathBuf::from("/metadata.json"),
            source,
        };
        assert!(
            json_error
                .to_string()
                .starts_with("invalid metadata JSON in /metadata.json: EOF while parsing")
        );
        assert!(
            json_error
                .source()
                .expect("JSON errors retain their cause")
                .downcast_ref::<serde_json::Error>()
                .is_some()
        );

        let validation_error = WorkspaceName::new("Invalid").unwrap_err();
        assert_eq!(
            validation_error.to_string(),
            "invalid workspace name \"Invalid\""
        );
        assert!(validation_error.source().is_none());
    }

    #[test]
    fn workspace_name_and_incarnation_accessors_preserve_values() {
        let main = WorkspaceName::new("main").unwrap();
        let session = WorkspaceName::new("raven-2").unwrap();
        assert!(main.is_main());
        assert!(!session.is_main());
        assert_eq!(main.as_str(), "main");
        assert_eq!(session.to_string(), "raven-2");

        let value = "0198f2c0b7e34dc795f17b238b331c80";
        let incarnation = WorkspaceIncarnation::new(value).unwrap();
        assert_eq!(incarnation.as_str(), value);
        assert_eq!(incarnation.to_string(), value);
    }

    #[test]
    fn role_and_marker_validation_reject_every_inconsistent_state() {
        let main = WorkspaceName::new("main").unwrap();
        let session = WorkspaceName::new("raven").unwrap();
        validate_role_name(WorkspaceRole::Main, &main).unwrap();
        validate_role_name(WorkspaceRole::Workspace, &session).unwrap();
        assert!(matches!(
            validate_role_name(WorkspaceRole::Main, &session),
            Err(MetadataError::WorkspaceRoleMismatch { workspace, role })
                if workspace == "raven" && role == WorkspaceRole::Main
        ));
        assert!(matches!(
            validate_role_name(WorkspaceRole::Workspace, &main),
            Err(MetadataError::WorkspaceRoleMismatch { workspace, role })
                if workspace == "main" && role == WorkspaceRole::Workspace
        ));

        let mut marker = marker_from_json();
        marker.validate().unwrap();
        marker.version = METADATA_VERSION + 1;
        assert!(matches!(
            marker.validate(),
            Err(MetadataError::UnsupportedVersion {
                kind: "workspace marker",
                version: 2
            })
        ));
        marker.version = METADATA_VERSION;
        marker.role = WorkspaceRole::Main;
        assert!(matches!(
            marker.validate(),
            Err(MetadataError::WorkspaceRoleMismatch { .. })
        ));
        marker.role = WorkspaceRole::Workspace;
        marker.forked_from = Some(WorkspaceName::new("main").unwrap());
        assert!(matches!(
            marker.validate(),
            Err(MetadataError::ReservedSessionName)
        ));
    }

    #[test]
    fn port_blocks_and_platform_grants_enforce_boundaries() {
        let lowest = PortBlock::new(0, PORT_BLOCK_SIZE).unwrap();
        let highest = PortBlock::new(u16::MAX - PORT_BLOCK_SIZE + 1, PORT_BLOCK_SIZE).unwrap();
        assert_eq!(lowest.ports().unwrap(), 0..=15);
        assert_eq!(highest.ports().unwrap(), (u16::MAX - 15)..=u16::MAX);
        for invalid in [
            PortBlock { base: 80, size: 0 },
            PortBlock {
                base: 80,
                size: PORT_BLOCK_SIZE - 1,
            },
            PortBlock {
                base: u16::MAX - 14,
                size: PORT_BLOCK_SIZE,
            },
        ] {
            assert!(matches!(
                invalid.validate(),
                Err(MetadataError::InvalidPortBlock { base, size })
                    if base == invalid.base && size == invalid.size
            ));
        }

        let macos = GrantSet::closed_baseline(Some(lowest)).unwrap();
        assert_eq!(macos.port_block, Some(lowest));
        assert_eq!(macos.revision, 0);
        assert!(macos.read.is_empty() && macos.write.is_empty() && macos.egress.is_empty());
        macos.validate(Platform::Macos).unwrap();
        assert!(
            GrantSet::closed_baseline(Some(PortBlock {
                base: u16::MAX,
                size: PORT_BLOCK_SIZE,
            }))
            .is_err()
        );

        let linux = GrantSet::closed_baseline(None).unwrap();
        linux.validate(Platform::Linux).unwrap();
        assert!(matches!(
            linux.validate(Platform::Macos),
            Err(MetadataError::InvalidPortBlock { base: 0, size: 0 })
        ));
        assert!(matches!(
            macos.validate(Platform::Linux),
            Err(MetadataError::InvalidPortBlock {
                base: 0,
                size: PORT_BLOCK_SIZE
            })
        ));
    }

    #[test]
    fn detached_sidecar_write_persists_and_validates_before_creation() {
        let directory = temp_directory("sidecar-write");
        let image = directory.join("raven.asif");
        let metadata: DetachedWorkspaceMetadata =
            serde_json::from_value(frozen_sidecar_json()).unwrap();
        metadata.write_for_image(&image).unwrap();
        let sidecar = sidecar_path(&image);
        assert!(sidecar.is_file());
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&image).unwrap(),
            metadata
        );

        let wrong_image = directory.join("wrong.sparseimage");
        assert!(matches!(
            metadata.write_for_image(&wrong_image),
            Err(MetadataError::ImageFormatMismatch { .. })
        ));
        assert!(!sidecar_path(&wrong_image).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn atomic_write_retries_only_name_collisions() {
        let directory = temp_directory("atomic-collision");
        let path = directory.join("metadata.json");
        let nonce = 42;
        let collision = directory.join(format!(
            "metadata.json.tmp.{}.{nonce}.0",
            std::process::id()
        ));
        fs::write(&collision, "do not replace").unwrap();

        let value = json!({ "revision": 9 });
        write_json_with_nonce(&path, &value, nonce).unwrap();
        assert_eq!(fs::read_to_string(&collision).unwrap(), "do not replace");
        assert_eq!(read_json::<serde_json::Value>(&path).unwrap(), value);
        assert!(
            !directory
                .join(format!(
                    "metadata.json.tmp.{}.{nonce}.1",
                    std::process::id()
                ))
                .exists()
        );

        let missing_parent_path = directory.join("missing").join("metadata.json");
        let error = write_json_with_nonce(&missing_parent_path, &value, nonce).unwrap_err();
        assert!(matches!(
            error,
            MetadataError::Io { path: error_path, source }
                if error_path == directory.join("missing").join(format!(
                    "metadata.json.tmp.{}.{nonce}.0",
                    std::process::id()
                )) && source.kind() == io::ErrorKind::NotFound
        ));
        fs::remove_dir_all(directory).unwrap();
    }
}
