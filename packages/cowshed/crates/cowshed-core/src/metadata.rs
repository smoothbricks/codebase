use crate::repository::RepoId;
use cowshed_gateway_types::{is_macos_port_block, is_port_block};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

pub const MARKER_VERSION: u32 = 1;
pub const SIDECAR_VERSION: u32 = 1;
pub const CHECKOUT_ROOT_VERSION: u32 = 1;
pub const SLOT_BINDINGS_VERSION: u32 = 1;
/// The macOS port range and the size new blocks get are the gateway's grammar; core names the
/// same items rather than restating them.
pub use cowshed_gateway_types::{MACOS_PORT_MAX, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE};

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
    InvalidLineage,
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
    InvalidPath {
        path: PathBuf,
        reason: &'static str,
    },
    SlotOutOfRange(u32),
    SlotAlreadyBound {
        slot: u32,
        workspace: String,
    },
    WorkspaceAlreadySlotted {
        workspace: String,
        slot: u32,
    },
    MainIsNotSlottable,
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
            Self::InvalidLineage => f.write_str(
                "workspace marker lineage must list each ancestor incarnation once and never the marker's own",
            ),
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
            Self::InvalidPath { path, reason } => {
                write!(f, "invalid metadata path {}: {reason}", path.display())
            }
            Self::SlotOutOfRange(slot) => {
                write!(f, "slot {slot} is outside 0..={}", SlotId::MAX)
            }
            Self::SlotAlreadyBound { slot, workspace } => {
                write!(f, "slot {slot} is already bound to workspace {workspace:?}")
            }
            Self::WorkspaceAlreadySlotted { workspace, slot } => {
                write!(f, "workspace {workspace:?} is already bound to slot {slot}")
            }
            Self::MainIsNotSlottable => f.write_str(
                "main cannot take a build slot: it mounts at the project's checkout",
            ),
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

/// A disk-image capacity, held as an exact byte count.
///
/// Capacities are spelled `100g`, `200g`, `1t` on the command line and the units are binary:
/// that is what `hdiutil` has always meant by them, and therefore what every image cowshed has
/// ever created is sized in. `diskutil`'s image verbs read the same letters as decimal SI, so
/// cowshed hands neither tool a unit — it resolves the letters here once and passes the byte
/// count, the one spelling both tools agree on and the only one a resize can be verified
/// against afterwards.
///
/// The smallest unit accepted from a caller is a mebibyte, which keeps every requested capacity
/// a whole number of the 4 KiB blocks both resize tools round to. A request that had to be
/// rounded could not be checked against what the image reports back.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ImageCapacity(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageCapacityError {
    Malformed(String),
    TooSmall(String),
    Unaligned(String),
}

impl fmt::Display for ImageCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(value) => write!(
                formatter,
                "capacity `{value}` is not a positive whole number with an optional m, g, or t unit"
            ),
            Self::TooSmall(value) => write!(
                formatter,
                "capacity `{value}` is below the one mebibyte minimum image capacity"
            ),
            Self::Unaligned(value) => write!(
                formatter,
                "capacity `{value}` is not a multiple of the 4 KiB block the image tools resize in"
            ),
        }
    }
}

impl Error for ImageCapacityError {}

impl ImageCapacity {
    pub const KIBIBYTE: u64 = 1024;
    pub const MEBIBYTE: u64 = 1024 * Self::KIBIBYTE;
    pub const GIBIBYTE: u64 = 1024 * Self::MEBIBYTE;
    pub const TEBIBYTE: u64 = 1024 * Self::GIBIBYTE;
    /// The block size both `hdiutil resize` and `diskutil image resize` round a request to.
    const BLOCK: u64 = 4 * Self::KIBIBYTE;

    pub const fn from_gibibytes(count: u64) -> Self {
        Self(count * Self::GIBIBYTE)
    }

    /// A capacity observed from a tool, which reports whatever the image actually holds.
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> u64 {
        self.0
    }

    pub fn parse(value: &str) -> Result<Self, ImageCapacityError> {
        let text = value.trim();
        let malformed = || ImageCapacityError::Malformed(value.to_owned());
        let (digits, unit) = match text.as_bytes().last() {
            Some(byte) if byte.is_ascii_digit() => (text, 1),
            Some(b'm' | b'M') => (&text[..text.len() - 1], Self::MEBIBYTE),
            Some(b'g' | b'G') => (&text[..text.len() - 1], Self::GIBIBYTE),
            Some(b't' | b'T') => (&text[..text.len() - 1], Self::TEBIBYTE),
            _ => return Err(malformed()),
        };
        let count: u64 = digits.parse().map_err(|_| malformed())?;
        let bytes = count.checked_mul(unit).ok_or_else(malformed)?;
        if bytes < Self::MEBIBYTE {
            return Err(ImageCapacityError::TooSmall(value.to_owned()));
        }
        if !bytes.is_multiple_of(Self::BLOCK) {
            return Err(ImageCapacityError::Unaligned(value.to_owned()));
        }
        Ok(Self(bytes))
    }
}

/// Renders the capacity in the largest binary unit that divides it exactly, so a capacity parsed
/// from `100g` prints as `100g`. A capacity observed from a tool that lands on no whole unit
/// prints as its byte count, which is what the tools themselves were given.
impl fmt::Display for ImageCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (unit, suffix) in [
            (Self::TEBIBYTE, "t"),
            (Self::GIBIBYTE, "g"),
            (Self::MEBIBYTE, "m"),
        ] {
            if self.0 >= unit && self.0.is_multiple_of(unit) {
                return write!(formatter, "{}{suffix}", self.0 / unit);
            }
        }
        write!(formatter, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkspaceName(String);

impl WorkspaceName {
    /// User-facing grammar `WorkspaceName::new` enforces: 1..=64 of `[a-z0-9][a-z0-9-]*`.
    pub const USAGE: &'static str = "workspace names must match [a-z0-9][a-z0-9-]{0,63}";

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

    /// The fixed name of the always-mounted main workspace.
    pub fn main() -> Self {
        Self("main".to_owned())
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

/// The project-level record of where the checkout is, written when main is removed.
///
/// While main has an image, its sidecar names the checkout. Removing main takes that image to the
/// trash and then to reclamation, and a project still bound afterwards — a restore that died
/// before it unbound — has no other record connecting its checkout to its identity. A checkout
/// with no remote cannot supply the identity itself, so this record is what reopens it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckoutRootRecord {
    version: u32,
    checkout_root: PathBuf,
}

impl CheckoutRootRecord {
    pub fn new(checkout_root: &Path) -> Result<Self, MetadataError> {
        let record = Self {
            version: CHECKOUT_ROOT_VERSION,
            checkout_root: checkout_root.to_owned(),
        };
        record.validate()?;
        Ok(record)
    }

    pub fn checkout_root(&self) -> &Path {
        &self.checkout_root
    }

    fn validate(&self) -> Result<(), MetadataError> {
        if self.version != CHECKOUT_ROOT_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "checkout root record",
                version: self.version,
            });
        }
        if !self.checkout_root.is_absolute() {
            return Err(MetadataError::InvalidPath {
                path: self.checkout_root.clone(),
                reason: "path is not absolute",
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CheckoutRootRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            version: u32,
            checkout_root: PathBuf,
        }

        let wire = Wire::deserialize(deserializer)?;
        let record = Self {
            version: wire.version,
            checkout_root: wire.checkout_root,
        };
        record.validate().map_err(serde::de::Error::custom)?;
        Ok(record)
    }
}

/// A per-project build slot: one stable mount path, occupied by one workspace at a time.
///
/// Slots predate path-independent cache sharing and are no longer what makes sccache hit.
/// Cargo computes `-C metadata` / `-C extra-filename` path-independently for workspace members
/// (measured on cargo 1.97: identical hashes for one workspace checked out at two paths), and
/// the bundled sccache keys its residual path-bearing inputs — cwd, blanket `CARGO_*`
/// environment values, argument bytes — relative to the request cwd when the client sets
/// `SCCACHE_BASEDIR_CWD=1`, which every workspace now does. Cross-path sharing is therefore the
/// default, with one deliberate exception: values rustc records as `# env-dep:` (a crate that
/// compiles `env!("CARGO_MANIFEST_DIR")` into its output) are never normalized, so such crates
/// fail closed across paths. A slot's remaining value is the stable absolute path itself, for
/// tooling that persists paths across tenant generations.
///
/// Slot numbering is shared with `coordinator.assignSlot`'s port blocks, hence the same upper
/// bound: a slot has to be expressible as a `NEW_PORT_BLOCK_SIZE`-aligned base inside the 16-bit
/// port space.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SlotId(u32);

impl SlotId {
    pub const MAX: u32 = (u16::MAX / NEW_PORT_BLOCK_SIZE) as u32;

    /// The mount directory leaf for this slot. `@` is outside the `WorkspaceName` grammar, so a
    /// slot mountpoint can never collide with a name-derived sibling under the same mount root.
    const MOUNT_PREFIX: &'static str = "slot@";

    pub fn new(value: u32) -> Result<Self, MetadataError> {
        if value > Self::MAX {
            return Err(MetadataError::SlotOutOfRange(value));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn mount_name(self) -> String {
        format!("{}{}", Self::MOUNT_PREFIX, self.0)
    }

    /// The slot a path is the mountpoint of, if any. This is how "entered through a slot path" is
    /// decided everywhere: the path is the identity, so no caller needs to carry a flag.
    pub fn from_mount_path(path: &Path) -> Option<Self> {
        let digits = path
            .file_name()?
            .to_str()?
            .strip_prefix(Self::MOUNT_PREFIX)?;
        Self::new(digits.parse().ok()?).ok()
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlotBinding {
    pub slot: SlotId,
    pub workspace: WorkspaceName,
}

/// The project-level record of which workspace occupies which slot.
///
/// A list rather than a map: the durable form is order-stable, the validation that no slot and no
/// workspace appears twice is explicit, and JSON map keys stay out of it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlotBindingsRecord {
    pub version: u32,
    pub bindings: Vec<SlotBinding>,
}

impl SlotBindingsRecord {
    pub fn new(bindings: &SlotBindings) -> Self {
        Self {
            version: SLOT_BINDINGS_VERSION,
            bindings: bindings.entries.clone(),
        }
    }

    pub fn into_bindings(self) -> Result<SlotBindings, MetadataError> {
        if self.version != SLOT_BINDINGS_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "slot bindings record",
                version: self.version,
            });
        }
        let mut bindings = SlotBindings::default();
        for entry in self.bindings {
            bindings.bind(entry.slot, entry.workspace)?;
        }
        Ok(bindings)
    }
}

impl<'de> Deserialize<'de> for SlotBindingsRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            version: u32,
            bindings: Vec<SlotBinding>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let record = Self {
            version: wire.version,
            bindings: wire.bindings,
        };
        record
            .clone()
            .into_bindings()
            .map_err(serde::de::Error::custom)?;
        Ok(record)
    }
}

/// Every slot occupancy for one project, kept sorted by slot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SlotBindings {
    entries: Vec<SlotBinding>,
}

impl SlotBindings {
    pub fn slot_of(&self, workspace: &WorkspaceName) -> Option<SlotId> {
        self.entries
            .iter()
            .find(|entry| &entry.workspace == workspace)
            .map(|entry| entry.slot)
    }

    pub fn tenant(&self, slot: SlotId) -> Option<&WorkspaceName> {
        self.entries
            .iter()
            .find(|entry| entry.slot == slot)
            .map(|entry| &entry.workspace)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SlotBinding> {
        self.entries.iter()
    }

    /// Occupy a slot. Both directions are exclusive: a slot holds one workspace, and a workspace
    /// mounts at one path. Re-binding the pair it already holds is accepted so create and repair
    /// are idempotent.
    pub fn bind(&mut self, slot: SlotId, workspace: WorkspaceName) -> Result<(), MetadataError> {
        if workspace.is_main() {
            return Err(MetadataError::MainIsNotSlottable);
        }
        if let Some(occupant) = self.tenant(slot) {
            if occupant == &workspace {
                return Ok(());
            }
            return Err(MetadataError::SlotAlreadyBound {
                slot: slot.get(),
                workspace: occupant.to_string(),
            });
        }
        if let Some(existing) = self.slot_of(&workspace) {
            return Err(MetadataError::WorkspaceAlreadySlotted {
                workspace: workspace.to_string(),
                slot: existing.get(),
            });
        }
        let entry = SlotBinding { slot, workspace };
        let at = self
            .entries
            .partition_point(|existing| existing.slot < entry.slot);
        self.entries.insert(at, entry);
        Ok(())
    }

    /// Vacate whatever slot this workspace held, reporting it.
    pub fn release(&mut self, workspace: &WorkspaceName) -> Option<SlotId> {
        let at = self
            .entries
            .iter()
            .position(|entry| &entry.workspace == workspace)?;
        Some(self.entries.remove(at).slot)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceRole {
    Main,
    Workspace,
}

impl WorkspaceRole {
    /// Role is a function of the name: `main` is the main workspace, everything else a session.
    /// [`validate_role_name`] enforces the same invariant on deserialized metadata.
    pub fn for_name(name: &WorkspaceName) -> Self {
        if name.is_main() {
            Self::Main
        } else {
            Self::Workspace
        }
    }
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
    /// The ancestor incarnations this image was cloned from, nearest first: a fork or restore
    /// copies the source image — including the job records its ancestors wrote — so the
    /// lineage is what authorizes those records when the supervisor opens the workspace. It
    /// lives in the image because it is a property of the image: the controller writes it when
    /// it mints the incarnation, and a host log has nothing to add. `None` marks a marker written
    /// before lineage was recorded; the controller heals it once from the records the image
    /// already carries and rewrites the marker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Vec<WorkspaceIncarnation>>,
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
    #[serde(default)]
    lineage: Option<Vec<WorkspaceIncarnation>>,
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
            lineage: wire.lineage,
        };
        marker.validate().map_err(serde::de::Error::custom)?;
        Ok(marker)
    }
}

impl WorkspaceMarker {
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.version != MARKER_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "workspace marker",
                version: self.version,
            });
        }
        validate_role_name(self.role, &self.workspace)?;
        if let Some(lineage) = &self.lineage {
            let mut seen = std::collections::BTreeSet::new();
            for ancestor in lineage {
                if ancestor == &self.workspace_incarnation || !seen.insert(ancestor) {
                    return Err(MetadataError::InvalidLineage);
                }
            }
        }
        Ok(())
    }

    /// The lineage a clone of this image inherits: this incarnation, then this image's own
    /// ancestors. A marker without recorded lineage contributes only itself.
    pub fn clone_lineage(&self) -> Vec<WorkspaceIncarnation> {
        let mut lineage = Vec::with_capacity(1 + self.lineage.as_ref().map_or(0, Vec::len));
        lineage.push(self.workspace_incarnation.clone());
        if let Some(ancestors) = &self.lineage {
            lineage.extend(ancestors.iter().cloned());
        }
        lineage
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
    /// A block is valid against its own size: a power of two of at least two ports, with `base`
    /// aligned to it. Live blocks keep the size they were allocated with.
    pub fn new(base: u16, size: u16) -> Result<Self, MetadataError> {
        if is_port_block(base, size) {
            Ok(Self { base, size })
        } else {
            Err(MetadataError::InvalidPortBlock { base, size })
        }
    }

    /// Every new-size block of the macOS range, lowest first.
    pub fn macos_candidates() -> impl Iterator<Item = Self> {
        (MACOS_PORT_MIN..=MACOS_PORT_MAX - (NEW_PORT_BLOCK_SIZE - 1))
            .step_by(usize::from(NEW_PORT_BLOCK_SIZE))
            .map(|base| Self {
                base,
                size: NEW_PORT_BLOCK_SIZE,
            })
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
        Ok(self.base..=self.last())
    }

    const fn last(self) -> u16 {
        self.base + (self.size - 1)
    }

    /// Whether the two blocks share a port.
    pub const fn overlaps(self, other: Self) -> bool {
        self.base <= other.last() && other.base <= self.last()
    }
}

impl fmt::Display for PortBlock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.base, self.last())
    }
}

/// The port blocks live workspaces hold, each at the size it was allocated with. A block that
/// shares a port with a held one is refused, so the set is disjoint by construction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReservedPortBlocks(BTreeMap<u16, PortBlock>);

impl ReservedPortBlocks {
    /// The held block sharing a port with `block`. Held blocks are disjoint, so the last one
    /// starting at or before `block`'s last port is the only one that can reach into it.
    pub fn overlapping(&self, block: PortBlock) -> Option<PortBlock> {
        self.0
            .range(..=block.last())
            .next_back()
            .map(|(_, held)| *held)
            .filter(|held| held.overlaps(block))
    }

    /// Holds `block`, or names the held block it would share a port with.
    pub fn insert(&mut self, block: PortBlock) -> Result<(), PortBlock> {
        match self.overlapping(block) {
            Some(held) => Err(held),
            None => {
                self.0.insert(block.base, block);
                Ok(())
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn blocks(&self) -> impl Iterator<Item = PortBlock> + '_ {
        self.0.values().copied()
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

pub const DEFAULT_EGRESS_PORTS: [u16; 2] = [443, 80];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressRule {
    pub host: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<u16>,
    #[serde(default, skip_serializing_if = "is_intercept")]
    pub mode: EgressMode,
}

impl EgressRule {
    pub fn effective_ports(&self) -> &[u16] {
        if self.ports.is_empty() {
            &DEFAULT_EGRESS_PORTS
        } else {
            &self.ports
        }
    }
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
            (Platform::Macos, Some(block)) if is_macos_port_block(block.base, block.size) => Ok(()),
            (Platform::Macos, Some(block)) => Err(MetadataError::InvalidPortBlock {
                base: block.base,
                size: block.size,
            }),
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
    /// This workspace is a registered linked worktree of main's repository rather than a
    /// standalone clone (`cowshed new --git-worktree`).
    ///
    /// Store-side, because every decision it drives — refusing checkpoint, pruning the
    /// registration out of main at retirement, requiring main mounted before attach — has to be
    /// made while the workspace itself is detached and its mount says nothing.
    ///
    /// Absent rather than `false` on a standalone workspace: the mode is the exception, and a
    /// sidecar that never asked for it keeps the spelling it already had.
    #[serde(default, skip_serializing_if = "is_false")]
    pub git_worktree: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PublicationState {
    Active,
    PendingFence,
}

const fn is_false(value: &bool) -> bool {
    !*value
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
    pub info_snapshot: WorkspaceInfoSnapshot,
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
    publication_state: PublicationState,
    updated_at: String,
    #[serde(flatten)]
    grants: GrantSet,
    info_snapshot: WorkspaceInfoSnapshot,
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
            grants: wire.grants,
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
        if self.version != SIDECAR_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                kind: "detached workspace",
                version: self.version,
            });
        }
        self.grants.validate(self.platform)?;
        let info = &self.info_snapshot;
        if !info.project_root.is_absolute() {
            return Err(MetadataError::InvalidPath {
                path: info.project_root.clone(),
                reason: "path is not absolute",
            });
        }
        let expected_role = WorkspaceRole::for_name(&self.workspace);
        if info.role != expected_role {
            return Err(MetadataError::WorkspaceRoleMismatch {
                workspace: self.workspace.to_string(),
                role: info.role,
            });
        }
        Ok(())
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

/// The one spelling of the grants-sidecar suffix; every recognizer and both path derivations
/// below go through it.
pub const GRANTS_SIDECAR_SUFFIX: &str = ".grants.json";

/// Append a suffix to a path's final component without touching its extension handling.
pub(crate) fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

pub fn sidecar_path(image_path: &Path) -> PathBuf {
    append_suffix(image_path, GRANTS_SIDECAR_SUFFIX)
}

/// The image a sidecar belongs to, or `None` for a path that is not a sidecar. Exact inverse of
/// [`sidecar_path`], so a walk of the store can recognise sidecars without re-deriving image names.
pub fn image_from_sidecar_path(sidecar: &Path) -> Option<PathBuf> {
    let name = sidecar.file_name()?.to_str()?;
    let stem = name.strip_suffix(GRANTS_SIDECAR_SUFFIX)?;
    if stem.is_empty() {
        return None;
    }
    Some(sidecar.with_file_name(stem))
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, MetadataError> {
    let file = File::open(path).map_err(|source| io_error(path, source))?;
    serde_json::from_reader(BufReader::new(file)).map_err(|source| MetadataError::Json {
        path: path.to_owned(),
        source,
    })
}

pub fn write_json<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<(), MetadataError> {
    publish(path, |writer| {
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

/// Atomically publishes private bytes with mode `0600` and fsyncs both the file and its parent
/// directory before returning.
pub fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), MetadataError> {
    publish(path, |writer| {
        writer
            .write_all(bytes)
            .map_err(|source| io_error(path, source))
    })
}

fn publish(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<(), MetadataError>,
) -> Result<(), MetadataError> {
    crate::fsio::publish_private_file(path, write).map_err(|error| match error {
        crate::fsio::PublishError::Io { path, source } => io_error(&path, source),
        crate::fsio::PublishError::Write(error) => error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    /// This tool writes every sidecar whole, so one missing its publication state or its workspace
    /// facts is damage, never an older writer's spelling to fill in.
    #[test]
    fn a_sidecar_missing_its_publication_state_or_info_snapshot_is_refused() {
        for field in ["publicationState", "infoSnapshot"] {
            let mut partial = frozen_sidecar_json();
            partial
                .as_object_mut()
                .expect("metadata object")
                .remove(field);
            assert!(
                serde_json::from_value::<DetachedWorkspaceMetadata>(partial).is_err(),
                "a sidecar without {field} decoded"
            );
        }
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
    fn marker_lineage_round_trips_and_rejects_self_and_duplicates() {
        let ancestor = "1198f2c0b7e34dc795f17b238b331c80";
        let older = "2198f2c0b7e34dc795f17b238b331c80";
        let mut with_lineage = serde_json::to_value(marker_from_json()).unwrap();
        with_lineage["lineage"] = json!([ancestor, older]);
        let marker: WorkspaceMarker = serde_json::from_value(with_lineage.clone()).unwrap();
        assert_eq!(serde_json::to_value(&marker).unwrap(), with_lineage);
        assert_eq!(
            marker
                .clone_lineage()
                .iter()
                .map(WorkspaceIncarnation::as_str)
                .collect::<Vec<_>>(),
            vec!["0198f2c0b7e34dc795f17b238b331c80", ancestor, older],
            "a clone inherits this incarnation first, then its ancestors"
        );
        assert!(
            marker_from_json().clone_lineage().len() == 1,
            "a legacy marker contributes only itself"
        );

        let mut self_in_lineage = serde_json::to_value(marker_from_json()).unwrap();
        self_in_lineage["lineage"] = json!(["0198f2c0b7e34dc795f17b238b331c80"]);
        assert!(serde_json::from_value::<WorkspaceMarker>(self_in_lineage).is_err());
        let mut duplicated = serde_json::to_value(marker_from_json()).unwrap();
        duplicated["lineage"] = json!([ancestor, ancestor]);
        assert!(serde_json::from_value::<WorkspaceMarker>(duplicated).is_err());
    }

    #[test]
    fn pending_fork_from_main_marker_reopens() {
        let mut pending_fork = serde_json::to_value(marker_from_json()).unwrap();
        pending_fork["forkedFrom"] = json!("main");

        let marker: WorkspaceMarker = serde_json::from_value(pending_fork).unwrap();
        assert_eq!(marker.forked_from, Some(WorkspaceName::main()));
    }

    #[test]
    fn marker_deserialization_rejects_inconsistent_invariants_and_unknown_fields() {
        let valid = serde_json::to_value(marker_from_json()).unwrap();
        for (field, value) in [
            ("version", json!(MARKER_VERSION + 1)),
            ("role", json!("main")),
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
            ("version", json!(SIDECAR_VERSION + 1)),
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
                "version": 1,
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

        /// A macOS sidecar's `portBlock` is drawn as a size and a block index rather than raw
        /// `u16`s, because the durable grammar is "a power-of-two block aligned to its own size
        /// inside 40960-49151" — a block keeps the size it was allocated with, so every size the
        /// grammar admits round-trips, not only the one new workspaces get.
        #[test]
        fn valid_sidecar_schemas_round_trip_across_platforms(
            macos in any::<bool>(),
            sparse in any::<bool>(),
            size_log2 in 1_u32..=13,
            block in any::<u16>(),
        ) {
            let mut expected = frozen_sidecar_json();
            expected["imageFormat"] = json!(if sparse { "sparse" } else { "asif" });
            if macos {
                let size = 1_u16 << size_log2;
                let blocks = (MACOS_PORT_MAX - MACOS_PORT_MIN + 1) / size;
                let base = MACOS_PORT_MIN + (block % blocks) * size;
                expected["platform"] = json!("macos");
                expected["portBlock"] = json!({ "base": base, "size": size });
            } else {
                expected["platform"] = json!("linux");
                expected.as_object_mut().unwrap().remove("portBlock");
            }

            let metadata: DetachedWorkspaceMetadata =
                serde_json::from_value(expected.clone()).unwrap();
            prop_assert_eq!(serde_json::to_value(metadata).unwrap(), expected);
        }

        /// Every block off the grammar — a size that is not a power of two, a base not aligned to
        /// its own size, a block reaching outside the range — is refused at the parse edge, so
        /// no reader downstream has to re-derive it. This is the pair to the round-trip above:
        /// together they say the grammar is exactly the aligned blocks and nothing else.
        #[test]
        fn macos_sidecars_reject_every_block_off_the_grammar(
            (base, size) in (
                any::<u16>(),
                prop_oneof![Just(16_u16), Just(NEW_PORT_BLOCK_SIZE), any::<u16>()],
            )
                .prop_filter("blocks on the grammar are valid", |(base, size)| {
                    !cowshed_gateway_types::is_macos_port_block(*base, *size)
                }),
        ) {
            let mut sidecar = frozen_sidecar_json();
            sidecar["platform"] = json!("macos");
            sidecar["portBlock"] = json!({ "base": base, "size": size });
            prop_assert!(
                serde_json::from_value::<DetachedWorkspaceMetadata>(sidecar).is_err(),
                "macOS block {{ base: {}, size: {} }} is off the grammar and must not parse",
                base,
                size
            );
        }

        #[test]
        fn public_metadata_deserializers_reject_every_unsupported_version(
            version in any::<u32>().prop_filter("version 1 is supported", |version| {
                *version != MARKER_VERSION && *version != SIDECAR_VERSION
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
        marker.version = MARKER_VERSION + 1;
        assert!(matches!(
            marker.validate(),
            Err(MetadataError::UnsupportedVersion {
                kind: "workspace marker",
                version: 2
            })
        ));
        marker.version = MARKER_VERSION;
        marker.role = WorkspaceRole::Main;
        assert!(matches!(
            marker.validate(),
            Err(MetadataError::WorkspaceRoleMismatch { .. })
        ));
    }

    /// An egress rule is a host, its ports and its mode: what the gateway enforces. A grant naming
    /// a TLS-fingerprint profile asks for something no gateway does, so it is refused rather than
    /// read as some other posture.
    #[test]
    fn an_egress_rule_naming_a_fingerprint_profile_is_refused() {
        let decoded = serde_json::from_str::<EgressRule>(
            r#"{"host":"scrape.example.com","mode":"intercept","impersonate":"chrome"}"#,
        );
        assert!(
            decoded.is_err(),
            "an egress rule naming a fingerprint profile decoded: {decoded:?}"
        );
    }

    #[test]
    fn port_blocks_and_platform_grants_enforce_boundaries() {
        let lowest = PortBlock::new(0, 16).unwrap();
        let highest =
            PortBlock::new(u16::MAX - NEW_PORT_BLOCK_SIZE + 1, NEW_PORT_BLOCK_SIZE).unwrap();
        assert_eq!(lowest.ports().unwrap(), 0..=15);
        assert_eq!(highest.ports().unwrap(), (u16::MAX - 63)..=u16::MAX);
        for invalid in [
            PortBlock { base: 80, size: 0 },
            PortBlock { base: 80, size: 15 },
            PortBlock { base: 64, size: 48 },
            // Aligned to 16 but not to its own size.
            PortBlock {
                base: MACOS_PORT_MIN + 16,
                size: NEW_PORT_BLOCK_SIZE,
            },
            PortBlock {
                base: u16::MAX - 14,
                size: 16,
            },
        ] {
            assert!(matches!(
                invalid.validate(),
                Err(MetadataError::InvalidPortBlock { base, size })
                    if base == invalid.base && size == invalid.size
            ));
        }

        // A live workspace's 16-port block and a new workspace's block both validate, each
        // against its own recorded size.
        let live = PortBlock::new(MACOS_PORT_MIN + 16, 16).unwrap();
        let fresh = PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).unwrap();
        for block in [live, fresh] {
            let grants = GrantSet::closed_baseline(Some(block)).unwrap();
            assert_eq!(grants.port_block, Some(block));
            assert_eq!(grants.revision, 0);
            assert!(grants.read.is_empty() && grants.write.is_empty() && grants.egress.is_empty());
            grants.validate(Platform::Macos).unwrap();
        }
        let macos = GrantSet::closed_baseline(Some(fresh)).unwrap();
        for outside in [
            lowest,
            PortBlock::new(MACOS_PORT_MIN - NEW_PORT_BLOCK_SIZE, NEW_PORT_BLOCK_SIZE).unwrap(),
            PortBlock::new(MACOS_PORT_MAX + 1, NEW_PORT_BLOCK_SIZE).unwrap(),
        ] {
            assert!(
                GrantSet::closed_baseline(Some(outside))
                    .unwrap()
                    .validate(Platform::Macos)
                    .is_err(),
                "{outside}"
            );
        }
        assert!(PortBlock::new(MACOS_PORT_MIN + 1, 16).is_err());
        assert!(
            GrantSet::closed_baseline(Some(PortBlock {
                base: u16::MAX,
                size: 16,
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
                base: MACOS_PORT_MIN,
                size: NEW_PORT_BLOCK_SIZE
            })
        ));
    }

    /// Live blocks of several sizes share the store; the reserved set holds any of them and
    /// refuses exactly the blocks that share a port with a held one, naming that one.
    #[test]
    fn reserved_blocks_of_mixed_sizes_never_overlap() {
        let block = |base: u16, size: u16| PortBlock::new(base, size).unwrap();
        let mut reserved = ReservedPortBlocks::default();
        let live = block(MACOS_PORT_MIN + 16, 16);
        reserved.insert(live).unwrap();
        reserved.insert(block(MACOS_PORT_MIN + 128, 16)).unwrap();
        assert_eq!(
            reserved.insert(block(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE)),
            Err(live)
        );
        assert_eq!(reserved.insert(block(MACOS_PORT_MIN + 16, 16)), Err(live));
        reserved.insert(block(MACOS_PORT_MIN, 16)).unwrap();
        reserved
            .insert(block(MACOS_PORT_MIN + 64, NEW_PORT_BLOCK_SIZE))
            .unwrap();
        assert_eq!(
            reserved.insert(block(MACOS_PORT_MIN + 64 + 48, 16)),
            Err(block(MACOS_PORT_MIN + 64, NEW_PORT_BLOCK_SIZE))
        );
        assert_eq!(
            PortBlock::macos_candidates()
                .find(|candidate| reserved.overlapping(*candidate).is_none()),
            Some(block(MACOS_PORT_MIN + 192, NEW_PORT_BLOCK_SIZE))
        );
        let held = reserved.blocks().collect::<Vec<_>>();
        for (index, first) in held.iter().enumerate() {
            for second in &held[index + 1..] {
                assert!(!first.overlaps(*second), "{first} and {second} overlap");
            }
        }
    }

    #[test]
    fn persisted_info_snapshot_round_trips() {
        let metadata: DetachedWorkspaceMetadata =
            serde_json::from_value(frozen_sidecar_json()).expect("decode snapshot");
        let encoded = serde_json::to_value(&metadata).expect("encode snapshot");
        assert_eq!(
            encoded["infoSnapshot"],
            frozen_sidecar_json()["infoSnapshot"]
        );
        assert_eq!(metadata.info_snapshot.project_root, Path::new("/project"));
    }

    /// The git-worktree fact has to survive a sidecar round trip, because every decision it drives
    /// — refusing checkpoint, pruning the registration out of main, requiring main mounted — is
    /// taken from the store side while the workspace itself may be detached. It is written only
    /// when true, so a standalone workspace's sidecar keeps the spelling it always had.
    #[test]
    fn git_worktree_mode_round_trips_and_is_absent_on_a_standalone_workspace() {
        let standalone: DetachedWorkspaceMetadata =
            serde_json::from_value(frozen_sidecar_json()).expect("decode sidecar");
        assert!(!standalone.info_snapshot.git_worktree);
        assert!(
            serde_json::to_value(&standalone).expect("encode sidecar")["infoSnapshot"]
                .get("gitWorktree")
                .is_none()
        );

        let mut wire = frozen_sidecar_json();
        wire["infoSnapshot"]["gitWorktree"] = serde_json::Value::Bool(true);
        let linked: DetachedWorkspaceMetadata =
            serde_json::from_value(wire.clone()).expect("decode git-worktree sidecar");
        assert!(linked.info_snapshot.git_worktree);
        assert_eq!(
            serde_json::to_value(&linked).expect("encode sidecar")["infoSnapshot"],
            wire["infoSnapshot"]
        );
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
    fn atomic_write_publishes_durably_and_leaves_no_residue() {
        let directory = temp_directory("atomic-publish");
        let path = directory.join("metadata.json");

        let value = json!({ "revision": 9 });
        write_json(&path, &value).unwrap();
        assert_eq!(read_json::<serde_json::Value>(&path).unwrap(), value);
        let residue: Vec<_> = fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| crate::fsio::is_temp_artifact(name))
            .collect();
        assert!(
            residue.is_empty(),
            "publish leaves no temp residue: {residue:?}"
        );

        let missing_parent_path = directory.join("missing").join("metadata.json");
        let error = write_json(&missing_parent_path, &value).unwrap_err();
        assert!(matches!(
            error,
            MetadataError::Io { path: error_path, source }
                if error_path.parent() == Some(directory.join("missing").as_path())
                    && source.kind() == io::ErrorKind::NotFound
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn each_document_version_is_one_and_refuses_any_other() {
        assert_eq!(MARKER_VERSION, 1);
        assert_eq!(SIDECAR_VERSION, 1);
        assert_eq!(SLOT_BINDINGS_VERSION, 1);
        marker_from_json().validate().unwrap();
        serde_json::from_value::<DetachedWorkspaceMetadata>(frozen_sidecar_json()).unwrap();
        assert!(
            serde_json::from_value::<SlotBindingsRecord>(json!({
                "version": 1,
                "bindings": []
            }))
            .unwrap()
            .into_bindings()
            .unwrap()
            .is_empty()
        );

        assert!(matches!(
            SlotBindingsRecord {
                version: 2,
                bindings: Vec::new(),
            }
            .into_bindings(),
            Err(MetadataError::UnsupportedVersion {
                kind: "slot bindings record",
                version: 2
            })
        ));
        let mut sidecar: DetachedWorkspaceMetadata =
            serde_json::from_value(frozen_sidecar_json()).unwrap();
        sidecar.version = 2;
        assert!(matches!(
            sidecar.validate(Path::new("/tmp/image.asif")),
            Err(MetadataError::UnsupportedVersion {
                kind: "detached workspace",
                version: 2
            })
        ));
    }
}
