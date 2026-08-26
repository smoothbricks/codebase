use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::metadata::WorkspaceName;
use crate::repository::RepoId;

pub const HOST_CONFIG_FILE: &str = "host.json";
pub const RETIRED_LAYOUT_HINT: &str =
    "cowshed setup --mount-root <dir> after detaching every workspace";
const HOST_CONFIG_VERSION: u32 = 1;
const RETIRED_MOUNT_DIRECTORY: &str = "mnt";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostConfig {
    version: u32,
    mount_root: PathBuf,
}

impl HostConfig {
    pub fn new(mount_root: impl Into<PathBuf>) -> Result<Self, HostConfigError> {
        let mount_root = mount_root.into();
        validate_absolute_path(&mount_root)?;
        Ok(Self {
            version: HOST_CONFIG_VERSION,
            mount_root,
        })
    }

    pub fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    /// Load the store-owned host configuration, using the documented per-user default when the
    /// configuration has not been written yet.
    pub fn load(store_root: &Path, home: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        validate_absolute_path(home)?;
        Self::load_or_default(store_root, home.join(".cowshed/mnt"))
    }

    /// Resolve configuration for callers that only possess the canonical store root.
    ///
    /// The current per-user substrate stores data at `~/.cowshed`, where the default is its `mnt`
    /// child. The dedicated-volume spelling cannot reveal the invoking user's home, so that one
    /// canonical path consults `HOME` only for the absent-config fallback. Persisted configuration
    /// is always authoritative and does not depend on process environment.
    pub fn load_for_store(store_root: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        let default = if store_root == Path::new("/private/cowshed/store") {
            let home = std::env::var_os("HOME").ok_or(HostConfigError::HomeUnavailable)?;
            let home = PathBuf::from(home);
            validate_absolute_path(&home)?;
            home.join(".cowshed/mnt")
        } else {
            store_root.join(RETIRED_MOUNT_DIRECTORY)
        };
        Self::load_or_default(store_root, default)
    }

    fn load_or_default(store_root: &Path, default: PathBuf) -> Result<Self, HostConfigError> {
        let path = store_root.join(HOST_CONFIG_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Self::new(default),
            Err(source) => return Err(io_error("read host configuration", path, source)),
        };
        require_private_file(&path)?;
        let config: Self = serde_json::from_slice(&bytes).map_err(|source| {
            HostConfigError::InvalidConfig {
                path: path.clone(),
                message: source.to_string(),
            }
        })?;
        if config.version != HOST_CONFIG_VERSION {
            return Err(HostConfigError::UnsupportedVersion {
                path,
                version: config.version,
            });
        }
        validate_absolute_path(&config.mount_root)?;
        Ok(config)
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct AttachedWorkspace {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
}

impl AttachedWorkspace {
    pub fn new(repo_id: RepoId, workspace: WorkspaceName) -> Self {
        Self { repo_id, workspace }
    }
}

impl std::fmt::Display for AttachedWorkspace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.repo_id, self.workspace)
    }
}

impl std::fmt::Debug for AttachedWorkspace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountRootChangePlan {
    store_root: PathBuf,
    config: HostConfig,
}

impl MountRootChangePlan {
    pub fn store_root(&self) -> &Path {
        &self.store_root
    }

    pub fn mount_root(&self) -> &Path {
        self.config.mount_root()
    }

    pub fn config_path(&self) -> PathBuf {
        self.store_root.join(HOST_CONFIG_FILE)
    }
}

/// Plan a host mount-root change without touching the filesystem.
///
/// The attached list is an authoritative kernel-derived snapshot supplied by the host adapter. It
/// is sorted and retained in the error so the CLI can name every workspace that blocks the change.
pub fn plan_mount_root_change(
    store_root: &Path,
    mount_root: &Path,
    attached: impl IntoIterator<Item = AttachedWorkspace>,
) -> Result<MountRootChangePlan, HostConfigError> {
    validate_absolute_path(store_root)?;
    let mut attached: Vec<_> = attached.into_iter().collect();
    attached.sort();
    attached.dedup();
    if !attached.is_empty() {
        return Err(HostConfigError::WorkspacesAttached { workspaces: attached });
    }
    Ok(MountRootChangePlan {
        store_root: store_root.to_path_buf(),
        config: HostConfig::new(mount_root.to_path_buf())?,
    })
}

/// Create the configured mount directory and atomically publish `host.json` with mode 0600.
pub fn execute_mount_root_change(
    plan: &MountRootChangePlan,
) -> Result<HostConfig, HostConfigError> {
    fs::create_dir_all(plan.mount_root()).map_err(|source| {
        io_error(
            "create workspace mount root",
            plan.mount_root().to_path_buf(),
            source,
        )
    })?;
    let bytes = serde_json::to_vec_pretty(&plan.config).map_err(|source| {
        HostConfigError::InvalidConfig {
            path: plan.config_path(),
            message: source.to_string(),
        }
    })?;
    write_private_atomic(&plan.config_path(), &bytes)?;
    Ok(plan.config.clone())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RetiredLayoutRecord {
    pub metadata_path: PathBuf,
    pub recorded_path: PathBuf,
}

impl RetiredLayoutRecord {
    pub fn doctor_message(&self) -> String {
        format!(
            "{} recorded under retired layout, run {RETIRED_LAYOUT_HINT}",
            self.recorded_path.display()
        )
    }
}

/// Find detached metadata that still records an absolute path beneath the retired `<store>/mnt`
/// layout. Invalid metadata remains the ordinary metadata doctor's responsibility; this detector
/// only reports valid JSON files containing an unmistakable retired absolute path.
pub fn retired_layout_paths(store_root: &Path) -> Result<Vec<RetiredLayoutRecord>, HostConfigError> {
    validate_absolute_path(store_root)?;
    let retired_root = store_root.join(RETIRED_MOUNT_DIRECTORY);
    if HostConfig::load_for_store(store_root)?.mount_root() == retired_root {
        return Ok(Vec::new());
    }
    let mut records = BTreeSet::new();
    if !store_root.exists() {
        return Ok(Vec::new());
    }
    for entry in WalkDir::new(store_root).follow_links(false) {
        let entry = entry.map_err(|source| HostConfigError::Scan {
            path: source
                .path()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| store_root.to_path_buf()),
            message: source.to_string(),
        })?;
        if !entry.file_type().is_file()
            || !entry
                .file_name()
                .to_string_lossy()
                .ends_with(".grants.json")
        {
            continue;
        }
        let bytes = fs::read(entry.path()).map_err(|source| {
            io_error(
                "read detached workspace metadata",
                entry.path().to_path_buf(),
                source,
            )
        })?;
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            continue;
        };
        collect_retired_paths(&value, &retired_root, entry.path(), &mut records);
    }
    Ok(records.into_iter().collect())
}

fn collect_retired_paths(
    value: &serde_json::Value,
    retired_root: &Path,
    metadata_path: &Path,
    records: &mut BTreeSet<RetiredLayoutRecord>,
) {
    match value {
        serde_json::Value::String(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() && path.starts_with(retired_root) {
                records.insert(RetiredLayoutRecord {
                    metadata_path: metadata_path.to_path_buf(),
                    recorded_path: path,
                });
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_retired_paths(value, retired_root, metadata_path, records);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_retired_paths(value, retired_root, metadata_path, records);
            }
        }
        _ => {}
    }
}

fn validate_absolute_path(path: &Path) -> Result<(), HostConfigError> {
    if !path.is_absolute() {
        return Err(HostConfigError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path is not absolute",
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(HostConfigError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path is not lexically normalized",
        });
    }
    Ok(())
}

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<(), HostConfigError> {
    let parent = path.parent().ok_or_else(|| HostConfigError::InvalidPath {
        path: path.to_path_buf(),
        reason: "path has no parent",
    })?;
    fs::create_dir_all(parent)
        .map_err(|source| io_error("create host configuration directory", parent, source))?;
    let temporary = parent.join(format!(".{HOST_CONFIG_FILE}.{}.tmp", Uuid::new_v4().simple()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .map_err(|source| io_error("create temporary host configuration", &temporary, source))?;
    let written = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()
    })();
    if let Err(source) = written {
        let _ = fs::remove_file(&temporary);
        return Err(io_error("write temporary host configuration", temporary, source));
    }
    drop(file);
    fs::rename(&temporary, path).map_err(|source| {
        let _ = fs::remove_file(&temporary);
        io_error("publish host configuration", path, source)
    })?;
    sync_directory(parent)?;
    Ok(())
}

fn require_private_file(path: &Path) -> Result<(), HostConfigError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("inspect host configuration", path, source))?;
    if !metadata.file_type().is_file() {
        return Err(HostConfigError::InvalidConfig {
            path: path.to_path_buf(),
            message: "configuration is not a regular file".to_owned(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(HostConfigError::InvalidMode {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), HostConfigError> {
    let directory = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|source| io_error("open host configuration directory", path, source))?;
    directory
        .sync_all()
        .map_err(|source| io_error("sync host configuration directory", path, source))
}

fn io_error(operation: &'static str, path: impl Into<PathBuf>, source: io::Error) -> HostConfigError {
    HostConfigError::Io {
        operation,
        path: path.into(),
        source,
    }
}

#[derive(Debug, Error)]
pub enum HostConfigError {
    #[error("invalid host path {path}: {reason}")]
    InvalidPath {
        path: PathBuf,
        reason: &'static str,
    },
    #[error("HOME is unavailable while resolving the default workspace mount root")]
    HomeUnavailable,
    #[error("host configuration {path} has unsupported version {version}")]
    UnsupportedVersion { path: PathBuf, version: u32 },
    #[error("host configuration {path} must have mode 0600, found {mode:04o}")]
    InvalidMode { path: PathBuf, mode: u32 },
    #[error("invalid host configuration {path}: {message}")]
    InvalidConfig { path: PathBuf, message: String },
    #[error("workspace mount root cannot change while attached: {workspaces:?}")]
    WorkspacesAttached { workspaces: Vec<AttachedWorkspace> },
    #[error("could not scan retired workspace layout at {path}: {message}")]
    Scan { path: PathBuf, message: String },
    #[error("could not {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn temp_directory(name: &str) -> PathBuf {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cowshed-host-config-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn absent_config_uses_default_mount_root() {
        let root = temp_directory("default");
        let home = root.join("home");
        let store = root.join("store");
        fs::create_dir_all(&store).unwrap();

        let config = HostConfig::load(&store, &home).unwrap();
        assert_eq!(config.mount_root(), home.join(".cowshed/mnt"));
        assert!(!store.join(HOST_CONFIG_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn custom_mount_root_round_trips_through_private_host_config() {
        let root = temp_directory("round-trip");
        let store = root.join("store");
        let mount_root = root.join("workspaces");
        fs::create_dir_all(&store).unwrap();

        let plan = plan_mount_root_change(&store, &mount_root, []).unwrap();
        execute_mount_root_change(&plan).unwrap();
        assert_eq!(
            HostConfig::load(&store, &root.join("home"))
                .unwrap()
                .mount_root(),
            mount_root
        );
        assert!(mount_root.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(store.join(HOST_CONFIG_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mount_root_change_names_every_attached_workspace_and_writes_nothing() {
        let root = temp_directory("attached");
        let store = root.join("store");
        let mount_root = root.join("workspaces");
        fs::create_dir_all(&store).unwrap();
        let attached = [
            AttachedWorkspace::new(
                RepoId::parse("zeta/widget").unwrap(),
                WorkspaceName::new("raven").unwrap(),
            ),
            AttachedWorkspace::new(
                RepoId::parse("acme/widget").unwrap(),
                WorkspaceName::new("swift").unwrap(),
            ),
        ];

        let error = plan_mount_root_change(&store, &mount_root, attached).unwrap_err();
        assert_eq!(
            error.to_string(),
            "workspace mount root cannot change while attached: [acme/widget/swift, zeta/widget/raven]"
        );
        let HostConfigError::WorkspacesAttached { workspaces } = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(
            workspaces.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["acme/widget/swift", "zeta/widget/raven"]
        );
        assert!(!mount_root.exists());
        assert!(!store.join(HOST_CONFIG_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_layout_detection_reports_metadata_and_recorded_path() {
        let root = temp_directory("retired");
        let store = root.join("store");
        let metadata = store.join("acme/widget/sessions/raven.asif.grants.json");
        fs::create_dir_all(metadata.parent().unwrap()).unwrap();
        let plan =
            plan_mount_root_change(&store, &root.join("configured-mount-root"), []).unwrap();
        execute_mount_root_change(&plan).unwrap();
        let recorded = store.join("mnt/acme/widget/raven");
        fs::write(
            &metadata,
            serde_json::to_vec(&serde_json::json!({
                "infoSnapshot": { "projectRoot": recorded },
                "read": [root.join("unrelated")]
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            retired_layout_paths(&store).unwrap(),
            vec![RetiredLayoutRecord {
                metadata_path: metadata,
                recorded_path: store.join("mnt/acme/widget/raven"),
            }]
        );
        assert_eq!(
            retired_layout_paths(&store).unwrap()[0].doctor_message(),
            format!(
                "{} recorded under retired layout, run {RETIRED_LAYOUT_HINT}",
                store.join("mnt/acme/widget/raven").display()
            )
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_default_mount_layout_is_not_reported_as_retired() {
        let root = temp_directory("active-default");
        let store = root.join("store");
        let metadata = store.join("acme/widget/sessions/raven.asif.grants.json");
        fs::create_dir_all(metadata.parent().unwrap()).unwrap();
        fs::write(
            &metadata,
            serde_json::to_vec(&serde_json::json!({
                "write": [store.join("mnt/acme/widget/raven")]
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(retired_layout_paths(&store).unwrap().is_empty());
        fs::remove_dir_all(root).unwrap();
    }
}
