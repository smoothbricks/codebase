use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use walkdir::WalkDir;

use crate::metadata::{GRANTS_SIDECAR_SUFFIX, WorkspaceName};
use crate::repository::RepoId;

use super::bootstrap::STORE_ROOT;

pub const HOST_CONFIG_FILE: &str = "host.json";
pub const RETIRED_LAYOUT_HINT: &str =
    "cowshed setup --mount-root <dir> after detaching every workspace";
const HOST_CONFIG_VERSION: u32 = 1;
pub(crate) const RETIRED_MOUNT_DIRECTORY: &str = "mnt";

/// Where a host presents workspace mounts when no configuration has been written. Stated once:
/// the retired-layout migration in `runtime::project` has to recognise the same directory, and a
/// literal there could not follow a change made here.
pub(crate) const DEFAULT_MOUNT_RELATIVE: &str = ".cowshed/mnt";

/// One approved gateway-backed registry route, as host state rather than workspace state.
///
/// The secret itself lives in the platform credential store; this is the non-secret half the
/// store cannot answer: which routes exist at all (a keychain item is found by its exact
/// account, never enumerated), and the NAME of the host environment variable the operator
/// enrolled from. That name is what lets a spawn withhold an ambient copy of the same token
/// from a sandbox — a gateway-held credential is pointless if the workspace also gets the bytes.
///
/// Unlike [`HostConfig`] itself, a route is strict: a key this build does not know could be a
/// constraint on where the credential may be used, and honouring the half it understands is
/// worse than refusing. Anything added here therefore has to answer the same question the file
/// already answered — what an older binary does when it meets it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialRoute {
    pub repo_id: String,
    pub origin: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_env_names: Vec<String>,
}

impl CredentialRoute {
    fn validate(&self) -> Result<(), HostConfigError> {
        if self.repo_id.is_empty() || self.origin.is_empty() {
            return Err(HostConfigError::InvalidCredentialRoute {
                reason: "a credential route needs both a repository id and an origin",
            });
        }
        for name in &self.secret_env_names {
            if !is_environment_name(name) {
                return Err(HostConfigError::InvalidCredentialRoute {
                    reason: "a registered secret source must be an environment variable name",
                });
            }
        }
        Ok(())
    }
}

/// A POSIX-shaped environment variable name. Anything else could not name a variable to withhold.
fn is_environment_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with(|first: char| first.is_ascii_digit())
        && name
            .chars()
            .all(|character| character == '_' || character.is_ascii_alphanumeric())
}

/// Host state every cowshed binary on the machine reads, so unknown keys are TOLERATED.
///
/// This file outlives any one build: a workspace image, a daemon, and the CLI on `PATH` can all
/// be different versions of cowshed at once. A reader that refuses a field a newer writer added
/// turns one enrolment into every other binary on the host failing to run at all — which is a
/// far worse failure than ignoring a key it has no use for. `version` is what a genuinely
/// incompatible layout would bump; additive keys are not that.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostConfig {
    version: u32,
    mount_root: PathBuf,
    /// Approved gateway-backed registry routes. Absent in a configuration that has none, so a
    /// host that never enrolled one keeps writing exactly the bytes it wrote before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    credential_routes: Vec<CredentialRoute>,
}

impl HostConfig {
    pub fn new(mount_root: impl Into<PathBuf>) -> Result<Self, HostConfigError> {
        let mount_root = mount_root.into();
        validate_absolute_path(&mount_root)?;
        Ok(Self {
            version: HOST_CONFIG_VERSION,
            mount_root,
            credential_routes: Vec::new(),
        })
    }

    pub fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    pub fn credential_routes(&self) -> &[CredentialRoute] {
        &self.credential_routes
    }

    /// Environment variable names this project's approved routes were enrolled from.
    ///
    /// A spawn withholds exactly these from the child: the gateway holds the credential, so an
    /// ambient copy in the operator's shell has no reason to travel into a sandbox.
    pub fn credential_env_names(&self, repo_id: &str) -> BTreeSet<String> {
        self.credential_routes
            .iter()
            .filter(|route| route.repo_id == repo_id)
            .flat_map(|route| route.secret_env_names.iter().cloned())
            .collect()
    }

    /// Record one route, replacing any earlier record of the same repository and origin.
    ///
    /// Enrolling again is rotation, which is the common case; refusing it would only leave an
    /// operator to delete and re-add, with no credential in between.
    pub fn upsert_credential_route(
        &mut self,
        route: CredentialRoute,
    ) -> Result<(), HostConfigError> {
        route.validate()?;
        self.credential_routes
            .retain(|held| held.repo_id != route.repo_id || held.origin != route.origin);
        self.credential_routes.push(route);
        self.credential_routes.sort();
        Ok(())
    }

    /// Forget one route. `false` means it was not recorded, which is already the asked state.
    pub fn remove_credential_route(&mut self, repo_id: &str, origin: &str) -> bool {
        let before = self.credential_routes.len();
        self.credential_routes
            .retain(|held| held.repo_id != repo_id || held.origin != origin);
        before != self.credential_routes.len()
    }

    /// Publish the configuration atomically at mode 0600, the way the mount root is published.
    pub fn save(&self, store_root: &Path) -> Result<(), HostConfigError> {
        validate_absolute_path(store_root)?;
        for route in &self.credential_routes {
            route.validate()?;
        }
        // `write_private_atomic` terminates the file itself; appending here as well wrote a
        // second newline, so a host that enrolled nothing no longer matched the bytes the
        // mount-root writer had produced.
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|source| HostConfigError::InvalidConfig {
                path: store_root.join(HOST_CONFIG_FILE),
                message: source.to_string(),
            })?;
        write_private_atomic(&store_root.join(HOST_CONFIG_FILE), &bytes)
    }

    /// Load the store-owned host configuration, using the documented per-user default when the
    /// configuration has not been written yet.
    pub fn load(store_root: &Path, home: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        validate_absolute_path(home)?;
        Self::load_or_default(store_root, home.join(DEFAULT_MOUNT_RELATIVE))
    }

    /// Resolve configuration for callers that only possess the canonical store root.
    ///
    /// Workspace mounts remain a per-user presentation layer even though the evidence volumes are
    /// machine-global. The fixed store path cannot reveal the invoking user's home, so the absent
    /// configuration fallback consults `HOME` only to derive `~/.cowshed/mnt`. Persisted
    /// configuration is always authoritative and does not depend on process environment.
    pub fn load_for_store(store_root: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        let default = if store_root == Path::new(STORE_ROOT) {
            let home = std::env::var_os("HOME").ok_or(HostConfigError::HomeUnavailable)?;
            let home = PathBuf::from(home);
            validate_absolute_path(&home)?;
            home.join(DEFAULT_MOUNT_RELATIVE)
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
        let config: Self =
            serde_json::from_slice(&bytes).map_err(|source| HostConfigError::InvalidConfig {
                path: path.clone(),
                message: source.to_string(),
            })?;
        if config.version != HOST_CONFIG_VERSION {
            return Err(HostConfigError::UnsupportedVersion {
                path,
                version: config.version,
            });
        }
        validate_absolute_path(&config.mount_root)?;
        for route in &config.credential_routes {
            route.validate()?;
        }
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
/// The attached list is an authoritative kernel-derived snapshot supplied by the host adapter.
/// Main is deliberately ignored: direct-mounted mains live at their checkout paths, outside the
/// configurable session mount root. Session workspaces are sorted and retained in the error so
/// the CLI can name every mount that actually blocks the change.
pub fn plan_mount_root_change(
    store_root: &Path,
    mount_root: &Path,
    attached: impl IntoIterator<Item = AttachedWorkspace>,
) -> Result<MountRootChangePlan, HostConfigError> {
    validate_absolute_path(store_root)?;
    let mut attached: Vec<_> = attached
        .into_iter()
        .filter(|workspace| !workspace.workspace.is_main())
        .collect();
    attached.sort();
    attached.dedup();
    if !attached.is_empty() {
        return Err(HostConfigError::WorkspacesAttached {
            workspaces: attached,
        });
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

/// Find detached metadata in validated `<store>/<owner>/<repo>` projects that still records an
/// absolute path beneath the retired `<store>/mnt` layout. Store neighbours and unreadable APFS
/// metadata directories are excluded by project discovery before this recursive scan begins.
/// Invalid grants remain the ordinary metadata doctor's responsibility; this detector only reports
/// valid JSON files containing an unmistakable retired absolute path.
pub fn retired_layout_paths(
    store_root: &Path,
) -> Result<Vec<RetiredLayoutRecord>, HostConfigError> {
    validate_absolute_path(store_root)?;
    let retired_root = store_root.join(RETIRED_MOUNT_DIRECTORY);
    if HostConfig::load_for_store(store_root)?.mount_root() == retired_root {
        return Ok(Vec::new());
    }
    let mut records = BTreeSet::new();
    if !store_root.exists() {
        return Ok(Vec::new());
    }
    let repositories =
        crate::gateway_inventory::discover_repositories(store_root).map_err(|source| {
            HostConfigError::Scan {
                path: store_root.to_path_buf(),
                message: source.to_string(),
            }
        })?;
    for repo_id in repositories {
        let project_root = crate::storage::StorageLayout::new(store_root, &repo_id)
            .map_err(|source| HostConfigError::Scan {
                path: store_root.to_path_buf(),
                message: source.to_string(),
            })?
            .project()
            .project_root
            .clone();
        for entry in WalkDir::new(&project_root).follow_links(false) {
            let entry = entry.map_err(|source| HostConfigError::Scan {
                path: source
                    .path()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| project_root.clone()),
                message: source.to_string(),
            })?;
            if !entry.file_type().is_file()
                || !entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(GRANTS_SIDECAR_SUFFIX)
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
    crate::fsio::publish_private_file::<io::Error>(path, |writer| {
        writer.write_all(bytes)?;
        writer.write_all(b"\n")
    })
    .map_err(|error| match error {
        crate::fsio::PublishError::Io { path, source } => {
            io_error("publish host configuration", path, source)
        }
        crate::fsio::PublishError::Write(source) => {
            io_error("write host configuration", path.to_path_buf(), source)
        }
    })
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

fn io_error(
    operation: &'static str,
    path: impl Into<PathBuf>,
    source: io::Error,
) -> HostConfigError {
    HostConfigError::Io {
        operation,
        path: path.into(),
        source,
    }
}

#[derive(Debug, Error)]
pub enum HostConfigError {
    #[error("invalid host path {path}: {reason}")]
    InvalidPath { path: PathBuf, reason: &'static str },
    #[error("HOME is unavailable while resolving the default workspace mount root")]
    HomeUnavailable,
    #[error("host configuration {path} has unsupported version {version}")]
    UnsupportedVersion { path: PathBuf, version: u32 },
    #[error("host configuration {path} must have mode 0600, found {mode:04o}")]
    InvalidMode { path: PathBuf, mode: u32 },
    #[error("invalid host configuration {path}: {message}")]
    InvalidConfig { path: PathBuf, message: String },
    #[error("invalid gateway credential route: {reason}")]
    InvalidCredentialRoute { reason: &'static str },
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

    fn bind_repository(store: &Path, repo_id: &str) -> PathBuf {
        let repo_id = RepoId::parse(repo_id).expect("repository identity");
        let paths = crate::storage::StorageLayout::new(store, &repo_id)
            .expect("project paths")
            .project()
            .clone();
        fs::create_dir_all(&paths.project_root).expect("project root");
        let binding =
            crate::repository::RepositoryBinding::new(vec![crate::repository::BoundIdentity {
                repo_id,
                remote_name: None,
                remote_url: None,
                primary: true,
            }])
            .expect("repository binding");
        crate::metadata::write_json(&paths.repository_binding, &binding).expect("binding file");
        paths.project_root
    }

    fn route(repo_id: &str, origin: &str, names: &[&str]) -> CredentialRoute {
        CredentialRoute {
            repo_id: repo_id.to_owned(),
            origin: origin.to_owned(),
            secret_env_names: names.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    #[test]
    fn credential_routes_persist_and_answer_per_project() {
        let store = temp_directory("routes");
        let mut config = HostConfig::new("/Users/tester/.cowshed/mnt").expect("config");
        config
            .upsert_credential_route(route(
                "owner/repo",
                "https://registry.test:443",
                &["REGISTRY_READ_TOKEN"],
            ))
            .expect("route");
        config
            .upsert_credential_route(route("other/repo", "https://registry.test:443", &["OTHER"]))
            .expect("route");
        // Enrolling the same origin again is rotation, not a second route.
        config
            .upsert_credential_route(route(
                "owner/repo",
                "https://registry.test:443",
                &["ROTATED_TOKEN"],
            ))
            .expect("route");
        config.save(&store).expect("save");

        let loaded = HostConfig::load(&store, Path::new("/Users/tester")).expect("load");
        assert_eq!(loaded.credential_routes().len(), 2);
        assert_eq!(
            loaded.credential_env_names("owner/repo"),
            BTreeSet::from(["ROTATED_TOKEN".to_owned()])
        );
        assert!(loaded.credential_env_names("absent/repo").is_empty());
        assert_eq!(loaded.mount_root(), Path::new("/Users/tester/.cowshed/mnt"));

        let mut without = loaded.clone();
        assert!(without.remove_credential_route("owner/repo", "https://registry.test:443"));
        assert!(!without.remove_credential_route("owner/repo", "https://registry.test:443"));
        without.save(&store).expect("save");
        assert!(
            HostConfig::load(&store, Path::new("/Users/tester"))
                .expect("load")
                .credential_env_names("owner/repo")
                .is_empty()
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }

    /// A host that never enrolled a credential must not be able to tell that this file gained
    /// the ability to hold one — byte for byte, because the two writers publish the same file.
    #[test]
    fn a_host_with_no_routes_writes_exactly_what_the_mount_root_writer_writes() {
        let root = temp_directory("no-routes");
        let saved_store = root.join("saved");
        let planned_store = root.join("planned");
        fs::create_dir_all(&saved_store).expect("saved store");
        fs::create_dir_all(&planned_store).expect("planned store");
        let mount_root = root.join("workspaces");

        HostConfig::new(&mount_root)
            .expect("config")
            .save(&saved_store)
            .expect("save");
        let plan =
            plan_mount_root_change(&planned_store, &mount_root, []).expect("mount root plan");
        execute_mount_root_change(&plan).expect("mount root change");

        let saved = fs::read(saved_store.join(HOST_CONFIG_FILE)).expect("saved bytes");
        let planned = fs::read(planned_store.join(HOST_CONFIG_FILE)).expect("planned bytes");
        assert_eq!(
            String::from_utf8_lossy(&saved),
            String::from_utf8_lossy(&planned)
        );
        assert!(!String::from_utf8_lossy(&saved).contains("credentialRoutes"));
        fs::remove_dir_all(&root).expect("cleanup");
    }

    /// One machine runs several cowshed builds at once — a workspace image, the daemon, and the
    /// CLI on `PATH`. A reader that refuses a key a newer writer added makes one enrolment brick
    /// every other binary on the host, which is exactly what a shared state file must not do.
    #[test]
    fn a_key_this_build_does_not_know_is_ignored_rather_than_fatal() {
        let store = temp_directory("forward-compatible");
        fs::write(
            store.join(HOST_CONFIG_FILE),
            br#"{"version":1,"mountRoot":"/Users/tester/.cowshed/mnt","somethingNewer":{"a":1}}"#,
        )
        .expect("write");
        #[cfg(unix)]
        fs::set_permissions(
            store.join(HOST_CONFIG_FILE),
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .expect("mode");

        let config = HostConfig::load(&store, Path::new("/Users/tester")).expect("load");
        assert_eq!(config.mount_root(), Path::new("/Users/tester/.cowshed/mnt"));
        fs::remove_dir_all(&store).expect("cleanup");
    }

    #[test]
    fn a_route_that_could_not_name_a_variable_to_withhold_is_refused() {
        let mut config = HostConfig::new("/Users/tester/.cowshed/mnt").expect("config");
        for names in [vec!["lower case"], vec!["1TOKEN"], vec![""]] {
            assert!(
                config
                    .upsert_credential_route(route(
                        "owner/repo",
                        "https://registry.test:443",
                        &names
                    ))
                    .is_err(),
                "{names:?} must be refused"
            );
        }
        assert!(
            config
                .upsert_credential_route(route("", "https://registry.test:443", &[]))
                .is_err()
        );
        assert!(config.credential_routes().is_empty());
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
    fn direct_mounted_main_does_not_block_mount_root_change() {
        let root = temp_directory("attached-main");
        let store = root.join("store");
        let mount_root = root.join("workspaces");
        fs::create_dir_all(&store).unwrap();

        let plan = plan_mount_root_change(
            &store,
            &mount_root,
            [AttachedWorkspace::new(
                RepoId::parse("example-org/example-app").unwrap(),
                WorkspaceName::new("main").unwrap(),
            )],
        )
        .expect("main is mounted at its checkout, not below the session mount root");
        execute_mount_root_change(&plan).unwrap();

        assert_eq!(
            HostConfig::load(&store, &root.join("home"))
                .unwrap()
                .mount_root(),
            mount_root
        );
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
                RepoId::parse("example-org/example-app").unwrap(),
                WorkspaceName::new("main").unwrap(),
            ),
            AttachedWorkspace::new(
                RepoId::parse("zeta/widget").unwrap(),
                WorkspaceName::new("raven").unwrap(),
            ),
        ];

        let error = plan_mount_root_change(&store, &mount_root, attached).unwrap_err();
        assert_eq!(
            error.to_string(),
            "workspace mount root cannot change while attached: [zeta/widget/raven]"
        );
        let HostConfigError::WorkspacesAttached { workspaces } = error else {
            panic!("unexpected error: {error}");
        };
        assert_eq!(
            workspaces
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["zeta/widget/raven"]
        );
        assert!(!mount_root.exists());
        assert!(!store.join(HOST_CONFIG_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_layout_detection_reports_metadata_and_recorded_path() {
        let root = temp_directory("retired");
        let store = root.join("store");
        let project = bind_repository(&store, "acme/widget");
        let metadata = project.join("sessions/raven.asif.grants.json");
        fs::create_dir_all(metadata.parent().unwrap()).unwrap();
        let plan = plan_mount_root_change(&store, &root.join("configured-mount-root"), []).unwrap();
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
        let project = bind_repository(&store, "acme/widget");
        let metadata = project.join("sessions/raven.asif.grants.json");
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

    #[cfg(unix)]
    #[test]
    fn retired_layout_detection_skips_unreadable_apfs_system_directories() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_directory("retired-system-directory");
        let store = root.join("store");
        bind_repository(&store, "acme/widget");
        let plan = plan_mount_root_change(&store, &root.join("configured-mount-root"), []).unwrap();
        execute_mount_root_change(&plan).unwrap();
        let spotlight = store.join(".Spotlight-V100");
        fs::create_dir_all(&spotlight).unwrap();
        fs::set_permissions(&spotlight, fs::Permissions::from_mode(0o000)).unwrap();

        let result = retired_layout_paths(&store);
        fs::set_permissions(&spotlight, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            result.unwrap().is_empty(),
            "an unrelated APFS system directory must not become a retired-layout scan failure"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
