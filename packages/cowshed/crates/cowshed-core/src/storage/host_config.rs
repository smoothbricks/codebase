use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use walkdir::WalkDir;

use cowshed_types::metadata::GRANTS_SIDECAR_SUFFIX;

use cowshed_gateway_types::STORE_ROOT;

pub const HOST_CONFIG_FILE: &str = "host.json";
pub const RETIRED_LAYOUT_HINT: &str =
    "cowshed setup after auditing detached metadata and grants against the fixed workspace root";
const HOST_CONFIG_VERSION: u32 = 1;
pub(crate) const RETIRED_MOUNT_DIRECTORY: &str = "mnt";

/// Stable macOS presentation root; numeric uid avoids home and temporary-directory aliases.
#[cfg(target_os = "macos")]
pub(crate) const MACOS_MOUNT_PREFIX: &str = "/private/var/folders/cowshed";

pub(crate) fn canonical_mount_root(home: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        let _ = home;
        macos_mount_root(unsafe { libc::getuid() })
    }
    #[cfg(not(target_os = "macos"))]
    {
        home.join(".cowshed/mnt")
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn macos_mount_root(uid: u32) -> PathBuf {
    Path::new(MACOS_MOUNT_PREFIX)
        .join(uid.to_string())
        .join("mnt")
}

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

/// Host state tolerates additive unknown keys across installed builds. The retired `mountRoot`
/// key is not an additive extension: a custom value must refuse rather than strand old mounts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostConfig {
    version: u32,
    /// Derived by the loader, never a host.json setting.
    #[serde(skip_serializing)]
    mount_root: PathBuf,
    /// Approved gateway-backed registry routes. Absent in a configuration that has none, so a
    /// host that never enrolled one keeps writing exactly the bytes it wrote before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    credential_routes: Vec<CredentialRoute>,
}

/// Read-only recognition of the retired field; no writer can publish it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredHostConfig {
    version: u32,
    #[serde(default, rename = "mountRoot")]
    retired_mount_root: Option<PathBuf>,
    #[serde(default)]
    credential_routes: Vec<CredentialRoute>,
}

impl HostConfig {
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

    /// Publish credential routes atomically at mode 0600; the mount root is never stored.
    pub fn save(&self, store_root: &Path) -> Result<(), HostConfigError> {
        validate_absolute_path(store_root)?;
        for route in &self.credential_routes {
            route.validate()?;
        }
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|source| HostConfigError::InvalidConfig {
                path: store_root.join(HOST_CONFIG_FILE),
                message: source.to_string(),
            })?;
        write_private_atomic(&store_root.join(HOST_CONFIG_FILE), &bytes)
    }

    /// Read host state and derive the fixed per-user root without writing configuration.
    pub fn load(store_root: &Path, home: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        validate_absolute_path(home)?;
        Self::load_with_root(store_root, canonical_mount_root(home))
    }

    /// Resolve the invoking host's fixed root. Explicit fixture stores retain their deterministic
    /// store-local root; neither case reads a configurable mount path.
    pub fn load_for_store(store_root: &Path) -> Result<Self, HostConfigError> {
        validate_absolute_path(store_root)?;
        let root = if store_root == Path::new(STORE_ROOT) {
            #[cfg(target_os = "macos")]
            {
                macos_mount_root(unsafe { libc::getuid() })
            }
            #[cfg(not(target_os = "macos"))]
            {
                let home = std::env::var_os("HOME").ok_or(HostConfigError::HomeUnavailable)?;
                let home = PathBuf::from(home);
                validate_absolute_path(&home)?;
                canonical_mount_root(&home)
            }
        } else {
            store_root.join(RETIRED_MOUNT_DIRECTORY)
        };
        Self::load_with_root(store_root, root)
    }

    fn load_with_root(store_root: &Path, mount_root: PathBuf) -> Result<Self, HostConfigError> {
        let path = store_root.join(HOST_CONFIG_FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    version: HOST_CONFIG_VERSION,
                    mount_root,
                    credential_routes: Vec::new(),
                });
            }
            Err(source) => return Err(io_error("read host configuration", path, source)),
        };
        require_private_file(&path)?;
        let stored: StoredHostConfig =
            serde_json::from_slice(&bytes).map_err(|source| HostConfigError::InvalidConfig {
                path: path.clone(),
                message: source.to_string(),
            })?;
        if stored.version != HOST_CONFIG_VERSION {
            return Err(HostConfigError::UnsupportedVersion {
                path,
                version: stored.version,
            });
        }
        if let Some(recorded) = stored.retired_mount_root
            && recorded != mount_root
        {
            return Err(HostConfigError::RetiredMountRoot {
                path,
                recorded,
                required: mount_root,
            });
        }
        for route in &stored.credential_routes {
            route.validate()?;
        }
        Ok(Self {
            version: stored.version,
            mount_root,
            credential_routes: stored.credential_routes,
        })
    }
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
    scan_retired_layout_paths(store_root, &retired_root)
}

fn scan_retired_layout_paths(
    store_root: &Path,
    retired_root: &Path,
) -> Result<Vec<RetiredLayoutRecord>, HostConfigError> {
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
            collect_retired_paths(&value, retired_root, entry.path(), &mut records);
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
    cowshed_os::fsio::publish_private_file::<io::Error>(path, |writer| {
        writer.write_all(bytes)?;
        writer.write_all(b"\n")
    })
    .map_err(|error| match error {
        cowshed_os::fsio::PublishError::Io { path, source } => {
            io_error("publish host configuration", path, source)
        }
        cowshed_os::fsio::PublishError::Write(source) => {
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
    #[error("could not scan retired workspace layout at {path}: {message}")]
    Scan { path: PathBuf, message: String },
    #[error("could not {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "host configuration {path} records retired custom mountRoot {recorded}; the workspace mount root is fixed at {required}. This read did not change mounts or configuration. Using the previous installed build, stop workspace jobs and daemons and detach every session and build volume at the old root; resolve busy holders and audit detached metadata and sandbox grants. Only then remove mountRoot from {path}, preserving credential routes and mode 0600, and run the new build's cowshed setup. For another access path, create a symlink at the preferred path pointing to {required}, never the reverse"
    )]
    RetiredMountRoot {
        path: PathBuf,
        recorded: PathBuf,
        required: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use cowshed_types::metadata::WorkspaceName;
    use cowshed_types::repository::RepoId;
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
        let layout = crate::storage::StorageLayout::new(store, &repo_id).expect("project paths");
        fs::create_dir_all(store).expect("store root");
        layout.provision_project().expect("project root");
        let paths = layout.project().clone();
        let binding = cowshed_types::repository::RepositoryBinding::new(vec![
            cowshed_types::repository::BoundIdentity {
                repo_id,
                remote_name: None,
                remote_url: None,
                primary: true,
            },
        ])
        .expect("repository binding");
        cowshed_types::metadata::write_json(&paths.repository_binding, &binding)
            .expect("binding file");
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
        let mut config = HostConfig::load(&store, Path::new("/Users/tester")).expect("config");
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
        assert_eq!(
            loaded.mount_root(),
            canonical_mount_root(Path::new("/Users/tester"))
        );

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

    /// Host paths are derived runtime facts, not serialized host configuration.
    #[test]
    fn host_config_never_serializes_the_derived_mount_root() {
        let store = temp_directory("no-routes");
        let config = HostConfig::load_for_store(&store).unwrap();
        config.save(&store).unwrap();
        let bytes = fs::read(store.join(HOST_CONFIG_FILE)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            serde_json::json!({"version": 1})
        );
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
        fs::remove_dir_all(store).unwrap();
    }

    /// Additive unknown keys remain forward compatible; the retired mount key is handled separately.
    #[test]
    fn a_key_this_build_does_not_know_is_ignored_rather_than_fatal() {
        let store = temp_directory("forward-compatible");
        write_private_atomic(
            &store.join(HOST_CONFIG_FILE),
            br#"{"version":1,"somethingNewer":{"a":1}}"#,
        )
        .unwrap();
        let config = HostConfig::load(&store, Path::new("/Users/tester")).unwrap();
        assert_eq!(
            config.mount_root(),
            canonical_mount_root(Path::new("/Users/tester"))
        );
        fs::remove_dir_all(store).unwrap();
    }

    #[test]
    fn a_route_that_could_not_name_a_variable_to_withhold_is_refused() {
        let store = temp_directory("invalid-route");
        let mut config = HostConfig::load_for_store(&store).expect("config");
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
        fs::remove_dir_all(store).unwrap();
    }
    #[test]
    fn absent_config_uses_default_mount_root() {
        let root = temp_directory("default");
        let home = root.join("home");
        let store = root.join("store");
        fs::create_dir_all(&store).unwrap();

        let config = HostConfig::load(&store, &home).unwrap();
        assert_eq!(config.mount_root(), canonical_mount_root(&home));
        assert!(!store.join(HOST_CONFIG_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_default_is_uid_stable_and_keeps_dirhelper_names_above_repo_owners() {
        let uid = unsafe { libc::getuid() };
        let expected = PathBuf::from(format!("/private/var/folders/cowshed/{uid}/mnt"));
        assert_eq!(canonical_mount_root(Path::new("/Users/first")), expected);
        assert_eq!(canonical_mount_root(Path::new("/Users/second")), expected);
        assert_eq!(
            macos_mount_root(501),
            Path::new("/private/var/folders/cowshed/501/mnt")
        );
        let store = temp_directory("stable-layout");
        for owner in ["t", "temporaryitems", "x", "c"] {
            let repo_id = RepoId::parse(&format!("{owner}/widget")).unwrap();
            let layout =
                crate::storage::StorageLayout::with_mount_root(&store, &expected, &repo_id)
                    .unwrap();
            assert_eq!(
                layout
                    .workspace_mount(&WorkspaceName::session("raven").unwrap())
                    .unwrap(),
                expected.join(owner).join("widget/raven")
            );
            let checkout = Path::new("/Users/tester/Dev/widget");
            assert_eq!(
                layout
                    .main_aware_workspace_mount(checkout, &WorkspaceName::new("main").unwrap())
                    .unwrap(),
                checkout
            );
            let builds = crate::build_volume::BuildVolumeLayout::new(layout.project()).unwrap();
            assert_eq!(
                builds.mounts(),
                expected.join(".build").join(owner).join("widget")
            );
        }
        fs::remove_dir_all(store).unwrap();
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn non_macos_default_remains_under_home() {
        assert_eq!(
            canonical_mount_root(Path::new("/home/tester")),
            Path::new("/home/tester/.cowshed/mnt")
        );
    }

    #[test]
    fn explicit_fixture_store_default_remains_store_local() {
        let store = temp_directory("fixture-default");
        assert_eq!(
            HostConfig::load_for_store(&store).unwrap().mount_root(),
            store.join("mnt")
        );
        fs::remove_dir_all(store).unwrap();
    }

    #[test]
    fn custom_legacy_root_is_refused_by_both_loaders_without_mutation() {
        let store = temp_directory("legacy-custom");
        let home = store.join("home");
        let old_root = store.join("indexed/custom-root");
        let legacy = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mountRoot": old_root,
            "credentialRoutes": [route("owner/repo", "https://registry.test:443", &["TOKEN"])]
        }))
        .unwrap();
        let path = store.join(HOST_CONFIG_FILE);
        write_private_atomic(&path, &legacy).unwrap();
        let before = fs::read(&path).unwrap();
        for (result, required) in [
            (HostConfig::load(&store, &home), canonical_mount_root(&home)),
            (HostConfig::load_for_store(&store), store.join("mnt")),
        ] {
            let error = result.unwrap_err();
            let message = error.to_string();
            assert!(matches!(error, HostConfigError::RetiredMountRoot { .. }));
            for text in [
                path.display().to_string(),
                old_root.display().to_string(),
                required.display().to_string(),
                "previous installed build".into(),
                "detach every session and build volume".into(),
                "remove mountRoot".into(),
                "symlink at the preferred path".into(),
            ] {
                assert!(message.contains(&text), "{message} lacks {text}");
            }
            assert_eq!(fs::read(&path).unwrap(), before);
        }
        assert!(!old_root.exists());
        assert!(!store.join("mnt").exists());
        fs::remove_dir_all(store).unwrap();
    }

    #[test]
    fn canonical_legacy_key_is_accepted_but_never_written_back() {
        let store = temp_directory("legacy-canonical");
        let home = store.join("home");
        let required = canonical_mount_root(&home);
        let path = store.join(HOST_CONFIG_FILE);
        write_private_atomic(
            &path,
            &serde_json::to_vec(&serde_json::json!({
                "version": 1, "mountRoot": required,
                "credentialRoutes": [route("owner/repo", "https://registry.test:443", &["TOKEN"])]
            }))
            .unwrap(),
        )
        .unwrap();
        let before = fs::read(&path).unwrap();
        let config = HostConfig::load(&store, &home).unwrap();
        assert_eq!(config.mount_root(), required);
        assert_eq!(fs::read(&path).unwrap(), before, "read never rewrites");
        config.save(&store).unwrap();
        let saved: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(saved.get("mountRoot").is_none());
        assert_eq!(
            HostConfig::load(&store, &home).unwrap().credential_routes(),
            config.credential_routes()
        );
        fs::remove_dir_all(store).unwrap();
    }

    #[test]
    fn retired_layout_detection_reports_metadata_and_recorded_path() {
        let root = temp_directory("retired");
        let store = root.join("store");
        let project = bind_repository(&store, "acme/widget");
        let metadata = project.join("sessions/raven.dmg.grants.json");
        fs::create_dir_all(metadata.parent().unwrap()).unwrap();
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
            scan_retired_layout_paths(&store, &store.join("mnt")).unwrap(),
            vec![RetiredLayoutRecord {
                metadata_path: metadata,
                recorded_path: store.join("mnt/acme/widget/raven"),
            }]
        );
        assert_eq!(
            scan_retired_layout_paths(&store, &store.join("mnt")).unwrap()[0].doctor_message(),
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
        let metadata = project.join("sessions/raven.dmg.grants.json");
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
        let spotlight = store.join(".Spotlight-V100");
        fs::create_dir_all(&spotlight).unwrap();
        fs::set_permissions(&spotlight, fs::Permissions::from_mode(0o000)).unwrap();

        let result = scan_retired_layout_paths(&store, &store.join("mnt"));
        fs::set_permissions(&spotlight, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            result.unwrap().is_empty(),
            "an unrelated APFS system directory must not become a retired-layout scan failure"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
