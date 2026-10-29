//! mbx, the Cargo compiler cache (03_caches.md, "mbx"): where the host's mbx lives once
//! `cowshed setup` installs cowshed's pin beside the installed cowshed (14_nix.md, "mbx"), the
//! one decision over that link, and the mbx capability (15_capabilities.md, "mbx detection").
//! The host's `mbx` is a symlink to the rooted executable, never a copied inode, so its realpath
//! is the pinned build's store path, and a project's own mbx is proven against it by realpath
//! equality, derived at every admission and never stored (05_gateway.md, "Pool selection and
//! revision").

use std::io;
use std::path::{Path, PathBuf};

use super::{
    CapabilityContribution, CapabilityGrant, CapabilityId, DetectionContext, DetectionScope,
    Detector, EnvAction, GrantAccess, GrantScope, MarkerKind, ReachedConvention,
};
use crate::{CowshedError, Result};

/// The lock of the flake cowshed's package ships at `nix/mbx`: the one record of the pinned
/// revision, read rather than restated.
const PIN_LOCK: &str = include_str!("../../../../nix/mbx/flake.lock");

/// The mbx revision cowshed pins, as its flake lock records it.
pub fn pinned_revision() -> Option<String> {
    serde_json::from_str::<serde_json::Value>(PIN_LOCK).ok()?["nodes"]["mbx"]["locked"]["rev"]
        .as_str()
        .map(str::to_owned)
}

/// The executable's name, in the pinned package's `bin` and beside the installed cowshed.
pub const BINARY_NAME: &str = "mbx";

/// The nix GC root `cowshed setup` builds the pinned mbx into (`nix build --out-link`): the
/// indirect root that keeps the store path the host's link names.
pub fn gc_root(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed/nix/mbx")
}

/// The host's `mbx`, beside the installed cowshed: a symlink to the rooted executable.
pub fn host_link(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed/bin")
        .join(BINARY_NAME)
}

/// What the host has at [`host_link`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkFound {
    Absent,
    /// A symlink, and the path it names, as written.
    Symlink(PathBuf),
    /// A file, a directory or anything else that is not a link: not cowshed's.
    Other,
}

/// What an install does to [`host_link`] for the rooted `program`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkPlan {
    /// The link already names the program.
    Current,
    Create,
    /// The link names another build (an earlier pin, or a store path since collected).
    Retarget {
        from: PathBuf,
    },
    /// Something that is not a link is there; cowshed removes nothing it did not make.
    Refuse,
}

pub fn plan_link(found: &LinkFound, program: &Path) -> LinkPlan {
    match found {
        LinkFound::Absent => LinkPlan::Create,
        LinkFound::Symlink(target) if target == program => LinkPlan::Current,
        LinkFound::Symlink(target) => LinkPlan::Retarget {
            from: target.clone(),
        },
        LinkFound::Other => LinkPlan::Refuse,
    }
}

/// The host's link as it is, never following it: a link to a collected store path is still a link.
pub fn observe_link(path: &Path) -> io::Result<LinkFound> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            std::fs::read_link(path).map(LinkFound::Symlink)
        }
        Ok(_) => Ok(LinkFound::Other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(LinkFound::Absent),
        Err(error) => Err(error),
    }
}

/// The host's installed mbx as its link resolves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostMbx {
    Absent,
    /// The link resolves: the executable's realpath.
    Installed(PathBuf),
    /// A link is there and does not resolve (a collected store path), or cannot be read: an
    /// admission failure, never absence.
    Unresolved {
        link: PathBuf,
        cause: String,
    },
}

pub fn observe_host(home: &Path) -> HostMbx {
    let link = host_link(home);
    match std::fs::canonicalize(&link) {
        Ok(executable) => HostMbx::Installed(executable),
        Err(error) => match std::fs::symlink_metadata(&link) {
            Err(absent) if absent.kind() == io::ErrorKind::NotFound => HostMbx::Absent,
            _ => HostMbx::Unresolved {
                link,
                cause: error.to_string(),
            },
        },
    }
}

/// The installed mbx's host-global scheduler settings, each `None` where it is unset and mbx
/// computes its default: read through the installed mbx's own settings interface, never copied
/// into a cowshed configuration (11_shell.md, "Canonical mbx environment").
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HostScheduler {
    pub cpus: Option<String>,
    pub reserve_cpus: Option<String>,
    pub memory: Option<String>,
}

impl HostScheduler {
    /// `scheduler.memory = "none"`: the pool keeps no measured-memory ledger, which mbx-mode
    /// admission refuses (05_gateway.md, "Pool selection and revision").
    pub fn memory_unscheduled(&self) -> bool {
        self.memory.as_deref() == Some("none")
    }
}

/// Ask the installed mbx at `executable` for its host-global scheduler settings, with nothing of
/// this process's environment but `home`: a job's or a caller's `MBX_*` must not stand in for
/// the host's file.
pub fn read_host_scheduler(executable: &Path, home: &Path) -> Result<HostScheduler> {
    use cowshed_os::fork_lock::Run as _;
    let get = |key: &str| -> Result<Option<String>> {
        // WHY unbounded: `mbx settings get` reads one TOML file and prints; it waits on nothing.
        #[allow(clippy::disallowed_methods)]
        let output = std::process::Command::new(executable)
            .env_clear()
            .env("HOME", home)
            .args(["settings", "get", key])
            .output_locked()
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "mbx-host-missing: the mbx installed beside cowshed, {}, cannot be executed: {error}",
                        executable.display()
                    ),
                    "cowshed setup",
                )
            })?;
        if !output.status.success() {
            return Err(CowshedError::environment_missing(
                format!(
                    "the mbx installed beside cowshed, {}, could not read {key}: {}",
                    executable.display(),
                    String::from_utf8_lossy(&output.stderr).trim_end()
                ),
                "repair mbx's global configuration with mbx settings",
            ));
        }
        let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        Ok((!value.is_empty()).then_some(value))
    };
    Ok(HostScheduler {
        cpus: get("scheduler.cpus")?,
        reserve_cpus: get("scheduler.reserve_cpus")?,
        memory: get("scheduler.memory")?,
    })
}

/// Whether work of a project whose canonical environment resolved mbx may run on this host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Admission {
    Admit,
    /// No mbx is installed beside cowshed (`mbx-host-missing`).
    HostMissing,
    /// The installed link does not resolve (`mbx-host-missing`, naming why).
    HostUnresolved {
        link: PathBuf,
        cause: String,
    },
    /// The project's mbx is another build than the host's (`mbx-revision-skew`).
    Skew {
        host: PathBuf,
    },
    /// The host's `scheduler.memory` is `"none"` (`mbx-memory-unscheduled`).
    MemoryUnscheduled,
    /// No provisioned build-cache volume holds the store (01a_apfs.md, "Dedicated build-cache
    /// volume").
    NoBuildCache,
}

/// The verdict for `project`, the realpath of the mbx the canonical environment resolved.
pub fn admit(project: &Path, host: &HostMbx, build_cache: Option<&Path>) -> Admission {
    match host {
        HostMbx::Absent => Admission::HostMissing,
        HostMbx::Unresolved { link, cause } => Admission::HostUnresolved {
            link: link.clone(),
            cause: cause.clone(),
        },
        HostMbx::Installed(host) if host != project => Admission::Skew { host: host.clone() },
        HostMbx::Installed(_) if build_cache.is_none() => Admission::NoBuildCache,
        HostMbx::Installed(_) => Admission::Admit,
    }
}

impl Admission {
    /// The finding code doctor and admission name this verdict by, or `None` when it admits.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Admit => None,
            Self::HostMissing | Self::HostUnresolved { .. } => Some("mbx-host-missing"),
            Self::Skew { .. } => Some("mbx-revision-skew"),
            Self::MemoryUnscheduled => Some("mbx-memory-unscheduled"),
            Self::NoBuildCache => Some("mbx-build-cache-missing"),
        }
    }

    /// The refusal a job of the project gets, or `None` when it is admitted. No arm selects
    /// another pool or falls back to cowshed's own ledger.
    pub fn refusal(&self, project: &Path) -> Option<CowshedError> {
        let pinned = pinned_revision().unwrap_or_else(|| "unknown".to_owned());
        match self {
            Self::Admit => None,
            Self::HostMissing => Some(CowshedError::environment_missing(
                format!(
                    "mbx-host-missing: this project's canonical environment resolves mbx at {}, and no mbx is installed beside cowshed",
                    project.display()
                ),
                "cowshed setup",
            )),
            Self::HostUnresolved { link, cause } => Some(CowshedError::environment_missing(
                format!(
                    "mbx-host-missing: this project's canonical environment resolves mbx at {}, and the mbx installed beside cowshed, {}, does not resolve: {cause}",
                    project.display(),
                    link.display()
                ),
                "cowshed setup",
            )),
            Self::Skew { host } => Some(CowshedError::conflict(
                format!(
                    "mbx-revision-skew: this project's canonical environment resolves mbx at {}, which is not the mbx installed beside cowshed at {} (cowshed pins revision {pinned})",
                    project.display(),
                    host.display()
                ),
                "align the project's devenv mbx pin with the revision cowshed pins, or disable the mbx capability",
            )),
            Self::MemoryUnscheduled => Some(CowshedError::environment_missing(
                format!(
                    "mbx-memory-unscheduled: the mbx installed beside cowshed has scheduler.memory = \"none\", so its pool keeps no memory ledger for {}",
                    project.display()
                ),
                "set a memory budget: mbx settings set scheduler.memory <size>, or unset it for mbx's default",
            )),
            Self::NoBuildCache => Some(CowshedError::environment_missing(
                format!(
                    "mbx-build-cache-missing: this project's canonical environment resolves mbx at {}, and this host has no provisioned build-cache volume for its store",
                    project.display()
                ),
                "cowshed setup",
            )),
        }
    }
}

/// The realpath of the mbx the checkout's last build-state discovery resolved in its canonical
/// job environment, as the checkout's build volume records it; `None` when it resolved none or
/// the checkout has no build volume yet.
pub fn recorded(checkout: &Path) -> Result<Option<PathBuf>> {
    let volume = checkout.join(crate::build_volume::BUILD_LINK);
    match std::fs::symlink_metadata(&volume) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!("cannot inspect {}: {error}", volume.display()),
                "attach the workspace so its build volume is mounted",
            ));
        }
    }
    Ok(crate::build_volume::BuildVolumeState::read_optional(&volume)?.and_then(|state| state.mbx))
}

/// Admit or refuse work of `checkout` on this host: a checkout whose canonical environment
/// resolved no mbx is admitted as it always was.
pub fn admit_checkout(checkout: &Path, home: &Path, build_cache: Option<&Path>) -> Result<()> {
    match checkout_verdict(checkout, home, build_cache)? {
        Some((project, verdict)) => match verdict.refusal(&project) {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        },
        None => Ok(()),
    }
}

/// The verdict for `checkout`'s jobs beside the realpath of the mbx its discovery recorded, or
/// `None` for a checkout whose canonical environment resolves no mbx: what admission refuses
/// with and doctor reports (05_gateway.md, "Pool selection and revision").
pub fn checkout_verdict(
    checkout: &Path,
    home: &Path,
    build_cache: Option<&Path>,
) -> Result<Option<(PathBuf, Admission)>> {
    let Some(project) = recorded(checkout)? else {
        return Ok(None);
    };
    let host = observe_host(home);
    let mut verdict = admit(&project, &host, build_cache);
    if verdict == Admission::Admit
        && let HostMbx::Installed(executable) = &host
        && read_host_scheduler(executable, home)?.memory_unscheduled()
    {
        verdict = Admission::MemoryUnscheduled;
    }
    Ok(Some((project, verdict)))
}

/// Host shells source the same canonical mbx environment as supervisor children (11_shell.md,
/// "Canonical mbx environment"). `RUSTC_WRAPPER` is Cargo's to withhold.
pub const HOST_ENVIRONMENT: [&str; 14] = [
    "MBX_CACHE_DIR",
    "MBX_TARGET_VIEWS",
    "MBX_TARGET_ROOT",
    "MBX_DISABLE",
    "MBX_INCREMENTAL",
    "RUSTC_WORKSPACE_WRAPPER",
    "MBX_SCHEDULER",
    "MBX_SCHEDULER_PRESSURE",
    "MBX_SCHEDULER_SUSPEND",
    "MBX_SCHEDULER_TESTS",
    "MBX_SCHEDULER_PRIORITY",
    "MBX_SCHEDULER_CPUS",
    "MBX_SCHEDULER_RESERVE_CPUS",
    "MBX_SCHEDULER_MEMORY",
];

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Mbx,
    marker_kind: MarkerKind::File,
    scope: DetectionScope::Project,
    all: &[],
    any: &["Cargo.toml"],
    contribute,
    reached_from: Some(ReachedConvention::TrackedManifest("Cargo.toml")),
    gate: Some(resolved),
};

fn resolved(context: &DetectionContext<'_>) -> Result<bool> {
    Ok(recorded(context.workspace_root)?.is_some())
}

/// The canonical mbx environment (11_shell.md, "Canonical mbx environment") and the store grant
/// (04_sandbox.md): the cache directory on the build-cache volume, managed targets at mbx's
/// default inside it, no wrapper beside mbx, and the inherited `target` link discarded at mint so
/// the fork's first build places its own (16_build_volumes.md, "Fork" step 4).
fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    if let Some(root) = context.build_cache {
        let store = root.join(BINARY_NAME);
        contribution.env.insert(
            "MBX_CACHE_DIR",
            EnvAction::Own(store.clone().into_os_string()),
        );
        contribution.grants.extend([
            CapabilityGrant {
                path: store,
                scope: GrantScope::Subtree,
                access: GrantAccess::ReadWrite,
            },
            CapabilityGrant {
                path: root.to_owned(),
                scope: GrantScope::Literal,
                access: GrantAccess::ReadMetadata,
            },
            CapabilityGrant {
                path: root.join(crate::storage::bootstrap::VOLUME_MARKER_FILE),
                scope: GrantScope::Literal,
                access: GrantAccess::Read,
            },
        ]);
    }
    contribution
        .env
        .insert("MBX_TARGET_VIEWS", EnvAction::Own("1".into()));
    // The one pool (05_gateway.md, "Host scheduling"), at the host's own settings, which a
    // project's `.mbx.toml` or a caller cannot change; never freezing a process. A job's
    // priority is its class's, set at its spawn; outside a job it is the non-gate value.
    for (name, value) in [
        ("MBX_SCHEDULER", "1"),
        ("MBX_SCHEDULER_PRESSURE", "1"),
        ("MBX_SCHEDULER_SUSPEND", "0"),
        ("MBX_SCHEDULER_TESTS", "1"),
        ("MBX_SCHEDULER_PRIORITY", "low"),
    ] {
        contribution.env.insert(name, EnvAction::Own(value.into()));
    }
    let host = match observe_host(context.home) {
        HostMbx::Installed(executable) => {
            read_host_scheduler(&executable, context.home).unwrap_or_default()
        }
        HostMbx::Absent | HostMbx::Unresolved { .. } => HostScheduler::default(),
    };
    for (name, value) in [
        ("MBX_SCHEDULER_CPUS", host.cpus),
        ("MBX_SCHEDULER_RESERVE_CPUS", host.reserve_cpus),
        ("MBX_SCHEDULER_MEMORY", host.memory),
    ] {
        contribution.env.insert(
            name,
            value.map_or(EnvAction::Unset, |value| EnvAction::Own(value.into())),
        );
    }
    for unset in [
        "MBX_TARGET_ROOT",
        "MBX_DISABLE",
        "MBX_INCREMENTAL",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
    ] {
        contribution.env.insert(unset, EnvAction::Unset);
    }
    let project = context
        .project_root
        .strip_prefix(context.workspace_root)
        .unwrap_or_else(|_| Path::new(""));
    contribution
        .daemon_isolation
        .discard_at_mint
        .push(project.join("target"));
    Ok(contribution)
}

/// The host-global mbx list that names checkouts whose managed targets mbx never collects for
/// age or size (03_caches.md, "mbx"): each adopted project's main.
pub const TARGET_KEEP: &str = "target.keep";
/// The host-global mbx list whose checkouts' targets go first when the targets are over budget:
/// each adopted project's shed mount tree.
pub const TARGET_EVICT_FIRST: &str = "target.evict_first";

/// A list setting as `mbx settings get` prints it: nothing when unset, else a TOML array of
/// strings. Anything else is the text itself as the error.
pub fn parse_setting_list(printed: &str) -> std::result::Result<Vec<String>, String> {
    #[derive(serde::Deserialize)]
    struct Printed {
        list: Vec<String>,
    }
    let text = printed.trim();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    toml::from_str::<Printed>(&format!("list = {text}"))
        .map(|printed| printed.list)
        .map_err(|_| text.to_owned())
}

/// The list `current` with every `required` path in it: the operator's entries kept, in their
/// order, and each missing path appended after them. `None` when every path is already there,
/// so nothing is written. A path mbx's comma-separated list input cannot carry is the error.
pub fn with_required(
    current: &[String],
    required: &[PathBuf],
) -> std::result::Result<Option<Vec<String>>, String> {
    let mut list = current.to_vec();
    for path in required {
        let Some(text) = path.to_str().filter(|text| !text.contains(',')) else {
            return Err(path.display().to_string());
        };
        if !list.iter().any(|entry| entry == text) {
            list.push(text.to_owned());
        }
    }
    Ok((list.len() != current.len()).then_some(list))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_printed_list_setting_is_unset_or_a_toml_array_of_strings() {
        for (printed, parsed) in [
            ("", Ok(vec![])),
            ("\n", Ok(vec![])),
            ("[]\n", Ok(vec![])),
            (
                "[\"/a b\", \"/c\"]\n",
                Ok(vec!["/a b".to_owned(), "/c".to_owned()]),
            ),
            ("\"/a\"\n", Err("\"/a\"".to_owned())),
            ("[1]\n", Err("[1]".to_owned())),
        ] {
            assert_eq!(parse_setting_list(printed), parsed, "{printed:?}");
        }
    }

    /// Setup and adopt add each required path once and keep what the operator listed, in its
    /// order; a list that already names every path is not written again.
    #[test]
    fn a_required_path_is_appended_once_after_the_operators_entries() {
        let main = PathBuf::from("/home/dev/src/widget");
        let sheds = PathBuf::from("/mnt/acme/widget");
        let own = |items: &[&str]| {
            items
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>()
        };
        for (current, required, written) in [
            (
                own(&[]),
                vec![main.clone()],
                Ok(Some(own(&["/home/dev/src/widget"]))),
            ),
            (
                own(&["~/work"]),
                vec![main.clone(), sheds.clone()],
                Ok(Some(own(&[
                    "~/work",
                    "/home/dev/src/widget",
                    "/mnt/acme/widget",
                ]))),
            ),
            (own(&["/home/dev/src/widget"]), vec![main.clone()], Ok(None)),
            (own(&["/x"]), vec![], Ok(None)),
            (
                own(&[]),
                vec![PathBuf::from("/home/dev/a,b")],
                Err("/home/dev/a,b".to_owned()),
            ),
        ] {
            assert_eq!(
                with_required(&current, &required),
                written,
                "{current:?} {required:?}"
            );
        }
    }

    #[test]
    fn every_refusing_verdict_has_the_code_its_message_leads_with() {
        let project = Path::new("/pinned/mbx-1.28.0/bin/mbx");
        for (verdict, code) in [
            (Admission::Admit, None),
            (Admission::HostMissing, Some("mbx-host-missing")),
            (
                Admission::HostUnresolved {
                    link: PathBuf::from("/home/dev/bin/mbx"),
                    cause: "gone".to_owned(),
                },
                Some("mbx-host-missing"),
            ),
            (
                Admission::Skew {
                    host: PathBuf::from("/pinned/mbx-1.27.0/bin/mbx"),
                },
                Some("mbx-revision-skew"),
            ),
            (Admission::MemoryUnscheduled, Some("mbx-memory-unscheduled")),
            (Admission::NoBuildCache, Some("mbx-build-cache-missing")),
        ] {
            assert_eq!(verdict.code(), code, "{verdict:?}");
            assert_eq!(
                verdict.refusal(project).map(|refusal| refusal
                    .message
                    .split(':')
                    .next()
                    .unwrap()
                    .to_owned()),
                code.map(str::to_owned),
                "{verdict:?}"
            );
        }
    }

    #[test]
    fn a_project_mbx_is_admitted_only_as_the_host_pin_itself() {
        let pin = Path::new("/pinned/mbx-1.28.0/bin/mbx");
        let other = PathBuf::from("/pinned/mbx-1.27.0/bin/mbx");
        let link = PathBuf::from("/home/dev/bin/mbx");
        let bc = Some(Path::new("/build-cache"));
        for (host, build_cache, expected) in [
            (HostMbx::Absent, bc, Admission::HostMissing),
            (
                HostMbx::Unresolved {
                    link: link.clone(),
                    cause: "gone".into(),
                },
                bc,
                Admission::HostUnresolved {
                    link: link.clone(),
                    cause: "gone".into(),
                },
            ),
            (
                HostMbx::Installed(other.clone()),
                bc,
                Admission::Skew {
                    host: other.clone(),
                },
            ),
            (
                HostMbx::Installed(pin.to_owned()),
                None,
                Admission::NoBuildCache,
            ),
            (HostMbx::Installed(pin.to_owned()), bc, Admission::Admit),
        ] {
            let verdict = admit(pin, &host, build_cache);
            assert_eq!(verdict, expected, "{host:?}");
            let refusal = verdict.refusal(pin);
            assert_eq!(refusal.is_none(), expected == Admission::Admit);
            if let Some(refusal) = refusal {
                assert!(
                    refusal.message.contains(&pin.display().to_string()),
                    "{}",
                    refusal.message
                );
            }
        }
        let skew = Admission::Skew {
            host: other.clone(),
        }
        .refusal(pin)
        .unwrap();
        assert!(
            skew.message.starts_with("mbx-revision-skew"),
            "{}",
            skew.message
        );
        assert!(
            skew.message.contains(&other.display().to_string()),
            "{}",
            skew.message
        );
        assert!(
            skew.message
                .contains(&pinned_revision().expect("the shipped lock names a revision")),
            "{}",
            skew.message
        );
        assert!(
            Admission::HostMissing
                .refusal(pin)
                .unwrap()
                .message
                .starts_with("mbx-host-missing")
        );
    }

    /// mbx is active exactly where the checkout's build volume records the mbx its canonical
    /// environment resolved; it names the store on the build-cache volume, sets no wrapper beside
    /// mbx, grants the store's subtree alone, and discards the inherited `target` link at mint.
    #[test]
    fn mbx_is_detected_from_the_recorded_resolution_and_contributes_the_canonical_environment() {
        use super::super::test_support::Fixture;
        let fixture = Fixture::new();
        fixture.files(&["Cargo.toml"]);
        assert!(
            DETECTOR.detect(&fixture.context()).unwrap().is_none(),
            "nothing recorded"
        );
        let volume = fixture.root.join("volume");
        std::fs::create_dir_all(&volume).unwrap();
        std::fs::create_dir_all(fixture.root.join(".cowshed")).unwrap();
        std::os::unix::fs::symlink(&volume, fixture.root.join(crate::build_volume::BUILD_LINK))
            .unwrap();
        crate::build_volume::BuildVolumeState::default()
            .write(&volume)
            .unwrap();
        assert!(
            DETECTOR.detect(&fixture.context()).unwrap().is_none(),
            "no mbx resolved"
        );
        let pin = PathBuf::from("/pinned/mbx-1.28.0/bin/mbx");
        crate::build_volume::BuildVolumeState {
            mbx: Some(pin.clone()),
            ..Default::default()
        }
        .write(&volume)
        .unwrap();
        assert_eq!(recorded(&fixture.root).unwrap(), Some(pin));
        let build_cache = fixture.root.join("build-cache");
        let context = DetectionContext {
            build_cache: Some(&build_cache),
            ..fixture.context()
        };
        let contribution = DETECTOR.detect(&context).unwrap().expect("mbx detected");
        assert_eq!(
            contribution.env.get("MBX_CACHE_DIR"),
            Some(&EnvAction::Own(build_cache.join("mbx").into_os_string()))
        );
        for unset in [
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "MBX_TARGET_ROOT",
            "MBX_DISABLE",
        ] {
            assert_eq!(
                contribution.env.get(unset),
                Some(&EnvAction::Unset),
                "{unset}"
            );
        }
        assert!(contribution.grants.contains(&CapabilityGrant {
            path: build_cache.join("mbx"),
            scope: GrantScope::Subtree,
            access: GrantAccess::ReadWrite,
        }));
        assert!(
            contribution
                .grants
                .iter()
                .filter(|grant| grant.access != GrantAccess::ReadMetadata)
                .all(|grant| grant.path.starts_with(build_cache.join("mbx"))
                    || grant.path
                        == build_cache.join(crate::storage::bootstrap::VOLUME_MARKER_FILE)),
            "{:?}",
            contribution.grants
        );
        assert_eq!(
            contribution.daemon_isolation.discard_at_mint,
            [PathBuf::from("target")]
        );
    }

    /// The host's scheduler settings come from the installed mbx's own settings interface, with
    /// none of this process's environment but HOME, and an unset one stays unset; `"none"` memory
    /// is the unscheduled finding.
    #[test]
    fn the_host_scheduler_is_read_through_the_installed_mbx_alone() {
        use std::os::unix::fs::PermissionsExt as _;
        let root =
            std::env::temp_dir().join(format!("cowshed-mbx-settings-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mbx = root.join("mbx");
        std::fs::write(
            &mbx,
            "#!/bin/sh\n[ -z \"$MBX_SCHEDULER_CPUS\" ] || exit 9\ncase \"$3\" in\nscheduler.cpus) echo 8 ;;\nscheduler.memory) echo none ;;\nesac\n",
        )
        .unwrap();
        std::fs::set_permissions(&mbx, std::fs::Permissions::from_mode(0o755)).unwrap();
        // SAFETY: this test alone names the variable, and only to prove it never reaches mbx.
        unsafe { std::env::set_var("MBX_SCHEDULER_CPUS", "99") };
        let read = read_host_scheduler(&mbx, &root).unwrap();
        unsafe { std::env::remove_var("MBX_SCHEDULER_CPUS") };
        assert_eq!(
            read,
            HostScheduler {
                cpus: Some("8".into()),
                reserve_cpus: None,
                memory: Some("none".into()),
            }
        );
        assert!(read.memory_unscheduled());
        assert!(!HostScheduler::default().memory_unscheduled());
        let refusal = Admission::MemoryUnscheduled
            .refusal(Path::new("/pinned/mbx-1.28.0/bin/mbx"))
            .unwrap();
        assert!(
            refusal.message.starts_with("mbx-memory-unscheduled"),
            "{}",
            refusal.message
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_pin_is_the_revision_the_shipped_lock_names() {
        assert_eq!(
            pinned_revision().as_deref(),
            Some("dca6f787a3029e0edbd126950e7b955701d39f4f")
        );
    }

    #[test]
    fn the_host_link_is_made_once_retargeted_to_a_new_pin_and_never_replaces_a_foreign_file() {
        let program = Path::new("/pinned/mbx-1.28.0/bin/mbx");
        let earlier = PathBuf::from("/pinned/mbx-1.27.0/bin/mbx");
        for (found, expected) in [
            (LinkFound::Absent, LinkPlan::Create),
            (LinkFound::Symlink(program.to_owned()), LinkPlan::Current),
            (
                LinkFound::Symlink(earlier.clone()),
                LinkPlan::Retarget { from: earlier },
            ),
            (LinkFound::Other, LinkPlan::Refuse),
        ] {
            assert_eq!(plan_link(&found, program), expected, "{found:?}");
        }
    }

    #[test]
    fn the_link_is_observed_without_following_it() {
        let root = std::env::temp_dir().join(format!("cowshed-mbx-link-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let link = root.join(BINARY_NAME);
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Absent);
        let collected = root.join("collected/bin/mbx");
        std::os::unix::fs::symlink(&collected, &link).unwrap();
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Symlink(collected));
        std::fs::remove_file(&link).unwrap();
        std::fs::write(&link, b"a copied inode").unwrap();
        assert_eq!(observe_link(&link).unwrap(), LinkFound::Other);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
