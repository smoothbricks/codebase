//! A workspace inherits the shell export of the checkout it was cloned from, when that export is
//! verified for this checkout (11_shell.md, "Shell activation and process reuse").
//!
//! A fork is an exact copy of its origin: the same inputs, already installed. What a fresh host
//! lacks is only the activated environment, and evaluating it again costs a cold devenv
//! evaluation (74.6 s measured on a devenv monorepo) to produce the origin's values with the
//! root moved. So
//! every successful activation leaves its export here, in the workspace's protected host
//! directory, and the clone carries it to the fork for free. A host with no live generation
//! adopts it instead of evaluating when, and only when:
//!
//! - the bytes of every input the activation recorded are the same at this checkout ([`key`]),
//! - the environment the export was computed against is the same up to the move ([`baseline`]),
//! - and no value still names the origin after the move.
//!
//! Anything else is a miss, which evaluates as before; a miss says why ([`AdoptionMiss`]).
//!
//! The record carries no credential and nothing the checkout owns per workspace: those values
//! come from the checkout's own sourced files ([`TAIL_FILES`]), read through direnv exactly as an
//! activation reads them, so an adopted environment and an evaluated one obtain them one way.
//! Without that filter every fork would re-copy every ancestor's secrets, multiplied by the
//! number of workspaces and the depth of their lineage. This is non-propagation, not isolation:
//! the files the values come from are in the image either way.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::{self, Read as _};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use super::shell_host::SHELL_HOST_DIRECTORY;
use super::shell_watch::{
    ActivationEvidence, CertifyError, Content, Contents, Snapshot, WatchEntry, certify,
    encode_direnv_watches,
};

/// The record's file in the workspace's host directory, which every job profile denies writes to.
pub const RECORD_FILE: &str = "activation.json";

/// The fence file every writer of [`RECORD_FILE`] locks ([`lock_record`]).
const RECORD_LOCK_FILE: &str = "activation.lock";

const RECORD_VERSION: u32 = 1;

/// A record is one export plus its watch list; a larger file is not one this code wrote.
const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

/// What an activation sources from the checkout itself after `use devenv`, in the managed
/// `.envrc`'s order: cowshed's published workspace environment, then the gitignored local
/// additions. Only the ones the activation's own watch list names are sourced on adoption.
pub const TAIL_FILES: [&str; 2] = [".cowshed/env", ".envrc-local"];

/// devenv's runtime directory is `<XDG_RUNTIME_DIR>/devenv-<first 7 hex of sha256(dotfile)>`
/// (devenv 2.4 `devenv-core/src/paths.rs`, `resolve_runtime_dir`): a hash of the root, which no
/// rebase reaches, so it is recomputed from the moved dotfile.
const DEVENV_RUNTIME: &str = "DEVENV_RUNTIME";
const DEVENV_DOTFILE: &str = "DEVENV_DOTFILE";
const XDG_RUNTIME_DIR: &str = "XDG_RUNTIME_DIR";

/// Why a value is not carried in the record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Withheld {
    /// direnv's own state, rebuilt for the adopting checkout by the host.
    Rebuilt,
    /// Not an environment variable a tool reads: the parent's task file runs scripts built
    /// with the parent's root, and a shell hook is entry logic.
    NotEnvironment,
    /// A credential, or a value carrying one. The adopting checkout's own files supply it.
    Credential,
    /// A value cowshed publishes per workspace in `.cowshed/env`; the adopting checkout's own
    /// copy supplies it.
    WorkspaceOwned,
}

impl Withheld {
    /// Whether the adopting host takes this name from its sourced tail files.
    pub fn from_tail(self) -> bool {
        matches!(self, Self::Credential | Self::WorkspaceOwned)
    }
}

const PROXY_NAMES: [&str; 6] = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
];

/// Token shapes the secret scanner already recognises (02_workspaces.md, "Detection").
const TOKEN_PREFIXES: [&str; 5] = ["ghp_", "xoxb-", "sk-", "AKIA", "-----BEGIN"];

fn workspace_owned(name: &str) -> bool {
    use crate::workspace_environment::{
        NX_REMOTE_SERVER_ENV, PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, RUNTIME_LINK_ENV, TEMP_DIR_ENV,
    };
    [
        PORT_BASE_ENV,
        PORT_BLOCK_SIZE_ENV,
        NX_REMOTE_SERVER_ENV,
        TEMP_DIR_ENV,
        RUNTIME_LINK_ENV,
        "NX_MAX_CACHE_SIZE",
        "CI",
    ]
    .contains(&name)
        || crate::capabilities::sccache::HOST_ENVIRONMENT.contains(&name)
}

fn credential(name: &str, value: Option<&str>) -> bool {
    use crate::workspace_environment::{NX_REMOTE_TOKEN_ENV, WORKSPACE_TOKEN_ENV};
    let upper = name.to_ascii_uppercase();
    name == WORKSPACE_TOKEN_ENV
        || name == NX_REMOTE_TOKEN_ENV
        || PROXY_NAMES.contains(&name)
        || ["_TOKEN", "_SECRET", "_KEY"]
            .iter()
            .any(|suffix| upper.ends_with(suffix))
        || value.is_some_and(|value| {
            TOKEN_PREFIXES.iter().any(|prefix| value.contains(prefix)) || carries_userinfo(value)
        })
}

/// `scheme://user[:password]@host`: a URL whose authority carries credentials.
fn carries_userinfo(value: &str) -> bool {
    value.match_indices("://").any(|(at, _)| {
        let authority = &value[at + 3..];
        let end = authority
            .find(|c: char| matches!(c, '/' | '?' | '#') || c.is_whitespace())
            .unwrap_or(authority.len());
        authority[..end].contains('@')
    })
}

/// Whether, and why, a variable of an export is not carried. A removal carries no value, so
/// only direnv's own rebuilt state is withheld when the value is `None`.
pub fn withheld(name: &str, value: Option<&str>) -> Option<Withheld> {
    if matches!(name, "DIRENV_WATCHES" | "DIRENV_DIFF") {
        Some(Withheld::Rebuilt)
    } else if value.is_none() {
        None
    } else if matches!(name, "DEVENV_TASK_FILE" | "shellHook") {
        Some(Withheld::NotEnvironment)
    } else if credential(name, value) {
        Some(Withheld::Credential)
    } else if workspace_owned(name) {
        Some(Withheld::WorkspaceOwned)
    } else {
        None
    }
}

/// One input of the recorded activation, named relative to the checkout when it lies inside.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct RecordedWatch {
    path: PathBuf,
    inside: bool,
    exists: bool,
}

/// The file as written. Read only through [`verify`].
#[derive(serde::Serialize, serde::Deserialize)]
struct ActivationRecord {
    v: u32,
    /// The activating checkout's [`Placement::places`], in their order.
    places: [Option<PathBuf>; PLACES],
    ports: Option<(u32, u32)>,
    key: String,
    baseline: String,
    watches: Vec<RecordedWatch>,
    /// direnv's export minus every withheld name: for each variable the activation changed, its
    /// complete value, or `null` for a removal, relative to the host's baseline.
    environment: BTreeMap<String, Option<String>>,
    /// The withheld names the export set that the adopting checkout's tail supplies. Names only;
    /// no value of a credential is ever written.
    from_tail: BTreeSet<String>,
    /// The certification of this input generation's producer: the checkout roots whose own
    /// evaluations of these input bytes, over equivalent baselines, gave exports equal after the
    /// move. Fewer than two distinct roots is no certification: the record then serves only the
    /// checkout it was written in.
    #[serde(default)]
    proof: Vec<PathBuf>,
}

/// Where a checkout's record lives.
pub fn record_path(mount: &Path) -> PathBuf {
    mount.join(SHELL_HOST_DIRECTORY).join(RECORD_FILE)
}

const PLACES: usize = 4;

/// Where a checkout is, and the per-workspace facts its host's baseline names.
#[derive(Clone, Debug)]
pub struct Placement<'a> {
    pub root: &'a Path,
    /// The baseline's `XDG_RUNTIME_DIR`: cowshed's short runtime link, named after the mount.
    pub runtime: Option<&'a Path>,
    /// The baseline's `TMPDIR`, which cowshed names after the workspace.
    pub temp_dir: Option<&'a Path>,
    /// The baseline's `HOME`, the workspace's private home.
    pub home: Option<&'a Path>,
    /// The workspace's port block, `[base, base + size)`.
    pub ports: Option<(u32, u32)>,
}

impl<'a> Placement<'a> {
    pub fn of(root: &'a Path, baseline: &'a BTreeMap<OsString, OsString>) -> Self {
        use crate::workspace_environment::{PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, TEMP_DIR_ENV};
        let get = |name: &str| baseline.get(std::ffi::OsStr::new(name));
        let number = |name: &str| get(name)?.to_str()?.parse::<u32>().ok();
        Self {
            root,
            runtime: get(XDG_RUNTIME_DIR).map(Path::new),
            temp_dir: get(TEMP_DIR_ENV).map(Path::new),
            home: get("HOME").map(Path::new),
            ports: number(PORT_BASE_ENV).zip(number(PORT_BLOCK_SIZE_ENV)),
        }
    }

    /// The per-workspace paths a value can name, in the order a move replaces them: the root
    /// first, so a home or runtime directory beneath it moves with it.
    fn places(&self) -> [Option<&'a Path>; PLACES] {
        [Some(self.root), self.runtime, self.temp_dir, self.home]
    }
}

/// Whether `value` holds a decimal number inside `[base, base + size)` as a whole token.
fn names_port(value: &str, (base, size): (u32, u32)) -> bool {
    value
        .split(|c: char| !c.is_ascii_digit())
        .filter_map(|token| token.parse::<u64>().ok())
        .any(|port| port >= u64::from(base) && port < u64::from(base) + u64::from(size))
}

fn is_component_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+' | b'@' | b'~')
}

/// Replace every occurrence of `from` that starts a path: not the tail of a longer path (walking
/// back over the bytes a component is made of reaches a `/`), and followed by the end, a `/`,
/// or a byte no path component continues with. `/x/repo` moves `/x/repo/tooling`,
/// `-I/x/repo/include` and `-Wl,-rpath,/x/repo/lib`, never `/x/repo-worktrees` or `/w/x/repo`.
pub fn rebase(value: &str, from: &str, to: &str) -> String {
    if from.is_empty() {
        return value.to_owned();
    }
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut copied = 0;
    for (at, _) in value.match_indices(from) {
        if at < copied {
            continue;
        }
        let inside_longer_path = bytes[..at]
            .iter()
            .rev()
            .find(|byte| !is_component_byte(**byte))
            == Some(&b'/');
        let end = at + from.len();
        if !inside_longer_path && ends_a_component(bytes, end) {
            out.push_str(&value[copied..at]);
            out.push_str(to);
            copied = end;
        }
    }
    out.push_str(&value[copied..]);
    out
}

fn ends_a_component(bytes: &[u8], end: usize) -> bool {
    end == bytes.len() || bytes[end] == b'/' || !is_component_byte(bytes[end])
}

/// Whether `value` still names `path` after a move. Stricter than [`rebase`] on purpose: any
/// occurrence that ends a component counts, wherever it starts, so a spelling the move did not
/// reach refuses adoption. A false refusal costs one evaluation; a false pass would hand the
/// clone the origin's tree.
fn contains_path(value: &str, path: &str) -> bool {
    !path.is_empty()
        && value
            .match_indices(path)
            .any(|(at, _)| ends_a_component(value.as_bytes(), at + path.len()))
}

fn relative(root: &Path, path: &Path) -> RecordedWatch {
    match path.strip_prefix(root) {
        Ok(rest) => RecordedWatch {
            path: rest.to_path_buf(),
            inside: true,
            exists: false,
        },
        Err(_) => RecordedWatch {
            path: path.to_path_buf(),
            inside: false,
            exists: false,
        },
    }
}

/// The cowshed-owned inputs whose bytes differ per workspace by construction: the published
/// environment (it holds the workspace token) and direnv's trust files, named by a hash of the
/// `.envrc`'s path. Their effect on the environment is the tail's, read at each checkout.
fn workspace_owned_input(watch: &RecordedWatch) -> bool {
    watch.inside
        && (watch.path == Path::new(".cowshed/env")
            || watch.path.starts_with(".cowshed/data/direnv/allow")
            || watch.path.starts_with(".cowshed/data/direnv/deny"))
}

/// The content key: SHA-256 over every recorded input except the cowshed-owned ones, ordered by
/// name, each as (inside the checkout, name, content). `contents` holds the certified bytes of
/// the inputs at `root` ([`certify`]); a recorded input missing from it is a missing file.
fn key(root: &Path, watches: &[RecordedWatch], contents: &Contents) -> String {
    let by_path: BTreeMap<&Path, Content> = contents.iter().collect();
    let mut ordered: Vec<&RecordedWatch> = watches
        .iter()
        .filter(|watch| !workspace_owned_input(watch))
        .collect();
    ordered.sort_by(|a, b| (a.inside, &a.path).cmp(&(b.inside, &b.path)));
    let mut hasher = Sha256::new();
    for watch in ordered {
        let path = if watch.inside {
            root.join(&watch.path)
        } else {
            watch.path.clone()
        };
        hasher.update([u8::from(watch.inside)]);
        hasher.update(watch.path.as_os_str().as_bytes());
        hasher.update([0]);
        match by_path
            .get(path.as_path())
            .copied()
            .unwrap_or(Content::Missing)
        {
            Content::Missing => hasher.update(b"-"),
            Content::Directory(digest) => {
                hasher.update(b"d");
                hasher.update(digest);
            }
            Content::File(digest) => {
                hasher.update(b"f");
                hasher.update(digest);
            }
        }
    }
    crate::api::dto::hex_lower(&hasher.finalize())
}

/// The baseline key: SHA-256 over the host's environment before activation, sorted, with every
/// per-workspace place replaced by a placeholder and every withheld name left out (a workspace
/// token or proxy URL differs per workspace by construction and is not an input the export was
/// computed from that adoption could carry).
fn baseline(placement: &Placement<'_>, environment: &BTreeMap<OsString, OsString>) -> String {
    let places = placement
        .places()
        .map(|place| place.map(Path::to_string_lossy));
    let mut hasher = Sha256::new();
    for (name, value) in environment {
        let (Some(name), Some(value)) = (name.to_str(), value.to_str()) else {
            // A non-UTF-8 variable is hashed as it is: it cannot be moved, so it must match.
            hasher.update(name.as_bytes());
            hasher.update([0]);
            hasher.update(value.as_bytes());
            hasher.update([0]);
            continue;
        };
        // Cowshed's managed git identity is a workspace's own file, and `/dev/null` in a main
        // that has none (`supervisor.rs`, `GIT_CONFIG_GLOBAL`): per workspace by construction,
        // and no shell evaluation's input.
        if withheld(name, Some(value)).is_some() || name == "GIT_CONFIG_GLOBAL" {
            continue;
        }
        let mut value = value.to_owned();
        for (index, place) in places.iter().enumerate() {
            if let Some(place) = place {
                value = rebase(&value, place, &format!("\0place{index}\0"));
            }
        }
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    crate::api::dto::hex_lower(&hasher.finalize())
}

/// Why a record could not be written.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("cannot encode the activation record: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("cannot publish the activation record: {0}")]
    Publish(#[from] crate::metadata::MetadataError),
    #[error("cannot lock the activation record {path:?}: {source}")]
    Lock { path: PathBuf, source: io::Error },
}

/// The fence every writer of a checkout's record holds across what it reads, decides and
/// writes: the activation that rewrites the record and a clone's certification of it. Without
/// it a certification could read a record, lose a race to a newer one, and write the old record
/// back with a proof it never earned. Released when the returned file closes.
fn lock_record(mount: &Path) -> Result<std::fs::File, RecordError> {
    let path = mount.join(SHELL_HOST_DIRECTORY).join(RECORD_LOCK_FILE);
    let locked = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .and_then(|file| file.lock().map(|()| file));
    locked.map_err(|source| RecordError::Lock { path, source })
}

/// Split an export into what a record carries and the withheld names its tail supplies.
fn carried(
    export: &BTreeMap<String, Option<String>>,
) -> (BTreeMap<String, Option<String>>, BTreeSet<String>) {
    let mut environment = BTreeMap::new();
    let mut from_tail = BTreeSet::new();
    for (name, value) in export {
        match withheld(name, value.as_deref()) {
            None => {
                environment.insert(name.clone(), value.clone());
            }
            Some(reason) if reason.from_tail() => {
                from_tail.insert(name.clone());
            }
            Some(_) => {}
        }
    }
    (environment, from_tail)
}

/// Why a record was written without trying to certify an inherited producer.
#[derive(Debug, Eq, PartialEq)]
pub enum NotAttempted {
    /// No uncertified record of another checkout was inherited.
    NothingInherited,
    /// The inputs this evaluation read are not the bytes the inherited record keyed (an
    /// evaluation that rewrote one of its own inputs, a lock file say).
    KeyMoved,
    /// The baseline this evaluation ran over is not the one the inherited record keyed.
    BaselineMoved,
}

/// What became of the origin's record when this evaluation certified its producer.
#[derive(Debug, Eq, PartialEq)]
pub enum Origin {
    /// The origin's record carries the proof now.
    Updated,
    /// The origin's record is no longer the one this checkout read; it stays as written.
    Changed,
    /// The origin's record could not be given the proof, and why: it is not this checkout's
    /// ancestor, a link redirects its host directory, it cannot be locked or written.
    Refused(String),
}

/// What writing a record did about its producer's certification.
#[derive(Debug, Eq, PartialEq)]
pub enum Certification {
    NotAttempted(NotAttempted),
    /// This evaluation agreed with the inherited export after the move: this record carries
    /// the proof, and `origin` says what became of the origin's.
    Certified {
        origin: Origin,
    },
    /// This evaluation disagreed with the inherited export after the move — a value the move
    /// cannot produce, such as a hash of the checkout path. Clones keep evaluating.
    Differs {
        name: String,
    },
}

/// Leave an activation's export for the checkouts cloned from this one. `export` is direnv's
/// export the host applied, `contents` the inputs' bytes certified when it finished, and
/// `baseline_environment` the host's environment before it. `inherited` is the verified but
/// uncertified record this checkout carried for the same input bytes, if any: this evaluation
/// is one more root its producer is measured at, and agreeing with it certifies this record
/// and then the origin's. This checkout's own record is written first; nothing the origin does
/// can cost it.
pub fn write_record(
    placement: &Placement<'_>,
    baseline_environment: &BTreeMap<OsString, OsString>,
    entries: &[WatchEntry],
    contents: &Contents,
    export: &BTreeMap<String, Option<String>>,
    inherited: Option<&Uncertified>,
) -> Result<Certification, RecordError> {
    let watches: Vec<RecordedWatch> = entries
        .iter()
        .map(|entry| RecordedWatch {
            exists: entry.exists,
            ..relative(placement.root, &entry.path)
        })
        .collect();
    let (environment, from_tail) = carried(export);
    let key = key(placement.root, &watches, contents);
    let baseline = baseline(placement, baseline_environment);
    let agreement = match inherited {
        None => Err(Certification::NotAttempted(NotAttempted::NothingInherited)),
        Some(inherited) if inherited.key != key => {
            Err(Certification::NotAttempted(NotAttempted::KeyMoved))
        }
        Some(inherited) if inherited.baseline != baseline => {
            Err(Certification::NotAttempted(NotAttempted::BaselineMoved))
        }
        Some(inherited) => match inherited.differs_from(&environment, &from_tail) {
            Some(name) => Err(Certification::Differs { name }),
            None => Ok(inherited),
        },
    };
    let proof = match &agreement {
        Ok(inherited) => inherited.proof_with(placement.root),
        Err(_) => Vec::new(),
    };
    let record = ActivationRecord {
        v: RECORD_VERSION,
        places: placement.places().map(|place| place.map(Path::to_path_buf)),
        ports: placement.ports,
        key,
        baseline,
        watches,
        environment,
        from_tail,
        proof,
    };
    {
        let _fence = lock_record(placement.root)?;
        crate::metadata::write_atomic_bytes(
            &record_path(placement.root),
            &serde_json::to_vec(&record)?,
        )?;
    }
    match agreement {
        Err(certification) => Ok(certification),
        // The fence above is released: no writer holds two.
        Ok(inherited) => Ok(Certification::Certified {
            origin: inherited.certify_origin(placement.root),
        }),
    }
}

/// A record of another checkout that verified for this one in every way but certification:
/// what this checkout's own evaluation must agree with to certify its producer.
#[derive(Debug)]
pub struct Uncertified {
    origin: PathBuf,
    /// SHA-256 of the origin's record file as it was read, so the origin's record gains the
    /// proof only if it is still that record.
    digest: [u8; 32],
    /// The roots the producer was already measured at; the origin alone if none.
    proof: Vec<PathBuf>,
    key: String,
    baseline: String,
    /// The origin's export moved to this checkout, without the rebuilt watch list.
    environment: BTreeMap<String, Option<String>>,
    from_tail: BTreeSet<String>,
}

impl Uncertified {
    /// The first variable on which this checkout's own export disagrees with the moved one.
    fn differs_from(
        &self,
        environment: &BTreeMap<String, Option<String>>,
        from_tail: &BTreeSet<String>,
    ) -> Option<String> {
        let names: BTreeSet<&String> = self.environment.keys().chain(environment.keys()).collect();
        names
            .into_iter()
            .find(|name| self.environment.get(*name) != environment.get(*name))
            .cloned()
            .or_else(|| {
                self.from_tail
                    .symmetric_difference(from_tail)
                    .next()
                    .cloned()
            })
    }

    /// The roots measured so far, the origin's first, with `root` added.
    fn proof_with(&self, root: &Path) -> Vec<PathBuf> {
        let mut proof = vec![self.origin.clone()];
        for measured in self.proof.iter().map(PathBuf::as_path).chain([root]) {
            if !proof.iter().any(|known| known == measured) {
                proof.push(measured.to_path_buf());
            }
        }
        proof
    }

    /// Give the origin's record the proof, if the origin is this checkout's ancestor and its
    /// record is still the one this checkout read. The origin is a path from bytes in this
    /// image, so it becomes a write target only once the controller's own markers say so: its
    /// incarnation is in this checkout's lineage, and no link redirects its host directory. The
    /// comparison and the write happen under the origin's record fence, so a newer record its
    /// own activation wrote in between is never overwritten.
    fn certify_origin(&self, root: &Path) -> Origin {
        use crate::metadata::WorkspaceMarker;
        use crate::storage::{WORKSPACE_MARKER_PATH, verify_no_symlinks};
        let refused = |reason: String| Origin::Refused(reason);
        let ours = match WorkspaceMarker::read_from(&root.join(WORKSPACE_MARKER_PATH)) {
            Ok(marker) => marker,
            Err(error) => return refused(format!("this checkout's marker is unreadable: {error}")),
        };
        let theirs = match WorkspaceMarker::read_from(&self.origin.join(WORKSPACE_MARKER_PATH)) {
            Ok(marker) => marker,
            Err(error) => return refused(format!("the origin's marker is unreadable: {error}")),
        };
        if !ours
            .lineage
            .as_ref()
            .is_some_and(|lineage| lineage.contains(&theirs.workspace_incarnation))
        {
            return refused(format!(
                "{} is not an ancestor of this checkout",
                self.origin.display()
            ));
        }
        if let Err(error) =
            verify_no_symlinks(&self.origin, &self.origin.join(SHELL_HOST_DIRECTORY))
        {
            return refused(format!(
                "the origin's host directory is not its own: {error}"
            ));
        }
        let path = record_path(&self.origin);
        let written = (|| -> Result<Origin, RecordError> {
            let _fence = lock_record(&self.origin)?;
            let Ok(bytes) = std::fs::read(&path) else {
                return Ok(Origin::Changed);
            };
            if <[u8; 32]>::from(Sha256::digest(&bytes)) != self.digest {
                return Ok(Origin::Changed);
            }
            let Ok(mut record) = serde_json::from_slice::<ActivationRecord>(&bytes) else {
                return Ok(Origin::Changed);
            };
            record.proof = self.proof_with(root);
            crate::metadata::write_atomic_bytes(&path, &serde_json::to_vec(&record)?)?;
            Ok(Origin::Updated)
        })();
        written.unwrap_or_else(|error| refused(error.to_string()))
    }
}

/// Why a host evaluates instead of adopting. Each is reported on the activating job's stderr.
/// `Uncertified` carries what a certifying evaluation compares with; it is not reported whole.
#[derive(Debug, thiserror::Error)]
pub enum AdoptionMiss {
    #[error("no inherited shell record")]
    Absent,
    #[error("the inherited shell record is unreadable: {0}")]
    Unreadable(io::Error),
    #[error("the inherited shell record is not one this cowshed writes: {0}")]
    Malformed(String),
    #[error("the inherited shell record is version {0}, not {RECORD_VERSION}")]
    Version(u32),
    #[error(transparent)]
    Inputs(#[from] CertifyError),
    #[error("the inherited shell's inputs differ from this checkout's")]
    KeyMoved,
    #[error("the inherited shell was computed against a different environment")]
    BaselineMoved,
    #[error("{name} still names the origin checkout after the move")]
    Residue { name: String },
    #[error("the inherited shell record carries {name}, which no record may")]
    Withheld { name: String },
    #[error(
        "the inherited shell's producer is not certified: no second checkout has evaluated these \
         inputs to the same export; this evaluation is that check"
    )]
    Uncertified(Box<Uncertified>),
}

impl AdoptionMiss {
    /// The record this checkout's evaluation can certify, when that is why adoption missed.
    pub fn into_uncertified(self) -> Option<Uncertified> {
        match self {
            Self::Uncertified(inherited) => Some(*inherited),
            _ => None,
        }
    }
}

/// An inherited export proven for this checkout. Built only by [`verify`].
#[derive(Debug)]
pub struct VerifiedActivation {
    environment: BTreeMap<String, Option<String>>,
    from_tail: BTreeSet<String>,
    tail: Vec<PathBuf>,
    entries: Vec<WatchEntry>,
    snapshot: Snapshot,
    contents: Contents,
}

impl VerifiedActivation {
    /// The moved export, with `DIRENV_WATCHES` rebuilt for this checkout.
    pub fn environment(&self) -> &BTreeMap<String, Option<String>> {
        &self.environment
    }
    /// Names the host takes from its tail; their values were never recorded.
    pub fn from_tail(&self) -> &BTreeSet<String> {
        &self.from_tail
    }
    /// The tail files, absolute, in the order the activation sourced them.
    pub fn tail(&self) -> &[PathBuf] {
        &self.tail
    }
    /// What the pool keeps for an adopted shell: no evaluation ran, so the identities certified
    /// with the bytes are both its before and its after.
    pub fn into_evidence(self) -> ActivationEvidence {
        ActivationEvidence {
            entries: self.entries,
            before: self.snapshot.clone(),
            clock: None,
            after: self.snapshot,
            contents: Some(self.contents),
        }
    }
}

/// The record and the SHA-256 of its file's bytes.
fn read_record(mount: &Path) -> Result<(ActivationRecord, [u8; 32]), AdoptionMiss> {
    let path = record_path(mount);
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(AdoptionMiss::Absent),
        Err(error) => return Err(AdoptionMiss::Unreadable(error)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(AdoptionMiss::Unreadable)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(AdoptionMiss::Malformed("larger than any record".into()));
    }
    let record: ActivationRecord = serde_json::from_slice(&bytes)
        .map_err(|error| AdoptionMiss::Malformed(error.to_string()))?;
    if record.v != RECORD_VERSION {
        return Err(AdoptionMiss::Version(record.v));
    }
    Ok((record, Sha256::digest(&bytes).into()))
}

/// Prove this checkout's inherited record, or say why it cannot serve.
pub fn verify(
    placement: &Placement<'_>,
    baseline_environment: &BTreeMap<OsString, OsString>,
) -> Result<VerifiedActivation, AdoptionMiss> {
    let (record, digest) = read_record(placement.root)?;
    if let Some((name, _)) = record
        .environment
        .iter()
        .find(|(name, value)| withheld(name, value.as_deref()).is_some())
    {
        return Err(AdoptionMiss::Withheld { name: name.clone() });
    }
    let absolute = |watch: &RecordedWatch| {
        if watch.inside {
            placement.root.join(&watch.path)
        } else {
            watch.path.clone()
        }
    };
    // Identities, bytes, identities: a write between the readings would let the identities the
    // pool keeps certify bytes nobody hashed ([`certify`]).
    let paths: Vec<PathBuf> = record.watches.iter().map(absolute).collect();
    let (snapshot, contents) = certify(paths.iter().map(PathBuf::as_path))?;
    let key = key(placement.root, &record.watches, &contents);
    if key != record.key {
        return Err(AdoptionMiss::KeyMoved);
    }
    if baseline(placement, baseline_environment) != record.baseline {
        return Err(AdoptionMiss::BaselineMoved);
    }
    // Each per-workspace place of the origin moves to this checkout's, root first, at a
    // path-component boundary.
    let moves: Vec<(String, Option<String>)> = record
        .places
        .iter()
        .zip(placement.places())
        .filter_map(|(from, to)| {
            let from = from.as_ref()?.to_string_lossy().into_owned();
            let to = to.map(|to| to.to_string_lossy().into_owned());
            (Some(&from) != to.as_ref()).then_some((from, to))
        })
        .collect();
    let mut environment = BTreeMap::new();
    for (name, value) in &record.environment {
        let moved = value.as_ref().map(|value| {
            moves
                .iter()
                .fold(value.clone(), |value, (from, to)| match to {
                    Some(to) => rebase(&value, from, to),
                    None => value,
                })
        });
        environment.insert(name.clone(), moved);
    }
    if let Some(Some(dotfile)) = environment.get(DEVENV_DOTFILE).cloned()
        && environment.contains_key(DEVENV_RUNTIME)
    {
        let digest = crate::api::dto::hex_lower(&Sha256::digest(dotfile.as_bytes()));
        let base = placement
            .runtime
            .map_or_else(|| PathBuf::from("/tmp"), Path::to_path_buf);
        environment.insert(
            DEVENV_RUNTIME.to_owned(),
            Some(
                base.join(format!("devenv-{}", &digest[..7]))
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
    }
    // What the move could not reach: a place of the origin this checkout has no counterpart
    // for, or a number inside the origin's port block. A hash of the origin's root, or
    // arithmetic on a fact cowshed does not know, is invisible here; the two-root equivalence
    // test is what catches those, for the producer it ran.
    let origin_ports = record.ports.filter(|ports| Some(*ports) != placement.ports);
    for (name, value) in &environment {
        let Some(value) = value else { continue };
        if moves.iter().any(|(from, _)| contains_path(value, from))
            || origin_ports.is_some_and(|ports| names_port(value, ports))
        {
            return Err(AdoptionMiss::Residue { name: name.clone() });
        }
    }
    // A record moves to another checkout only once its producer is certified: roots whose own
    // evaluations of these input bytes agreed after the move ([`write_record`]). A value no move
    // can produce — an `.envrc` exporting a hash of `$PWD`, arithmetic on the port block — makes
    // those evaluations disagree, so such a record never certifies and every clone of it
    // evaluates. Agreeing roots certify only what differs between them: two siblings agree on
    // anything derived from their shared parent directory or depth. So a clone adopts only when
    // its own parent is the parent of a root the producer was measured at; any other clone
    // evaluates, and agreeing adds its root to the proof. The checkout the record was written in
    // needs no proof: nothing moves.
    let origin = record.places[0].clone().unwrap_or_default();
    let distinct: BTreeSet<&PathBuf> = record.proof.iter().collect();
    let certified = distinct.len() >= 2
        && distinct.contains(&origin)
        && distinct
            .iter()
            .any(|root| root.parent() == placement.root.parent());
    if origin != placement.root && !certified {
        return Err(AdoptionMiss::Uncertified(Box::new(Uncertified {
            origin,
            digest,
            proof: record.proof,
            key: record.key,
            baseline: record.baseline,
            environment,
            from_tail: record.from_tail,
        })));
    }
    let entries: Vec<WatchEntry> = record
        .watches
        .iter()
        .map(|watch| {
            let path = absolute(watch);
            let exists = snapshot.exists(&path);
            WatchEntry { path, exists }
        })
        .collect();
    environment.insert(
        "DIRENV_WATCHES".to_owned(),
        Some(encode_direnv_watches(&entries, &snapshot)),
    );
    let tail = entries
        .iter()
        .filter(|entry| {
            entry
                .path
                .strip_prefix(placement.root)
                .is_ok_and(|rest| TAIL_FILES.iter().any(|tail| rest == Path::new(tail)))
        })
        .map(|entry| entry.path.clone())
        .collect();
    Ok(VerifiedActivation {
        environment,
        from_tail: record.from_tail,
        tail,
        entries,
        snapshot,
        contents,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebase_moves_whole_path_prefixes_only() {
        let from = "/home/dev/repo";
        let to = "/mnt/owner/repo/shed";
        assert_eq!(rebase("/home/dev/repo", from, to), "/mnt/owner/repo/shed");
        assert_eq!(
            rebase("/home/dev/repo/tooling:/nix/store/x/bin", from, to),
            "/mnt/owner/repo/shed/tooling:/nix/store/x/bin"
        );
        assert_eq!(
            rebase("/home/dev/repo-worktrees/a", from, to),
            "/home/dev/repo-worktrees/a"
        );
        assert_eq!(rebase("/x/home/dev/repo/a", from, to), "/x/home/dev/repo/a");
        assert_eq!(
            rebase("a=/home/dev/repo b", from, to),
            "a=/mnt/owner/repo/shed b"
        );
        assert_eq!(rebase("/w/batch-25", "/w/batch-2", "/w/y"), "/w/batch-25");
        // Compiler and linker flags glue the path to the flag: the path still starts there.
        assert_eq!(
            rebase("-I/home/dev/repo/include -L/home/dev/repo/lib", from, to),
            "-I/mnt/owner/repo/shed/include -L/mnt/owner/repo/shed/lib"
        );
        assert_eq!(
            rebase(
                "-Wl,-rpath,/home/dev/repo/lib -isystem/home/dev/repo/x",
                from,
                to
            ),
            "-Wl,-rpath,/mnt/owner/repo/shed/lib -isystem/mnt/owner/repo/shed/x"
        );
    }

    #[test]
    fn the_residue_check_sees_what_the_move_left() {
        let origin = "/home/dev/repo";
        assert!(
            contains_path("/w/home/dev/repo/x", origin),
            "inside a longer path"
        );
        assert!(contains_path("-I/home/dev/repo", origin));
        assert!(
            !contains_path("/home/dev/repo-worktrees/x", origin),
            "a sibling"
        );
        assert!(!contains_path(
            &rebase("-I/home/dev/repo/include", origin, "/s"),
            origin
        ));
    }

    #[test]
    fn credentials_and_workspace_values_are_withheld() {
        for name in [
            "HF_TOKEN",
            "MORPH_API_KEY",
            "FORGEJO_TOKEN",
            "TYPESAFE_API_KEY",
        ] {
            assert_eq!(
                withheld(name, Some("x")),
                Some(Withheld::Credential),
                "{name}"
            );
        }
        // A repository's own service configuration is not a credential by name: devenv sets it
        // and the tail does not, so withholding it would make an adopted shell lack it.
        assert_eq!(withheld("MINIO_ROOT_PASSWORD", Some("minioadmin")), None);
        assert_eq!(withheld("HF_TOKEN", None), None);
        assert_eq!(
            withheld("HTTPS_PROXY", Some("http://127.0.0.1:1")),
            Some(Withheld::Credential)
        );
        assert_eq!(
            withheld("SOME_URL", Some("https://user:pw@host/x")),
            Some(Withheld::Credential)
        );
        assert_eq!(withheld("SOME_URL", Some("https://host/x@y")), None);
        assert_eq!(
            withheld("COWSHED_RUNTIME_LINK", Some("/tmp/cs-1")),
            Some(Withheld::WorkspaceOwned)
        );
        assert_eq!(
            withheld("DEVENV_TASK_FILE", Some("/nix/store/x")),
            Some(Withheld::NotEnvironment)
        );
        assert_eq!(withheld("PATH", Some("/a:/b")), None);
        assert_eq!(withheld("DEVENV_ROOT", Some("/a")), None);
    }

    /// One checkout of the tree, with its own per-workspace facts.
    struct Site {
        root: PathBuf,
        link: &'static str,
        name: &'static str,
        port: u32,
    }

    impl Site {
        fn baseline(&self) -> BTreeMap<OsString, OsString> {
            let (root, name, port) = (self.root.display(), self.name, self.port);
            [
                ("PATH", "/usr/bin:/bin".to_owned()),
                ("HOME", format!("{root}/.cowshed/home")),
                ("XDG_RUNTIME_DIR", self.link.to_owned()),
                ("TMPDIR", format!("/store/tmp/{name}")),
                ("COWSHED_PORT_BASE", port.to_string()),
                ("COWSHED_PORT_BLOCK_SIZE", "64".to_owned()),
                (
                    "HTTPS_PROXY",
                    format!("http://{name}-token@127.0.0.1:{port}"),
                ),
            ]
            .into_iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
            .collect()
        }

        /// What this checkout's own evaluation exports, as devenv computes it here, plus
        /// `extra`'s values for this checkout.
        fn export(&self, extra: Extra) -> BTreeMap<String, Option<String>> {
            let root = self.root.display();
            let dotfile = format!("{root}/tooling/direnv/.devenv");
            let digest = crate::api::dto::hex_lower(&Sha256::digest(dotfile.as_bytes()));
            let mut export: BTreeMap<String, Option<String>> = [
                ("DEVENV_ROOT", Some(format!("{root}/tooling/direnv"))),
                (
                    "DEVENV_RUNTIME",
                    Some(format!("{}/devenv-{}", self.link, &digest[..7])),
                ),
                ("DEVENV_DOTFILE", Some(dotfile)),
                (
                    "PATH",
                    Some(format!(
                        "{root}/node_modules/.bin:/nix/store/abc-tool/bin:/usr/bin:/bin"
                    )),
                ),
                ("HF_TOKEN", Some("hf_secret".to_owned())),
                (
                    "DEVENV_TASK_FILE",
                    Some(format!("/nix/store/{}-tasks.json", self.name)),
                ),
                ("COWSHED_RUNTIME_LINK", Some(self.link.to_owned())),
                ("REMOVED", None),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
            for (name, value) in extra(self) {
                export.insert(name.to_owned(), value);
            }
            export
        }
    }

    type Extra = fn(&Site) -> Vec<(&'static str, Option<String>)>;

    const NOTHING: Extra = |_| Vec::new();

    /// The origin that evaluates first, a witness that evaluates the same inputs second, and the
    /// clone that adopts.
    struct Sites {
        scratch: PathBuf,
        origin: Site,
        witness: Site,
        clone: Site,
    }

    impl Drop for Sites {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.scratch);
        }
    }

    const INPUTS: [&str; 5] = [
        ".envrc",
        "tooling/direnv/devenv.nix",
        ".cowshed/env",
        ".envrc-local",
        "bun.lock",
    ];

    fn sites(label: &str) -> Sites {
        let scratch = std::env::temp_dir().join(format!(
            "cowshed-inherit-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let site = |path: &str, link, name, port| Site {
            root: scratch.join(path),
            link,
            name,
            port,
        };
        let sites = Sites {
            origin: site("dev/repo", "/tmp/cs-aaaaaaaaaaaa", "origin", 42240),
            witness: site(
                "mnt/owner/repo/first",
                "/tmp/cs-cccccccccccc",
                "first",
                42368,
            ),
            clone: site(
                "mnt/owner/repo/shed",
                "/tmp/cs-bbbbbbbbbbbb",
                "clone",
                42304,
            ),
            scratch: scratch.clone(),
        };
        for checkout in [&sites.origin, &sites.witness, &sites.clone] {
            let root = &checkout.root;
            std::fs::create_dir_all(root.join("tooling/direnv")).unwrap();
            std::fs::create_dir_all(root.join(SHELL_HOST_DIRECTORY)).unwrap();
            std::fs::write(root.join(".envrc"), "use devenv\n").unwrap();
            std::fs::write(root.join("tooling/direnv/devenv.nix"), "{ }\n").unwrap();
            std::fs::write(root.join(".envrc-local"), "export HF_TOKEN=hf_secret\n").unwrap();
            std::fs::write(root.join("bun.lock"), "lock\n").unwrap();
            // Differs per workspace by construction: it holds the workspace's own token.
            std::fs::write(
                root.join(".cowshed/env"),
                format!("export T={}\n", checkout.name),
            )
            .unwrap();
            // The controller's marker: the origin is main, the others are its clones, and only an
            // ancestor in a clone's lineage is ever written by that clone.
            let (role, workspace, lineage) = match checkout.name {
                "origin" => ("main", "main", "[]"),
                _ => (
                    "workspace",
                    checkout.name,
                    "[\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"]",
                ),
            };
            let incarnation = match checkout.name {
                "origin" => "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "first" => "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                _ => "cccccccccccccccccccccccccccccccc",
            };
            std::fs::write(
                root.join(crate::storage::WORKSPACE_MARKER_PATH),
                format!(
                    "{{\"version\":1,\"repoId\":\"owner/repo\",\"projectRoot\":\"/p\",\
                     \"workspace\":\"{workspace}\",\"workspaceIncarnation\":\"{incarnation}\",\
                     \"role\":\"{role}\",\"baseCommit\":\"0\",\"createdAt\":\"2026-01-01T00:00:00Z\",\
                     \"forkedFrom\":null,\"createdTrace\":\"t\",\"lineage\":{lineage},\
                     \"ownedLinksRevision\":1}}"
                ),
            )
            .unwrap();
        }
        sites
    }

    /// Record `site`'s own evaluation as an activation would, certifying `inherited` if given.
    fn evaluate(site: &Site, extra: Extra, inherited: Option<&Uncertified>) -> Certification {
        let baseline = site.baseline();
        let entries: Vec<WatchEntry> = INPUTS
            .iter()
            .map(|input| WatchEntry {
                path: site.root.join(input),
                exists: true,
            })
            .collect();
        let (_, contents) = certify(entries.iter().map(|entry| entry.path.as_path())).unwrap();
        write_record(
            &Placement::of(&site.root, &baseline),
            &baseline,
            &entries,
            &contents,
            &site.export(extra),
            inherited,
        )
        .unwrap()
    }

    fn verify_at(site: &Site) -> Result<VerifiedActivation, AdoptionMiss> {
        let baseline = site.baseline();
        verify(&Placement::of(&site.root, &baseline), &baseline)
    }

    /// The lineage as it happens: the origin evaluates; the witness carries its record, cannot
    /// adopt an uncertified one, evaluates the same inputs and so certifies or not; the clone then
    /// carries the origin's record as it stands. Returns what the witness's record did.
    fn lineage(sites: &Sites, extra: Extra) -> Certification {
        evaluate(&sites.origin, extra, None);
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.witness.root),
        )
        .unwrap();
        let certification = match verify_at(&sites.witness) {
            Err(miss) => match miss.into_uncertified() {
                Some(inherited) => evaluate(&sites.witness, extra, Some(&inherited)),
                None => Certification::NotAttempted(NotAttempted::NothingInherited),
            },
            Ok(_) => panic!("an uncertified record of another checkout was adopted"),
        };
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.clone.root),
        )
        .unwrap();
        certification
    }

    #[test]
    fn a_clone_adopts_a_certified_export_moved_and_never_its_credentials() {
        let sites = sites("adopt");
        assert_eq!(
            lineage(&sites, NOTHING),
            Certification::Certified {
                origin: Origin::Updated
            }
        );
        let written = std::fs::read_to_string(record_path(&sites.clone.root)).unwrap();
        assert!(
            !written.contains("hf_secret"),
            "no credential value is written"
        );
        assert!(!written.contains("tasks.json"), "no task file is carried");

        let verified = verify_at(&sites.clone).unwrap();
        let environment = verified.environment();
        let clone = sites.clone.root.display();
        let value = |name: &str| environment.get(name).cloned().flatten();
        assert_eq!(
            value("DEVENV_ROOT"),
            Some(format!("{clone}/tooling/direnv"))
        );
        assert_eq!(
            value("PATH"),
            Some(format!(
                "{clone}/node_modules/.bin:/nix/store/abc-tool/bin:/usr/bin:/bin"
            ))
        );
        assert_eq!(
            value("DEVENV_RUNTIME"),
            sites
                .clone
                .export(NOTHING)
                .remove("DEVENV_RUNTIME")
                .flatten(),
            "recomputed as devenv computes it here"
        );
        assert!(!environment.contains_key("HF_TOKEN"));
        assert!(!environment.contains_key("DEVENV_TASK_FILE"));
        assert!(!environment.contains_key("COWSHED_RUNTIME_LINK"));
        assert_eq!(
            environment.get("REMOVED"),
            Some(&None),
            "a removal is carried"
        );
        assert!(environment.contains_key("DIRENV_WATCHES"));
        assert_eq!(
            verified
                .from_tail()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["COWSHED_RUNTIME_LINK", "HF_TOKEN"]
        );
        assert_eq!(
            verified.tail(),
            [
                sites.clone.root.join(".cowshed/env"),
                sites.clone.root.join(".envrc-local")
            ]
        );
    }

    /// A certification never writes over a record the origin rewrote after the clone read it:
    /// the clone's own record certifies, the origin's newer one stays as written.
    #[test]
    fn a_certification_never_overwrites_a_newer_origin_record() {
        let sites = sites("newer-origin");
        evaluate(&sites.origin, NOTHING, None);
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.witness.root),
        )
        .unwrap();
        let inherited = verify_at(&sites.witness)
            .expect_err("uncertified")
            .into_uncertified()
            .expect("only for want of certification");
        let newer: Extra = |_| vec![("ADDED", Some("later".to_owned()))];
        evaluate(&sites.origin, newer, None);
        let rewritten = std::fs::read(record_path(&sites.origin.root)).unwrap();
        assert_eq!(
            evaluate(&sites.witness, NOTHING, Some(&inherited)),
            Certification::Certified {
                origin: Origin::Changed
            }
        );
        assert_eq!(
            std::fs::read(record_path(&sites.origin.root)).unwrap(),
            rewritten,
            "the origin's newer record is untouched"
        );
    }

    /// Roots that agreed certify only what differs between them: a clone under a parent no
    /// measured root has evaluates, however certified the record is.
    #[test]
    fn a_clone_under_an_unmeasured_parent_evaluates() {
        let sites = sites("unmeasured-parent");
        lineage(&sites, NOTHING);
        let elsewhere = Site {
            root: sites.scratch.join("slots/slot@1"),
            link: "/tmp/cs-dddddddddddd",
            name: "slot",
            port: 42432,
        };
        for input in INPUTS
            .iter()
            .chain([&crate::storage::WORKSPACE_MARKER_PATH])
        {
            let to = elsewhere.root.join(input);
            std::fs::create_dir_all(to.parent().unwrap()).unwrap();
            std::fs::copy(sites.clone.root.join(input), to).unwrap();
        }
        std::fs::create_dir_all(elsewhere.root.join(SHELL_HOST_DIRECTORY)).unwrap();
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&elsewhere.root),
        )
        .unwrap();
        assert!(matches!(
            verify_at(&elsewhere),
            Err(AdoptionMiss::Uncertified(_))
        ));
        assert!(
            verify_at(&sites.clone).is_ok(),
            "a sibling of a measured root adopts"
        );
    }

    #[test]
    fn an_uncertified_export_serves_only_its_own_checkout() {
        let sites = sites("uncertified");
        evaluate(&sites.origin, NOTHING, None);
        assert!(
            verify_at(&sites.origin).is_ok(),
            "nothing moves in the origin itself"
        );
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.clone.root),
        )
        .unwrap();
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::Uncertified(_))
        ));
    }

    /// The concrete case no runtime check can see: a value derived from a hash of the checkout
    /// path. The witness's own evaluation disagrees with the moved one, so the producer never
    /// certifies and the clone evaluates instead of taking the origin's hash.
    #[test]
    fn a_value_hashed_from_the_checkout_path_never_certifies() {
        let sites = sites("hashed-pwd");
        let hashed: Extra = |site| {
            let digest = Sha256::digest(site.root.as_os_str().as_bytes());
            vec![("PWD_HASH", Some(crate::api::dto::hex_lower(&digest)))]
        };
        assert_eq!(
            lineage(&sites, hashed),
            Certification::Differs {
                name: "PWD_HASH".into()
            }
        );
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::Uncertified(_))
        ));
    }

    #[test]
    fn a_changed_input_byte_misses() {
        let sites = sites("key");
        lineage(&sites, NOTHING);
        std::fs::write(sites.clone.root.join("bun.lock"), "lock2\n").unwrap();
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::KeyMoved)
        ));
    }

    #[test]
    fn a_rewrite_of_the_same_bytes_still_adopts() {
        let sites = sites("same-bytes");
        lineage(&sites, NOTHING);
        std::fs::remove_file(sites.clone.root.join("bun.lock")).unwrap();
        std::fs::write(sites.clone.root.join("bun.lock"), "lock\n").unwrap();
        assert!(verify_at(&sites.clone).is_ok());
    }

    #[test]
    fn another_baseline_misses() {
        let sites = sites("baseline");
        lineage(&sites, NOTHING);
        let mut baseline = sites.clone.baseline();
        baseline.insert("EXTRA".into(), "1".into());
        assert!(matches!(
            verify(&Placement::of(&sites.clone.root, &baseline), &baseline),
            Err(AdoptionMiss::BaselineMoved)
        ));
    }

    #[test]
    fn every_workspace_place_moves() {
        let sites = sites("places");
        let places: Extra = |site| {
            vec![
                ("SCRATCH", Some(format!("/store/tmp/{}/cache", site.name))),
                (
                    "CONFIG",
                    Some(format!("{}/.cowshed/home/.config", site.root.display())),
                ),
            ]
        };
        assert_eq!(
            lineage(&sites, places),
            Certification::Certified {
                origin: Origin::Updated
            }
        );
        let verified = verify_at(&sites.clone).unwrap();
        let value = |name: &str| verified.environment().get(name).cloned().flatten();
        assert_eq!(value("SCRATCH"), Some("/store/tmp/clone/cache".into()));
        assert_eq!(
            value("CONFIG"),
            Some(format!(
                "{}/.cowshed/home/.config",
                sites.clone.root.display()
            ))
        );
    }

    #[test]
    fn a_value_the_move_cannot_reach_misses() {
        // A number inside the origin's port block.
        let sites = self::sites("port-residue");
        let port: Extra = |site| {
            vec![(
                "LEFTOVER",
                Some(format!("http://127.0.0.1:{}/", site.port + 10)),
            )]
        };
        lineage(&sites, port);
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
        // A spelling the move does not reach: the origin inside a longer path.
        let sites = self::sites("glued-residue");
        let glued: Extra = |site| {
            vec![(
                "LEFTOVER",
                Some(format!("/prefix{}/lib", site.root.display())),
            )]
        };
        lineage(&sites, glued);
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
        // A place of the origin this checkout has no counterpart for.
        let sites = self::sites("place-residue");
        let scratch: Extra =
            |site| vec![("LEFTOVER", Some(format!("/store/tmp/{}/cache", site.name)))];
        lineage(&sites, scratch);
        let mut baseline = sites.clone.baseline();
        baseline.remove(std::ffi::OsStr::new("TMPDIR"));
        assert!(matches!(
            verify(&Placement::of(&sites.clone.root, &baseline), &baseline),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
    }
}
