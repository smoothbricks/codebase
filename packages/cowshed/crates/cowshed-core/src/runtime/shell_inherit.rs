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
use crate::sandbox::{EgressGrant, SandboxGrants};

/// The record's file in the workspace's host directory, which every job profile denies writes to.
pub const RECORD_FILE: &str = "activation.json";

/// The baseline entry under which an activator keys its pool's grants: what an evaluation could
/// read or reach is part of what it was computed against. Angle brackets cannot begin an
/// environment variable name, so no variable collides with it, and a baseline that differs on
/// it reports the grants by this name.
pub const GRANTS_KEY: &str = "<grants>";

/// The selected shell directory, normalized by the baseline's existing checkout-place move.
pub const SHELL_KEY: &str = "<shell>";

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
        BUILD_CACHE_ENV, NODE_COMPILE_CACHE_ENV, NX_REMOTE_READ_ONLY_ENV, NX_REMOTE_SERVER_ENV,
        PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV, RUNTIME_LINK_ENV, TEMP_DIR_ENV,
    };
    [
        PORT_BASE_ENV,
        PORT_BLOCK_SIZE_ENV,
        NX_REMOTE_SERVER_ENV,
        NX_REMOTE_READ_ONLY_ENV,
        TEMP_DIR_ENV,
        NODE_COMPILE_CACHE_ENV,
        RUNTIME_LINK_ENV,
        BUILD_CACHE_ENV,
        "NX_MAX_CACHE_SIZE",
        "CI",
    ]
    .contains(&name)
        || crate::capabilities::sccache::HOST_ENVIRONMENT.contains(&name)
        || crate::capabilities::cargo::HOST_ENVIRONMENT.contains(&name)
        || crate::capabilities::mbx::HOST_ENVIRONMENT.contains(&name)
}

fn credential(name: &str, value: Option<&str>) -> bool {
    use crate::workspace_environment::WORKSPACE_TOKEN_ENV;
    let upper = name.to_ascii_uppercase();
    name == WORKSPACE_TOKEN_ENV
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

impl RecordedWatch {
    fn absolute(&self, root: &Path) -> PathBuf {
        if self.inside {
            root.join(&self.path)
        } else {
            self.path.clone()
        }
    }
}

/// The file as written, decoded and filtered through read_record.
#[derive(serde::Serialize, serde::Deserialize)]
struct ActivationRecord {
    v: u32,
    /// The activating checkout's root: the origin every move of this record starts from.
    root: PathBuf,
    /// The activating checkout's [`Placement::beside`], in their order.
    beside: [Option<PathBuf>; BESIDE],
    ports: Option<(u32, u32)>,
    key: String,
    baseline: Baseline,
    watches: Vec<RecordedWatch>,
    /// direnv's export minus every withheld name: for each variable the activation changed, its
    /// complete value, or `null` for a removal, relative to the host's baseline.
    environment: BTreeMap<String, Option<String>>,
    /// The withheld names the export set that the adopting checkout's tail supplies. Names only;
    /// no value of a credential is ever written.
    from_tail: BTreeSet<String>,
}

impl ActivationRecord {
    /// The activating checkout's [`Placement::places`], root first.
    fn places(&self) -> [Option<&Path>; BESIDE + 1] {
        let [runtime, temp_dir, home] = &self.beside;
        [
            Some(self.root.as_path()),
            runtime.as_deref(),
            temp_dir.as_deref(),
            home.as_deref(),
        ]
    }

    fn inputs(&self, root: &Path) -> Result<(Snapshot, Contents), AdoptionMiss> {
        let paths: Vec<_> = self
            .watches
            .iter()
            .map(|watch| watch.absolute(root))
            .collect();
        // Identities, bytes, identities: the snapshot must describe the bytes that were keyed.
        let (snapshot, contents) = certify(paths.iter().map(PathBuf::as_path))?;
        if key(root, &self.watches, &contents) != self.key {
            return Err(AdoptionMiss::KeyMoved);
        }
        Ok((snapshot, contents))
    }
}

/// Where a checkout's record lives.
pub fn record_path(mount: &Path) -> PathBuf {
    mount.join(SHELL_HOST_DIRECTORY).join(RECORD_FILE)
}

/// The grants as the baseline keys them under [`GRANTS_KEY`]: a byte encoding fixed here, so
/// every cowshed build keys the same grants alike, which `Debug` output does not promise. Each
/// list is its name, its length and its entries, every field NUL-terminated: no path, host or
/// port holds a NUL. Paths stay text, so the baseline moves the checkout's places in them.
/// The loopback grant follows the lists as its name and 0 or 1, both NUL-terminated.
pub fn keyed_grants(grants: &SandboxGrants) -> OsString {
    use std::fmt::Write as _;
    const WRITE: &str = "writing to an OsString cannot fail";
    // Destructured whole: a grant field added later does not compile until it is keyed here.
    let SandboxGrants {
        read,
        write,
        deny_write,
        deny,
        egress,
        loopback_any,
    } = grants;
    let mut keyed = OsString::new();
    for (name, paths) in [
        ("read", read),
        ("write", write),
        ("deny-write", deny_write),
        ("deny", deny),
    ] {
        write!(keyed, "{name}\0{}\0", paths.len()).expect(WRITE);
        for path in paths {
            keyed.push(path);
            keyed.push("\0");
        }
    }
    write!(keyed, "egress\0{}\0", egress.len()).expect(WRITE);
    for EgressGrant { host, ports } in egress {
        write!(keyed, "{host}\0{}\0", ports.len()).expect(WRITE);
        for port in ports {
            write!(keyed, "{port}\0").expect(WRITE);
        }
    }
    write!(keyed, "loopback-any\0{}\0", u8::from(*loopback_any)).expect(WRITE);
    keyed
}

/// How many per-workspace places a checkout has beside its root ([`Placement::beside`]).
const BESIDE: usize = 3;

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
    fn places(&self) -> [Option<&'a Path>; BESIDE + 1] {
        let [runtime, temp_dir, home] = self.beside();
        [Some(self.root), runtime, temp_dir, home]
    }

    /// The per-workspace paths beside the root that a value can name, each only when the
    /// baseline names it: the runtime link, the temporary directory and the home.
    fn beside(&self) -> [Option<&'a Path>; BESIDE] {
        [self.runtime, self.temp_dir, self.home]
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

/// The baseline key: for each variable of the host's environment before activation, SHA-256 of
/// its value with every per-workspace place replaced by a placeholder, every withheld name left
/// out (a workspace token or proxy URL differs per workspace by construction and is not an
/// input the export was computed from that adoption could carry). Kept per variable, so a
/// baseline that differs says which variable does; no value is kept.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
struct Baseline(BTreeMap<String, String>);

impl Baseline {
    /// The first variable set in one baseline and not the other, or set differently.
    fn differs_from(&self, other: &Self) -> Option<String> {
        let names: BTreeSet<&String> = self.0.keys().chain(other.0.keys()).collect();
        names
            .into_iter()
            .find(|name| self.0.get(*name) != other.0.get(*name))
            .cloned()
    }
}

fn baseline(placement: &Placement<'_>, environment: &BTreeMap<OsString, OsString>) -> Baseline {
    let places = placement
        .places()
        .map(|place| place.map(Path::to_string_lossy));
    let digest = |bytes: &[u8]| crate::api::dto::hex_lower(&Sha256::digest(bytes));
    let mut keyed = BTreeMap::new();
    for (name, value) in environment {
        let (Some(name), Some(value)) = (name.to_str(), value.to_str()) else {
            // A non-UTF-8 variable is keyed as it is: it cannot be moved, so it must match.
            keyed.insert(
                name.to_string_lossy().into_owned(),
                digest(value.as_bytes()),
            );
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
        keyed.insert(name.to_owned(), digest(value.as_bytes()));
    }
    Baseline(keyed)
}

/// Why a record could not be written.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("cannot encode the activation record: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("cannot publish the activation record: {0}")]
    Publish(#[from] crate::metadata::MetadataError),
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

/// Leave an activation's export for the checkouts cloned from this one. `export` is direnv's
/// export the host applied, `contents` the inputs' bytes certified when it finished, and
/// `baseline_environment` the host's environment before it. The checkout's own supervisor is
/// the record's only writer: no clone writes back to the checkout it inherited from.
pub fn write_record(
    placement: &Placement<'_>,
    baseline_environment: &BTreeMap<OsString, OsString>,
    entries: &[WatchEntry],
    contents: &Contents,
    export: &BTreeMap<String, Option<String>>,
) -> Result<(), RecordError> {
    let watches: Vec<RecordedWatch> = entries
        .iter()
        .map(|entry| RecordedWatch {
            exists: entry.exists,
            ..relative(placement.root, &entry.path)
        })
        .collect();
    let (environment, from_tail) = carried(export);
    let record = ActivationRecord {
        v: RECORD_VERSION,
        root: placement.root.to_path_buf(),
        beside: placement.beside().map(|place| place.map(Path::to_path_buf)),
        ports: placement.ports,
        key: key(placement.root, &watches, contents),
        baseline: baseline(placement, baseline_environment),
        watches,
        environment,
        from_tail,
    };
    crate::metadata::write_atomic_bytes(
        &record_path(placement.root),
        &serde_json::to_vec(&record)?,
    )?;
    Ok(())
}

/// Why a host evaluates instead of adopting. Each is reported on the activating job's stderr.
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
    #[error("the inherited shell was computed against a different environment: {name} differs")]
    BaselineMoved { name: String },
    #[error("{name} still names the origin checkout after the move")]
    Residue { name: String },
    #[error("the inherited shell record carries {name}, which no record may")]
    Withheld { name: String },
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

/// The key of the activation a checkout last recorded (SHA-256 over its watched inputs), or
/// `None` when it records none: the canonical activation's input identity, which build-state
/// discovery keys on (15_capabilities.md, "mbx detection").
pub fn recorded_key(mount: &Path) -> Option<String> {
    read_record(mount).ok().map(|record| record.key)
}

/// Read a checkout's record, refusing one no record may be: oversized, of another version,
/// or carrying a withheld name.
fn read_record(mount: &Path) -> Result<ActivationRecord, AdoptionMiss> {
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
    if !u64::try_from(bytes.len()).is_ok_and(|length| length <= MAX_RECORD_BYTES) {
        return Err(AdoptionMiss::Malformed("larger than any record".into()));
    }
    let record: ActivationRecord = serde_json::from_slice(&bytes)
        .map_err(|error| AdoptionMiss::Malformed(error.to_string()))?;
    if record.v != RECORD_VERSION {
        return Err(AdoptionMiss::Version(record.v));
    }
    if let Some((name, _)) = record
        .environment
        .iter()
        .find(|(name, value)| withheld(name, value.as_deref()).is_some())
    {
        return Err(AdoptionMiss::Withheld { name: name.clone() });
    }
    Ok(record)
}

/// Check the activation's watched input bytes without evaluating or mutating any checkout.
pub fn current_inputs(root: &Path) -> Result<(), AdoptionMiss> {
    let record = read_record(root)?;
    record.inputs(root).map(|_| ())
}

/// Prove this checkout's inherited record, or say why it cannot serve.
pub fn verify(
    placement: &Placement<'_>,
    baseline_environment: &BTreeMap<OsString, OsString>,
) -> Result<VerifiedActivation, AdoptionMiss> {
    let record = read_record(placement.root)?;
    let (snapshot, contents) = record.inputs(placement.root)?;
    if let Some(name) = baseline(placement, baseline_environment).differs_from(&record.baseline) {
        return Err(AdoptionMiss::BaselineMoved { name });
    }
    // Each per-workspace place of the origin moves to this checkout's, root first, at a
    // path-component boundary.
    let moves: Vec<(String, Option<String>)> = record
        .places()
        .into_iter()
        .zip(placement.places())
        .filter_map(|(from, to)| {
            let from = from?.to_string_lossy().into_owned();
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
    // for, or a number inside the origin's port block. A value derived from the origin's root
    // in a way no move names — a hash of it, say — is invisible here and is inherited as the
    // origin recorded it: a fork is its origin's clone, and nothing re-evaluates to check.
    let origin_ports = record.ports.filter(|ports| Some(*ports) != placement.ports);
    for (name, value) in &environment {
        let Some(value) = value else { continue };
        if moves.iter().any(|(from, _)| contains_path(value, from))
            || origin_ports.is_some_and(|ports| names_port(value, ports))
        {
            return Err(AdoptionMiss::Residue { name: name.clone() });
        }
    }
    let entries: Vec<WatchEntry> = record
        .watches
        .iter()
        .map(|watch| {
            let path = watch.absolute(placement.root);
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
            withheld("CARGO_UNSTABLE_MTIME_ON_USE", Some("true")),
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
        fn path_hash(&self) -> String {
            crate::api::dto::hex_lower(&Sha256::digest(self.root.as_os_str().as_bytes()))
        }

        fn baseline(&self) -> BTreeMap<OsString, OsString> {
            let (root, name, port) = (self.root.display(), self.name, self.port);
            [
                ("PATH", "/usr/bin:/bin".to_owned()),
                ("HOME", format!("{root}/.cowshed/home")),
                ("XDG_RUNTIME_DIR", self.link.to_owned()),
                ("TMPDIR", format!("/store/tmp/{name}")),
                ("COWSHED_PORT_BASE", port.to_string()),
                (
                    "COWSHED_PORT_BLOCK_SIZE",
                    crate::metadata::NEW_PORT_BLOCK_SIZE.to_string(),
                ),
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

    /// The origin that evaluates, the clone that adopts its record, and a clone under another parent.
    struct Sites {
        scratch: PathBuf,
        origin: Site,
        clone: Site,
        elsewhere: Site,
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
        let site = |path: &str, link, name| Site {
            root: scratch.join(path),
            link,
            name,
            port: u32::from(cowshed_gateway_types::MACOS_FIXTURE_PORT_MIN),
        };
        let mut sites = Sites {
            origin: site("dev/repo", "/tmp/cs-aaaaaaaaaaaa", "origin"),
            clone: site("mnt/owner/repo/shed", "/tmp/cs-bbbbbbbbbbbb", "clone"),
            elsewhere: site("slots/slot@1", "/tmp/cs-dddddddddddd", "slot"),
            scratch: scratch.clone(),
        };
        // Namespace uniqueness is not an oracle input: neither real paths nor their derived
        // hashes may accidentally look like a port of a different workspace in these fixtures.
        let values: Vec<_> = [&sites.origin, &sites.clone, &sites.elsewhere]
            .into_iter()
            .flat_map(|site| {
                site.export(NOTHING)
                    .into_values()
                    .flatten()
                    .chain(std::iter::once(site.path_hash()))
            })
            .collect();
        let mut ports =
            crate::metadata::PortBlock::macos_candidates(crate::metadata::PortAllocation::Fixture)
                .filter(|block| {
                    let range = (u32::from(block.base()), u32::from(block.size()));
                    values.iter().all(|value| !names_port(value, range))
                });
        for site in [&mut sites.origin, &mut sites.clone, &mut sites.elsewhere] {
            site.port = u32::from(
                ports
                    .next()
                    .expect("a disjoint fixture block for every site's path inputs")
                    .base(),
            );
        }
        for checkout in [&sites.origin, &sites.clone] {
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
        }
        sites
    }

    /// Record `site`'s own evaluation as an activation would.
    fn evaluate(site: &Site, extra: Extra) {
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
        )
        .unwrap()
    }

    #[test]
    fn source_freshness_checks_current_bytes_and_rejects_withheld_exports() {
        let sites = sites("source-current");
        assert!(matches!(
            current_inputs(&sites.origin.root),
            Err(AdoptionMiss::Absent)
        ));
        evaluate(&sites.origin, NOTHING);
        assert!(current_inputs(&sites.origin.root).is_ok());
        let input = sites.origin.root.join("bun.lock");
        std::fs::write(&input, "lock\n").unwrap();
        assert!(
            current_inputs(&sites.origin.root).is_ok(),
            "new identity, same input bytes"
        );
        std::fs::write(&input, "changed\n").unwrap();
        assert!(matches!(
            current_inputs(&sites.origin.root),
            Err(AdoptionMiss::KeyMoved)
        ));
        std::fs::write(&input, "lock\n").unwrap();
        let path = record_path(&sites.origin.root);
        let mut recorded: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        recorded["environment"]["HF_TOKEN"] = serde_json::Value::String("fixture-value".into());
        std::fs::write(path, serde_json::to_vec(&recorded).unwrap()).unwrap();
        assert!(matches!(
            current_inputs(&sites.origin.root),
            Err(AdoptionMiss::Withheld { .. })
        ));
    }

    fn verify_at(site: &Site) -> Result<VerifiedActivation, AdoptionMiss> {
        let baseline = site.baseline();
        verify(&Placement::of(&site.root, &baseline), &baseline)
    }

    /// The lineage as it happens: the origin evaluates, and the clone carries its record as
    /// the APFS clone carries it, ready to adopt.
    fn lineage(sites: &Sites, extra: Extra) {
        evaluate(&sites.origin, extra);
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.clone.root),
        )
        .unwrap();
    }

    #[test]
    fn a_fixed_decimal_in_the_fixture_root_is_an_actual_port_residue() {
        let mut sites = sites("fixed-42240");
        sites.origin.port = 42240;
        assert!(names_port(
            &sites.clone.root.display().to_string(),
            (
                sites.origin.port,
                u32::from(crate::metadata::NEW_PORT_BLOCK_SIZE)
            ),
        ));
        lineage(&sites, NOTHING);
        let miss = verify_at(&sites.clone).expect_err("the root names the origin's port range");
        assert!(
            matches!(&miss, AdoptionMiss::Residue { .. }),
            "actual refusal: {miss:?}"
        );
    }

    #[test]
    fn a_clone_adopts_its_origins_export_moved_and_never_its_credentials() {
        let sites = sites("fixed-42240");
        lineage(&sites, NOTHING);
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

    /// The directory a runtime compiles into is the workspace's own private cache, so the
    /// record carries it no more than it carries the workspace's temp dir: the clone's
    /// adopted export has none, its own published-environment tail is what must supply it,
    /// and no origin path is ever written into the record a clone inherits.
    #[test]
    fn a_clone_never_adopts_the_origins_compile_cache() {
        let compile_cache: Extra = |site| {
            vec![(
                crate::workspace_environment::NODE_COMPILE_CACHE_ENV,
                Some(format!(
                    "{}/.cowshed/cache/{}",
                    site.root.display(),
                    crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY
                )),
            )]
        };
        let sites = sites("compile-cache");
        lineage(&sites, compile_cache);
        let written = std::fs::read_to_string(record_path(&sites.clone.root)).unwrap();
        assert!(
            !written.contains(crate::workspace_environment::NODE_COMPILE_CACHE_DIRECTORY),
            "the origin's compile-cache path is written into the record"
        );
        let verified = verify_at(&sites.clone).unwrap();
        assert!(
            !verified
                .environment()
                .contains_key(crate::workspace_environment::NODE_COMPILE_CACHE_ENV),
            "an adopted export carries the origin's compile cache"
        );
        assert!(
            verified
                .from_tail()
                .iter()
                .any(|name| name.as_str() == crate::workspace_environment::NODE_COMPILE_CACHE_ENV),
            "the clone's own published environment must supply its compile cache, tail names {:?}",
            verified.from_tail()
        );
    }
    /// The root every move starts from is part of the record's type: a record without one is
    /// not one this cowshed writes, never a move from the empty path.
    #[test]
    fn a_record_without_its_root_is_malformed() {
        let sites = sites("rootless");
        lineage(&sites, NOTHING);
        let path = record_path(&sites.clone.root);
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["root"] = serde_json::Value::Null;
        std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        assert!(matches!(
            verify_at(&sites.clone),
            Err(AdoptionMiss::Malformed(_))
        ));
    }

    #[test]
    fn the_grants_key_is_a_fixed_encoding() {
        let mut grants = SandboxGrants {
            read: vec!["/x/repo/a".into()],
            write: Vec::new(),
            deny_write: vec![".git".into()],
            deny: Vec::new(),
            egress: vec![EgressGrant {
                host: "example.com".into(),
                ports: vec![443, 80],
            }],
            loopback_any: true,
        };
        let mut fields = [
            "read",
            "1",
            "/x/repo/a",
            "write",
            "0",
            "deny-write",
            "1",
            ".git",
            "deny",
            "0",
            "egress",
            "1",
            "example.com",
            "2",
            "443",
            "80",
            "loopback-any",
            "1",
        ];
        let expected: String = fields.iter().map(|field| format!("{field}\0")).collect();
        let with_loopback = keyed_grants(&grants);
        assert_eq!(with_loopback, OsString::from(expected));
        grants.loopback_any = false;
        fields[17] = "0";
        let expected: String = fields.iter().map(|field| format!("{field}\0")).collect();
        let without_loopback = keyed_grants(&grants);
        assert_eq!(without_loopback, OsString::from(expected));
        assert_ne!(with_loopback, without_loopback);
    }

    /// A clone adopts under any parent: nothing measures the producer at a second root first.
    #[test]
    fn a_clone_under_any_parent_adopts() {
        let sites = sites("any-parent");
        lineage(&sites, NOTHING);
        let elsewhere = &sites.elsewhere;
        for input in INPUTS {
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
        assert!(
            verify_at(elsewhere).is_ok(),
            "a clone under another parent adopts"
        );
        assert!(verify_at(&sites.clone).is_ok(), "and so does a sibling");
    }

    #[test]
    fn an_origins_export_serves_its_first_clone_as_it_serves_itself() {
        let sites = sites("first-clone");
        evaluate(&sites.origin, NOTHING);
        assert!(
            verify_at(&sites.origin).is_ok(),
            "nothing moves in the origin itself"
        );
        std::fs::copy(
            record_path(&sites.origin.root),
            record_path(&sites.clone.root),
        )
        .unwrap();
        assert!(
            verify_at(&sites.clone).is_ok(),
            "the first clone adopts without evaluating"
        );
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
            Err(AdoptionMiss::BaselineMoved { name }) if name == "EXTRA"
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
        lineage(&sites, places);
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
