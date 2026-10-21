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
}

/// Leave an activation's export for the checkouts cloned from this one. `export` is direnv's
/// export the host applied, `contents` the inputs' bytes certified when it finished, and
/// `baseline_environment` the host's environment before it.
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
    let record = ActivationRecord {
        v: RECORD_VERSION,
        places: placement.places().map(|place| place.map(Path::to_path_buf)),
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
    #[error("the inherited shell was computed against a different environment")]
    BaselineMoved,
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
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(AdoptionMiss::Malformed("larger than any record".into()));
    }
    let record: ActivationRecord = serde_json::from_slice(&bytes)
        .map_err(|error| AdoptionMiss::Malformed(error.to_string()))?;
    if record.v != RECORD_VERSION {
        return Err(AdoptionMiss::Version(record.v));
    }
    Ok(record)
}

/// Prove this checkout's inherited record, or say why it cannot serve.
pub fn verify(
    placement: &Placement<'_>,
    baseline_environment: &BTreeMap<OsString, OsString>,
) -> Result<VerifiedActivation, AdoptionMiss> {
    let record = read_record(placement.root)?;
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

    /// Two checkouts of the same tree at different mounts, with their own per-workspace facts.
    struct Pair {
        scratch: PathBuf,
        origin: PathBuf,
        clone: PathBuf,
    }

    impl Drop for Pair {
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

    fn pair(label: &str) -> Pair {
        let scratch = std::env::temp_dir().join(format!(
            "cowshed-inherit-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let origin = scratch.join("dev/repo");
        let clone = scratch.join("mnt/owner/repo/shed");
        for (root, workspace) in [(&origin, "origin"), (&clone, "clone")] {
            std::fs::create_dir_all(root.join("tooling/direnv")).unwrap();
            std::fs::create_dir_all(root.join(SHELL_HOST_DIRECTORY)).unwrap();
            std::fs::write(root.join(".envrc"), "use devenv\n").unwrap();
            std::fs::write(root.join("tooling/direnv/devenv.nix"), "{ }\n").unwrap();
            std::fs::write(root.join(".envrc-local"), "export HF_TOKEN=hf_secret\n").unwrap();
            std::fs::write(root.join("bun.lock"), "lock\n").unwrap();
            // Differs per workspace by construction: it holds the workspace's own token.
            std::fs::write(root.join(".cowshed/env"), format!("export T={workspace}\n")).unwrap();
        }
        Pair {
            scratch,
            origin,
            clone,
        }
    }

    fn baseline_of(
        root: &Path,
        link: &str,
        workspace: &str,
        port: u32,
    ) -> BTreeMap<OsString, OsString> {
        [
            ("PATH", "/usr/bin:/bin".to_owned()),
            ("HOME", format!("{}/.cowshed/home", root.display())),
            ("XDG_RUNTIME_DIR", link.to_owned()),
            ("TMPDIR", format!("/store/tmp/{workspace}")),
            ("COWSHED_PORT_BASE", port.to_string()),
            ("COWSHED_PORT_BLOCK_SIZE", "64".to_owned()),
            (
                "HTTPS_PROXY",
                format!("http://{workspace}-token@127.0.0.1:{port}"),
            ),
        ]
        .into_iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect()
    }

    fn entries(root: &Path) -> Vec<WatchEntry> {
        INPUTS
            .iter()
            .map(|input| WatchEntry {
                path: root.join(input),
                exists: true,
            })
            .collect()
    }

    /// Evaluate nothing: record `export` at the origin as an activation would, and carry the
    /// record to the clone as the APFS clone does.
    fn record_and_clone(
        pair: &Pair,
        export: &BTreeMap<String, Option<String>>,
    ) -> BTreeMap<OsString, OsString> {
        let baseline = baseline_of(&pair.origin, "/tmp/cs-aaaaaaaaaaaa", "origin", 42240);
        let entries = entries(&pair.origin);
        let (_, contents) = certify(entries.iter().map(|entry| entry.path.as_path())).unwrap();
        write_record(
            &Placement::of(&pair.origin, &baseline),
            &baseline,
            &entries,
            &contents,
            export,
        )
        .unwrap();
        std::fs::copy(record_path(&pair.origin), record_path(&pair.clone)).unwrap();
        baseline_of(&pair.clone, "/tmp/cs-bbbbbbbbbbbb", "clone", 42304)
    }

    fn export_at(origin: &Path) -> BTreeMap<String, Option<String>> {
        let root = origin.display();
        [
            ("DEVENV_ROOT", Some(format!("{root}/tooling/direnv"))),
            (
                "DEVENV_DOTFILE",
                Some(format!("{root}/tooling/direnv/.devenv")),
            ),
            (
                "DEVENV_RUNTIME",
                Some("/tmp/cs-aaaaaaaaaaaa/devenv-0000000".to_owned()),
            ),
            (
                "PATH",
                Some(format!(
                    "{root}/node_modules/.bin:/nix/store/abc-tool/bin:/usr/bin:/bin"
                )),
            ),
            ("SIBLING", Some(format!("{root}-worktrees/other"))),
            ("HF_TOKEN", Some("hf_secret".to_owned())),
            (
                "DEVENV_TASK_FILE",
                Some("/nix/store/xyz-tasks.json".to_owned()),
            ),
            (
                "COWSHED_RUNTIME_LINK",
                Some("/tmp/cs-aaaaaaaaaaaa".to_owned()),
            ),
            ("REMOVED", None),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
    }

    #[test]
    fn a_clone_adopts_the_moved_export_and_never_its_credentials() {
        let pair = pair("adopt");
        let clone_baseline = record_and_clone(&pair, &export_at(&pair.origin));
        let written = std::fs::read_to_string(record_path(&pair.clone)).unwrap();
        assert!(
            !written.contains("hf_secret"),
            "no credential value is written"
        );
        assert!(
            !written.contains("xyz-tasks"),
            "the parent's task file is not carried"
        );

        let verified = verify(
            &Placement::of(&pair.clone, &clone_baseline),
            &clone_baseline,
        )
        .unwrap();
        let environment = verified.environment();
        let clone = pair.clone.display();
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
            value("SIBLING"),
            Some(format!("{}-worktrees/other", pair.origin.display())),
            "a sibling of the origin is not the origin"
        );
        let dotfile = format!("{clone}/tooling/direnv/.devenv");
        let digest = crate::api::dto::hex_lower(&Sha256::digest(dotfile.as_bytes()));
        assert_eq!(
            value("DEVENV_RUNTIME"),
            Some(format!("/tmp/cs-bbbbbbbbbbbb/devenv-{}", &digest[..7]))
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
                pair.clone.join(".cowshed/env"),
                pair.clone.join(".envrc-local")
            ]
        );
    }

    #[test]
    fn a_changed_input_byte_misses() {
        let pair = pair("key");
        let clone_baseline = record_and_clone(&pair, &export_at(&pair.origin));
        std::fs::write(pair.clone.join("bun.lock"), "lock2\n").unwrap();
        assert!(matches!(
            verify(
                &Placement::of(&pair.clone, &clone_baseline),
                &clone_baseline
            ),
            Err(AdoptionMiss::KeyMoved)
        ));
    }

    #[test]
    fn a_rewrite_of_the_same_bytes_still_adopts() {
        let pair = pair("same-bytes");
        let clone_baseline = record_and_clone(&pair, &export_at(&pair.origin));
        std::fs::remove_file(pair.clone.join("bun.lock")).unwrap();
        std::fs::write(pair.clone.join("bun.lock"), "lock\n").unwrap();
        assert!(
            verify(
                &Placement::of(&pair.clone, &clone_baseline),
                &clone_baseline
            )
            .is_ok()
        );
    }

    #[test]
    fn another_baseline_misses() {
        let pair = pair("baseline");
        let mut clone_baseline = record_and_clone(&pair, &export_at(&pair.origin));
        clone_baseline.insert("EXTRA".into(), "1".into());
        assert!(matches!(
            verify(
                &Placement::of(&pair.clone, &clone_baseline),
                &clone_baseline
            ),
            Err(AdoptionMiss::BaselineMoved)
        ));
    }

    #[test]
    fn every_workspace_place_moves() {
        let pair = pair("places");
        let mut export = export_at(&pair.origin);
        export.insert("SCRATCH".into(), Some("/store/tmp/origin/cache".into()));
        export.insert(
            "CONFIG".into(),
            Some(format!("{}/.cowshed/home/.config", pair.origin.display())),
        );
        let clone_baseline = record_and_clone(&pair, &export);
        let verified = verify(
            &Placement::of(&pair.clone, &clone_baseline),
            &clone_baseline,
        )
        .unwrap();
        let value = |name: &str| verified.environment().get(name).cloned().flatten();
        assert_eq!(value("SCRATCH"), Some("/store/tmp/clone/cache".into()));
        assert_eq!(
            value("CONFIG"),
            Some(format!("{}/.cowshed/home/.config", pair.clone.display()))
        );
    }

    #[test]
    fn a_value_the_move_cannot_reach_misses() {
        // A number inside the origin's port block.
        let pair = pair("port-residue");
        let mut export = export_at(&pair.origin);
        export.insert("LEFTOVER".into(), Some("http://127.0.0.1:42250/".into()));
        let clone_baseline = record_and_clone(&pair, &export);
        assert!(matches!(
            verify(&Placement::of(&pair.clone, &clone_baseline), &clone_baseline),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
        // A spelling the move does not reach: the origin inside a longer path.
        let pair = self::pair("glued-residue");
        let mut export = export_at(&pair.origin);
        export.insert(
            "LEFTOVER".into(),
            Some(format!("/prefix{}/lib", pair.origin.display())),
        );
        let clone_baseline = record_and_clone(&pair, &export);
        assert!(matches!(
            verify(&Placement::of(&pair.clone, &clone_baseline), &clone_baseline),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
        // A place of the origin this checkout has no counterpart for.
        let pair = self::pair("place-residue");
        let mut export = export_at(&pair.origin);
        export.insert("LEFTOVER".into(), Some("/store/tmp/origin/cache".into()));
        let mut clone_baseline = record_and_clone(&pair, &export);
        clone_baseline.remove(std::ffi::OsStr::new("TMPDIR"));
        assert!(matches!(
            verify(&Placement::of(&pair.clone, &clone_baseline), &clone_baseline),
            Err(AdoptionMiss::Residue { name }) if name == "LEFTOVER"
        ));
    }
}
