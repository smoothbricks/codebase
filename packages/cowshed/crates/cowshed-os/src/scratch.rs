//! Shared-temp scratch ownership and bounded reclamation. No name or age proves death.

use std::fs::{self, OpenOptions, ReadDir};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const OWNER_FILE: &str = ".cowshed-owner";
const MAGIC: &str = "cowshed-scratch-v1";
/// Bound directory enumeration too, not just successful removals: a busy /tmp is the problem.
pub const PASS_ENTRIES: usize = 256;
pub const INTERVAL: Duration = Duration::from_secs(60);
/// Evidence gets a recovery window after the rename, not an expiry measured from creation.
pub const EVIDENCE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_OWNER_BYTES: u64 = 1024;
const MAX_SOCKET_ENTRIES: usize = 8;

/// The bytes a named child adds to its prefix: `-` and 16 characters, the length of the unowned
/// 16-hex name main gave every fixture (0520d27b4). A fixture under the workspace's runtime link
/// keeps its nested sockets inside `sockaddr_un.sun_path` only at this length.
const NAMED_SUFFIX: usize = 17;

/// The directory every test run on macOS shares: `/private/var/folders/cowshed/<euid>/t`, holding the
/// user's image template, the lane pools and the dead-run sweep's lock and record. Each run's own
/// tree is its test namespace's root beside it (`HostNamespace::macos_user_root`), never inside;
/// production's names in the per-user root are `mnt` and `ram<revision>`, and nothing production
/// runs reads `t` or a test namespace (specs/cowshed/01_storage.md, "Host namespace"). Spotlight's
/// `mds` admits no volume whose mount path contains `/var/folders/` and keeps, until it exits, one
/// `AppleAPFSVolume` right for every volume it admits; a volume mounted below the per-user root
/// takes none (specs/cowshed/01a_apfs.md, "Spotlight media references"). The per-user root
/// `cowshed setup` makes must exist; `t` is created 0700 and refused unless it is a directory this
/// euid owns with that mode.
#[cfg(target_os = "macos")]
pub fn macos_scratch_root() -> io::Result<PathBuf> {
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let root = PathBuf::from(format!("/private/var/folders/cowshed/{euid}/t"));
    match fs::DirBuilder::new().mode(0o700).create(&root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "create the scratch root {} (its parent is the per-user root `cowshed setup` makes): {error}",
                    root.display()
                ),
            ));
        }
    }
    let metadata = fs::symlink_metadata(&root)?;
    if !metadata.is_dir() || metadata.uid() != euid || metadata.mode() & 0o777 != 0o700 {
        return Err(io::Error::other(format!(
            "the scratch root {} must be a 0700 directory owned by uid {euid}",
            root.display()
        )));
    }
    Ok(root)
}

/// Create a private scratch root and publish its creating pid before it holds any output.
pub fn create(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)?;
    record_owners(path, &[std::process::id()])
}

/// Single-owner fixtures keep their payload pristine (it may itself be a Git or telemetry root).
/// Ownership is published atomically by mkdir in a reserved, explicitly pid-bearing name.
pub fn create_named(parent: &Path, prefix: &str) -> io::Result<PathBuf> {
    if Path::new(prefix).file_name() != Some(std::ffi::OsStr::new(prefix)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "scratch prefix must be one file name",
        ));
    }
    let mut attempt = 0;
    loop {
        let path = named_path(parent, prefix, std::process::id());
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => return Ok(path),
            // The nonce is 28 bits within one pid; mkdir never adopts a directory that exists.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && attempt < 8 => {
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// `<prefix>-p<8 hex pid><7 hex nonce>`, [`NAMED_SUFFIX`] bytes after the prefix. `p` is not a
/// hex digit, so no unowned `<prefix>-<16 hex>` name parses as an owner.
fn named_path(parent: &Path, prefix: &str, pid: u32) -> PathBuf {
    let (_, nonce) = uuid::Uuid::new_v4().as_u64_pair();
    parent.join(format!("{prefix}-p{pid:08x}{:07x}", nonce & 0x0fff_ffff))
}

/// The pid carried by a named scratch root of exactly `prefix`, admitted by the
/// same grammar as the production sweep. A longer family, an unowned name and a
/// non-positive or out-of-range pid are unknown, never ownership proof.
pub fn named_owner_for(path: &Path, prefix: &str) -> Option<i32> {
    let name = path.file_name()?.to_str()?;
    (name.strip_prefix(prefix)?.len() == NAMED_SUFFIX).then(|| named_owner(path))?
}

fn named_owner(path: &Path) -> Option<i32> {
    let name = path.file_name()?.to_str()?;
    if !name.starts_with("cowshed-") && !name.starts_with("cs-") {
        return None;
    }
    let (_, suffix) = name.rsplit_once('-')?;
    let digits = suffix.strip_prefix('p')?;
    if digits.len() != NAMED_SUFFIX - 2
        || !digits
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    i32::from_str_radix(&digits[..8], 16)
        .ok()
        .filter(|pid| *pid > 0)
}

/// Publish every process that can keep writing. An absent/partial record is unknown, never dead.
/// Detached children publish only after spawn, so death in that window leaves an unknown owner.
pub fn record_owners(path: &Path, pids: &[u32]) -> io::Result<()> {
    if pids.is_empty()
        || pids.len() > 32
        || pids
            .iter()
            .any(|pid| *pid == 0 || *pid > i32::MAX.unsigned_abs())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid scratch owner pid",
        ));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path.join(OWNER_FILE))?;
    writeln!(file, "{MAGIC}")?;
    for pid in pids {
        writeln!(file, "{pid}")?;
    }
    // A complete first pid is not a complete multi-writer lease if publication is interrupted.
    writeln!(file, "end")
}

#[derive(Debug, Eq, PartialEq)]
pub enum Disposition {
    Kept(String),
    Deleted,
    Archived(PathBuf),
    WouldDelete,
    WouldArchive,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Finding {
    pub path: PathBuf,
    pub disposition: Disposition,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.disposition {
            Disposition::Kept(reason) => {
                write!(f, "kept scratch {}: {reason}", self.path.display())
            }
            Disposition::Deleted => write!(
                f,
                "deleted socket scratch {}: recorded owners are dead",
                self.path.display()
            ),
            Disposition::Archived(to) => write!(
                f,
                "archived scratch {} to {}: recorded owners are dead; evidence retained for 24 hours",
                self.path.display(),
                to.display()
            ),
            Disposition::WouldDelete => write!(
                f,
                "would delete socket scratch {}: recorded owners are dead",
                self.path.display()
            ),
            Disposition::WouldArchive => write!(
                f,
                "would archive scratch {}: recorded owners are dead; may hold run evidence",
                self.path.display()
            ),
        }
    }
}

#[derive(Default, Debug)]
pub struct Report {
    pub examined: usize,
    pub findings: Vec<Finding>,
}

impl Report {
    pub fn print(&self) {
        for finding in &self.findings {
            crate::eprintln!("cowshed: {finding}");
        }
        crate::eprintln!(
            "cowshed: scratch examined {} temp entries (bound {PASS_ENTRIES}); RAM leases and their archives belong to bounded-exec acquire/release, not this sweep",
            self.examined
        );
    }
}

/// One swept root and its open listing, if any.
struct Root {
    path: PathBuf,
    entries: Option<ReadDir>,
}

/// What a pass does next at its roots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Turn {
    /// Open the root's listing, then answer [`Cursors::listed`].
    List(usize),
    /// Read the root's next entry, then answer [`Cursors::read`].
    Read(usize),
    /// The pass is over, having met this many entries.
    End(usize),
}

/// Where the sweep is among its roots, kept between passes: the root it turns to next, round
/// robin, and which roots have a listing open. A listing survives passes, so retained or live
/// entries cannot starve later dead ones, and a listing that ended is opened afresh next pass.
struct Cursors {
    next: usize,
    open: Vec<bool>,
}

/// One pass over the roots: the entries it may still meet, those it met, the roots it is done
/// with, and a root whose listing just opened, to read at once.
struct Pass {
    budget: usize,
    met: usize,
    done: Vec<bool>,
    opened: Option<usize>,
}

impl Cursors {
    fn new(roots: usize) -> Self {
        Self {
            next: 0,
            open: vec![false; roots],
        }
    }

    /// A pass that meets at most `budget` entries, an unreadable one included.
    fn pass(&self, budget: usize) -> Pass {
        Pass {
            budget,
            met: 0,
            done: vec![false; self.open.len()],
            opened: None,
        }
    }

    /// The pass's next turn.
    fn turn(&mut self, pass: &mut Pass) -> Turn {
        if let Some(index) = pass.opened.take() {
            return Turn::Read(index);
        }
        while pass.met < pass.budget && pass.done.iter().any(|done| !done) {
            let index = self.next;
            self.next = (index + 1) % self.open.len();
            if !pass.done[index] {
                return if self.open[index] {
                    Turn::Read(index)
                } else {
                    Turn::List(index)
                };
            }
        }
        Turn::End(pass.met)
    }

    /// Root `index`'s listing opened, or could not be opened and is passed over for the rest of
    /// this pass.
    fn listed(&mut self, pass: &mut Pass, index: usize, opened: bool) {
        if opened {
            self.open[index] = true;
            pass.opened = Some(index);
        } else {
            pass.done[index] = true;
        }
    }

    /// Root `index`'s listing gave an entry, readable or not, which counts against the pass's
    /// bound; or it ended, and the next pass opens it afresh.
    fn read(&mut self, pass: &mut Pass, index: usize, entry: bool) {
        if entry {
            pass.met += 1;
        } else {
            self.open[index] = false;
            pass.done[index] = true;
        }
    }
}

/// The host's scratch sweep over its roots' direct children only: RAM leases, their ledger,
/// staging and archives are not ours.
pub struct Reaper {
    roots: Vec<Root>,
    cursors: Cursors,
}

impl Reaper {
    pub fn new(roots: impl IntoIterator<Item = PathBuf>) -> io::Result<Self> {
        let mut unique = Vec::<Root>::new();
        for path in roots {
            let path = fs::canonicalize(path)?;
            if !unique.iter().any(|root| root.path == path) {
                unique.push(Root {
                    path,
                    entries: None,
                });
            }
        }
        Ok(Self {
            cursors: Cursors::new(unique.len()),
            roots: unique,
        })
    }

    pub fn host() -> io::Result<Self> {
        let temp = fs::canonicalize(std::env::temp_dir())?;
        Self::new(host_roots(temp))
    }

    pub fn sweep(&mut self, store: &Path, dry_run: bool) -> Report {
        let mut findings = Vec::new();
        let mut pass = self.cursors.pass(PASS_ENTRIES);
        let examined = loop {
            match self.cursors.turn(&mut pass) {
                Turn::List(index) => {
                    let root = &mut self.roots[index];
                    let opened = match fs::read_dir(&root.path) {
                        Ok(entries) => {
                            root.entries = Some(entries);
                            true
                        }
                        Err(error) => {
                            findings.push(kept(&root.path, format!("cannot enumerate: {error}")));
                            false
                        }
                    };
                    self.cursors.listed(&mut pass, index, opened);
                }
                Turn::Read(index) => {
                    let root = &mut self.roots[index];
                    let entry = match root.entries.as_mut().and_then(Iterator::next) {
                        None => {
                            root.entries = None;
                            false
                        }
                        Some(Err(error)) => {
                            findings.push(kept(&root.path, format!("cannot read entry: {error}")));
                            true
                        }
                        Some(Ok(entry)) => {
                            findings.extend(inspect(&entry.path(), store, dry_run));
                            true
                        }
                    };
                    self.cursors.read(&mut pass, index, entry);
                }
                Turn::End(examined) => break examined,
            }
        };
        Report { examined, findings }
    }
}

/// The roots a host sweep may enter. Bounded-exec gives its command a RAM
/// lease as TMPDIR (a child of the volume ram-temp.ts mounts below the
/// per-user root), so a daemon or gc started under such a command would
/// otherwise sweep inside that lease: bounded-exec alone reclaims leases,
/// under its own lock and ledger, and this selection is where that owner
/// boundary is kept (specs/cowshed/01_storage.md, Shared temporary scratch).
fn host_roots(temp: PathBuf) -> Vec<PathBuf> {
    let mut roots = vec![PathBuf::from("/tmp")];
    if !is_ram_temp_owned(&temp) {
        roots.push(temp);
    }
    roots
}

/// Whether a canonical path is at or below a bounded-exec RAM temp volume.
/// The names count only in the slots ram-temp.ts mounts them in: the leaf
/// ram<revision> (and its ram<revision>-new staging sibling) directly below
/// a namespace's per-user root, production's /private/var/folders/cowshed/<uid>
/// or a test namespace's directory below it, or a volume-name mount under
/// /Volumes (revisions 1-2). A ram-shaped name anywhere else is an ordinary
/// temp root and stays in the sweep.
fn is_ram_temp_owned(path: &Path) -> bool {
    let parts: Vec<String> = path
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.len() >= 3 && parts[1] == "Volumes" && parts[2].starts_with("smoo-ram") {
        return true;
    }
    if parts.len() < 7
        || parts[1..5] != ["private", "var", "folders", "cowshed"]
        || !digits(&parts[5])
    {
        return false;
    }
    if ram_mountpoint(&parts[6]) {
        return true;
    }
    crate::namespace::TestNamespace::parse(&parts[6]).is_some()
        && parts.get(7).is_some_and(|name| ram_mountpoint(name))
}

fn ram_mountpoint(name: &str) -> bool {
    let Some(revision) = name.strip_prefix("ram") else {
        return false;
    };
    digits(revision.strip_suffix("-new").unwrap_or(revision))
}

fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn kept(path: &Path, reason: impl Into<String>) -> Finding {
    Finding {
        path: path.to_owned(),
        disposition: Disposition::Kept(reason.into()),
    }
}

fn foreign(name: &str) -> bool {
    [
        "node_", "bash_", "cargo_", "bun_", "devenv-", "sccache_", "smoo-ram",
    ]
    .iter()
    .any(|prefix| name.starts_with(prefix))
}

fn owners(path: &Path) -> io::Result<Vec<i32>> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path.join(OWNER_FILE))
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return named_owner(path).map(|pid| vec![pid]).ok_or(error);
        }
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    // SAFETY: geteuid has no preconditions.
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
        || metadata.mode() & 0o022 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "owner record is not a private regular file",
        ));
    }
    let mut text = String::new();
    file.take(MAX_OWNER_BYTES + 1).read_to_string(&mut text)?;
    owner_record(&text)
}

/// The pids an owner record's `text` publishes: the magic line, one positive pid per line, at
/// least one, then `end`, within [`MAX_OWNER_BYTES`]. Anything else, a record torn before its
/// `end` included, is an unknown owner, never a dead one.
fn owner_record(text: &str) -> io::Result<Vec<i32>> {
    let mut lines = text.lines();
    if text.len() > usize::try_from(MAX_OWNER_BYTES).expect("small constant")
        || !text.ends_with('\n')
        || lines.next() != Some(MAGIC)
        || lines.next_back() != Some("end")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown owner record format",
        ));
    }
    lines
        .map(|line| line.parse::<i32>().ok().filter(|pid| *pid > 0))
        .collect::<Option<Vec<_>>>()
        .filter(|pids| !pids.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown owner pid"))
}

/// Why a sweep keeps an entry with a cowshed name rather than reclaim it.
#[derive(Debug)]
enum Keep {
    /// Real-APFS fixture scratch, which its detach-aware sweep reclaims.
    ApfsFixture,
    Uninspected(io::Error),
    NotPrivate,
    UnknownOwner(io::Error),
    LiveOwner(i32),
    UninspectableOwner(i32, io::Error),
}

impl std::fmt::Display for Keep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApfsFixture => {
                f.write_str("APFS fixture scratch belongs to the detach-aware test sweep")
            }
            Self::Uninspected(error) => write!(f, "cannot inspect: {error}"),
            Self::NotPrivate => f.write_str("not a private directory of this user"),
            Self::UnknownOwner(error) => write!(f, "unknown owner: {error}"),
            Self::LiveOwner(pid) => write!(f, "owner pid {pid} is live (or cannot be inspected)"),
            Self::UninspectableOwner(pid, error) => {
                write!(f, "owner pid {pid} cannot be inspected: {error}")
            }
        }
    }
}

/// What a sweep does with one temp entry.
#[derive(Debug)]
enum Verdict<M> {
    /// Not cowshed's to judge: another tool's name, or an entry cowshed never named or owned.
    Pass,
    Keep(Keep),
    /// Every recorded owner is dead: reclaim the private directory observed as `M`.
    Reclaim(M),
}

/// What a sweep asks of one temp entry next, or its verdict. Each observation is asked only when
/// the decision needs it: another tool's entry is never stat'ed, an owner record is read only in
/// a private directory of this user, and liveness only of a complete record's owners. No name or
/// age proves death: only every recorded owner observed dead reclaims, and an entry with a cowshed
/// name that cannot be judged is kept, named.
#[derive(Debug)]
enum Look<M> {
    /// Stat the entry without following a link; [`Look::metadata`] takes the answer.
    Metadata {
        known: bool,
    },
    /// Read its owner record; [`Look::owners`] takes the answer.
    Owners {
        known: bool,
        entry: M,
    },
    /// Ask each owner's liveness; [`Look::liveness`] takes the answers in the record's order.
    Liveness {
        entry: M,
        owners: Vec<i32>,
    },
    Verdict(Verdict<M>),
}

impl<M> Look<M> {
    /// What the entry's name alone decides.
    fn name(name: &str) -> Self {
        if foreign(name) {
            Self::Verdict(Verdict::Pass)
        } else if name.starts_with("cowshed-itest-") {
            Self::Verdict(Verdict::Keep(Keep::ApfsFixture))
        } else {
            Self::Metadata {
                known: name.starts_with("cowshed-") || name.starts_with("cs-"),
            }
        }
    }

    /// The entry, stat'ed without following a link, as a private directory of this user (`Some`,
    /// kept for its reclamation) or as anything else.
    fn metadata(known: bool, private: io::Result<Option<M>>) -> Self {
        let unjudged = |keep| {
            Self::Verdict(if known {
                Verdict::Keep(keep)
            } else {
                Verdict::Pass
            })
        };
        match private {
            Ok(Some(entry)) => Self::Owners { known, entry },
            Ok(None) => unjudged(Keep::NotPrivate),
            Err(error) => unjudged(Keep::Uninspected(error)),
        }
    }

    /// The entry's owner record as [`owner_record`] read it.
    fn owners(known: bool, entry: M, owners: io::Result<Vec<i32>>) -> Self {
        match owners {
            Ok(owners) => Self::Liveness { entry, owners },
            Err(error) if error.kind() == io::ErrorKind::NotFound && !known => {
                Self::Verdict(Verdict::Pass)
            }
            Err(error) => Self::Verdict(Verdict::Keep(Keep::UnknownOwner(error))),
        }
    }

    /// Each recorded owner's observed liveness, in the record's order: the first not observed dead
    /// keeps the entry, naming it. No observed owner is no proof that every owner is dead: an empty
    /// set is an unknown owner, never a reclaim.
    fn liveness(entry: M, observed: Vec<(i32, io::Result<bool>)>) -> Self {
        if observed.is_empty() {
            return Self::Verdict(Verdict::Keep(Keep::UnknownOwner(io::Error::new(
                io::ErrorKind::InvalidData,
                "the owner record names no owner",
            ))));
        }
        for (pid, running) in observed {
            match running {
                Ok(false) => {}
                Ok(true) => return Self::Verdict(Verdict::Keep(Keep::LiveOwner(pid))),
                Err(error) => {
                    return Self::Verdict(Verdict::Keep(Keep::UninspectableOwner(pid, error)));
                }
            }
        }
        Self::Verdict(Verdict::Reclaim(entry))
    }
}

/// Whether `metadata`, read without following a link, is a directory this user owns that no other
/// user can write: the only entry a sweep judges, so it never crosses an ownership boundary or
/// inspects a mutable public directory.
fn private_directory(metadata: &fs::Metadata) -> bool {
    // SAFETY: geteuid has no preconditions.
    metadata.is_dir()
        && metadata.uid() == unsafe { libc::geteuid() }
        && metadata.mode() & 0o022 == 0
}

fn inspect(path: &Path, store: &Path, dry_run: bool) -> Option<Finding> {
    let name = path.file_name()?.to_string_lossy();
    let mut look = Look::name(&name);
    let verdict = loop {
        look = match look {
            Look::Metadata { known } => Look::metadata(
                known,
                fs::symlink_metadata(path)
                    .map(|metadata| private_directory(&metadata).then_some(metadata)),
            ),
            Look::Owners { known, entry } => Look::owners(known, entry, owners(path)),
            Look::Liveness { entry, owners } => {
                let observed = owners
                    .into_iter()
                    .map(|pid| (pid, crate::process::running(pid)))
                    .collect();
                Look::liveness(entry, observed)
            }
            Look::Verdict(verdict) => break verdict,
        };
    };
    match verdict {
        Verdict::Pass => None,
        Verdict::Keep(keep) => Some(kept(path, keep.to_string())),
        Verdict::Reclaim(metadata) => Some(match reclaim(path, store, dry_run, &metadata) {
            Ok(disposition) => Finding {
                path: path.to_owned(),
                disposition,
            },
            Err(error) => kept(path, format!("reclamation failed: {error}")),
        }),
    }
}

/// Only this tiny, closed grammar is disposable. Anything else may be the only run evidence.
fn socket_files(path: &Path) -> io::Result<Option<Vec<PathBuf>>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)?.take(MAX_SOCKET_ENTRIES + 1) {
        let entry = entry?;
        if files.len() == MAX_SOCKET_ENTRIES {
            return Ok(None);
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        let name = entry.file_name();
        let disposable = if name == OWNER_FILE {
            metadata.is_file() && metadata.len() <= MAX_OWNER_BYTES
        } else if name == "s" || name == "s.sock" {
            metadata.file_type().is_socket()
        } else if name == "s.sock.lock" {
            metadata.is_file() && metadata.len() == 0
        } else {
            false
        };
        if !disposable {
            return Ok(None);
        }
        files.push(entry.path());
    }
    Ok(Some(files))
}

/// What reclaiming a dead owner's directory does: socket scratch, the closed grammar
/// [`socket_files`] admits as `F`, is deleted; anything else may be the only run evidence and is
/// archived whole. A dry run says which and changes nothing.
#[derive(Debug, Eq, PartialEq)]
enum Plan<F> {
    Delete(F),
    Archive,
    WouldDelete,
    WouldArchive,
}

fn plan<F>(sockets: Option<F>, dry_run: bool) -> Plan<F> {
    match (sockets, dry_run) {
        (Some(_), true) => Plan::WouldDelete,
        (None, true) => Plan::WouldArchive,
        (Some(files), false) => Plan::Delete(files),
        (None, false) => Plan::Archive,
    }
}

fn reclaim(
    path: &Path,
    store: &Path,
    dry_run: bool,
    original: &fs::Metadata,
) -> io::Result<Disposition> {
    let sockets = socket_files(path)?;
    let _socket_lease = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path.join("s.sock.lock"))
    {
        Ok(file) => {
            let lease = crate::fork_lock::Fenced::new(file);
            lease.try_lock().map_err(|error| {
                io::Error::other(format!("socket lease is held or unreadable: {error}"))
            })?;
            Some(lease)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let now = fs::symlink_metadata(path)?;
    if !now.is_dir() || (now.dev(), now.ino()) != (original.dev(), original.ino()) {
        return Err(io::Error::other("scratch directory identity changed"));
    }
    let sockets = match plan(sockets, dry_run) {
        Plan::WouldDelete => return Ok(Disposition::WouldDelete),
        Plan::WouldArchive => return Ok(Disposition::WouldArchive),
        Plan::Delete(files) => Some(files),
        Plan::Archive => None,
    };
    if let Some(files) = sockets {
        // No recursive removal: a concurrent unexpected writer leaves its output in place.
        for file in files
            .iter()
            .filter(|file| file.file_name().is_some_and(|name| name != OWNER_FILE))
        {
            fs::remove_file(file)?;
        }
        if files
            .iter()
            .any(|file| file.file_name().is_some_and(|name| name == OWNER_FILE))
        {
            fs::remove_file(path.join(OWNER_FILE))?;
        }
        fs::remove_dir(path)?;
        Ok(Disposition::Deleted)
    } else {
        let trash = store.join(".trash/scratch");
        crate::fsio::AnchoredDirectory::create(&trash)?;
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();
        let destination = trash.join(format!("{seconds}-{}", uuid::Uuid::new_v4().simple()));
        if let Some(pid) = named_owner(path) {
            match fs::symlink_metadata(path.join(OWNER_FILE)) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    record_owners(path, &[u32::try_from(pid).map_err(io::Error::other)?])?;
                }
                Err(error) => return Err(error),
            }
        }
        // A cross-volume rename is kept and reported, never converted into a copy/delete.
        fs::rename(path, &destination)?;
        Ok(Disposition::Archived(destination))
    }
}

#[cfg(target_os = "macos")]
fn mount_identity(_path: &Path, metadata: &fs::Metadata) -> io::Result<u64> {
    Ok(metadata.dev())
}

#[cfg(target_os = "linux")]
fn mount_identity(path: &Path, _metadata: &fs::Metadata) -> io::Result<u64> {
    let directory = crate::fsio::open_directory_nofollow(path)?;
    crate::fsio::mount_id(&directory)
}

/// One archived tree's background removal, at most 256 directory entries per step. A mounted
/// descendant is retained, including a Linux bind mount on the same device.
#[derive(Default)]
pub struct TrashReaper {
    entries: Option<ReadDir>,
    walk: Vec<(PathBuf, ReadDir)>,
    device: u64,
}

impl TrashReaper {
    /// One entry of the archive being walked: a file removed, a directory entered, or a finished
    /// directory removed. The archive root's ownership record goes last, once a fresh listing
    /// shows nothing else in it, and comes back if the root then cannot be removed: the archive
    /// stays owned through every failure.
    fn step(&mut self) -> io::Result<usize> {
        let archive_root = self.walk.len() == 1;
        let Some((path, children)) = self.walk.last_mut() else {
            return Ok(0);
        };
        match children.next() {
            Some(entry) => {
                let entry = entry?;
                if archive_root && entry.file_name() == OWNER_FILE {
                    return Ok(0);
                }
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_dir() {
                    if mount_identity(&entry.path(), &metadata)? != self.device {
                        return Err(io::Error::other(format!(
                            "holds mounted directory {}",
                            entry.path().display()
                        )));
                    }
                    let children = fs::read_dir(entry.path())?;
                    self.walk.push((entry.path(), children));
                    Ok(0)
                } else {
                    fs::remove_file(entry.path())?;
                    Ok(1)
                }
            }
            None => {
                if archive_root {
                    for entry in fs::read_dir(&*path)? {
                        let name = entry?.file_name();
                        if name != OWNER_FILE {
                            return Err(io::Error::other(format!(
                                "still holds {}",
                                name.to_string_lossy()
                            )));
                        }
                    }
                    // The record is the archive's only proof of ownership: should the directory outlive
                    // its removal, the same bytes go back before the failure is reported.
                    let record = path.join(OWNER_FILE);
                    let mut bytes = Vec::new();
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
                        .open(&record)?
                        .take(MAX_OWNER_BYTES + 1)
                        .read_to_end(&mut bytes)?;
                    fs::remove_file(&record)?;
                    if let Err(error) = fs::remove_dir(&*path) {
                        let restored = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .mode(0o600)
                            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                            .open(&record)
                            .and_then(|mut file| file.write_all(&bytes));
                        return Err(match restored {
                            Ok(()) => error,
                            Err(lost) => io::Error::new(
                                error.kind(),
                                format!("{error}; its ownership record was not restored: {lost}"),
                            ),
                        });
                    }
                } else {
                    fs::remove_dir(&*path)?;
                }
                self.walk.pop();
                Ok(1)
            }
        }
    }

    pub fn sweep(&mut self, store: &Path, now: SystemTime) -> io::Result<usize> {
        let trash = store.join(".trash/scratch");
        if self.entries.is_none() {
            match crate::fsio::AnchoredDirectory::open(&trash) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
                Err(error) => return Err(error),
            }
            self.entries = Some(fs::read_dir(&trash)?);
        }
        let mut removed = 0;
        for _ in 0..PASS_ENTRIES {
            if !self.walk.is_empty() {
                match self.step() {
                    Ok(count) => removed += count,
                    // The archive keeps its ownership record. Giving up its walk lets the archives
                    // behind it take their turn; the next full listing walks it again from its root.
                    Err(error) => {
                        let (archive, _) = self.walk.swap_remove(0);
                        self.walk.clear();
                        return Err(io::Error::new(
                            error.kind(),
                            format!("kept scratch archive {}: {error}", archive.display()),
                        ));
                    }
                }
            } else {
                let Some(entry) = self.entries.as_mut().and_then(Iterator::next) else {
                    self.entries = None;
                    break;
                };
                let entry = entry?;
                let path = entry.path();
                let name = entry.file_name();
                let Some((seconds, suffix)) = name.to_str().and_then(|name| name.split_once('-'))
                else {
                    continue;
                };
                let Some(created) = seconds
                    .parse::<u64>()
                    .ok()
                    .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)))
                else {
                    continue;
                };
                if suffix.len() != 32 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    crate::eprintln!(
                        "cowshed: kept unknown scratch trash entry {}",
                        path.display()
                    );
                    continue;
                }
                if now.duration_since(created).unwrap_or_default() < EVIDENCE_RETENTION {
                    crate::eprintln!(
                        "cowshed: kept scratch archive {}: 24-hour evidence retention",
                        path.display()
                    );
                    continue;
                }
                let metadata = fs::symlink_metadata(&path)?;
                // The rename into the private trash is the completed lease. Unknown entries stay.
                // SAFETY: geteuid has no preconditions.
                if !metadata.is_dir()
                    || metadata.uid() != unsafe { libc::geteuid() }
                    || metadata.mode() & 0o022 != 0
                {
                    crate::eprintln!(
                        "cowshed: kept scratch trash entry {}: not an owned directory",
                        path.display()
                    );
                    continue;
                }
                if let Err(error) = owners(&path) {
                    crate::eprintln!(
                        "cowshed: kept scratch archive {}: unknown ownership: {error}",
                        path.display()
                    );
                    continue;
                }
                self.device = mount_identity(&path, &metadata)?;
                if self.device != mount_identity(&trash, &fs::symlink_metadata(&trash)?)? {
                    crate::eprintln!(
                        "cowshed: kept scratch archive {}: archive root is mounted",
                        path.display()
                    );
                    continue;
                }
                self.walk.push((path.clone(), fs::read_dir(path)?));
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests;
