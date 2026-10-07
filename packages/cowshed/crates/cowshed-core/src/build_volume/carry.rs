//! Carry (16_build_volumes.md, "Carry"): before a target adopts a landing volume, every Nx
//! cache entry the target's current volume indexes and the landing volume does not is copied
//! into the landing volume and indexed there.
//!
//! An Nx cache entry is content-addressed: its hash names exactly the task inputs it answers, so
//! an entry is a correct hit for any tree that computes that hash. The landing volume holds what
//! its workspace forked from plus what it ran; the target's volume also holds what other lands
//! and the target itself ran meanwhile. Without the carry, adopting one land's volume discarded
//! every other land's entries, and a fork made after the next land missed tasks main had already
//! run at that very tree.
//!
//! Rebase uses [`stage_rebase`] instead: only current-tree task hashes are selected, including
//! during commit, and one row/byte/free-space budget bounds all copies in both phases. Regular
//! file data is copied through a length-limited reader, with `copyfile(3)` preserving metadata.
//!
//! Stock Nx 23.2.1 keeps an entry in three places (`native/cache/cache.rs`): the directory
//! `<cache>/<hash>` with the task's outputs, the file `<cache>/terminalOutputs/<hash>`, and a
//! `cache_outputs` row in the task database `<workspace-data>/<machine>-v<schema>.db`, whose
//! `task_details` row it references. `put` deletes the entry's directory, writes the files, and
//! upserts the row last, stamping `created_at`; eviction deletes the row first, then the files;
//! `get` answers a hit only for a row. The carry keeps that order.
//!
//! It runs in two phases, so the target's task database is closed for milliseconds, as before:
//!
//! 1. [`stage`], while the target still runs: read the rows the landing volume lacks from the
//!    target's database (one SQLite read, its write-ahead log included), and copy each entry's
//!    files into the landing volume's staging directory, [`STAGING`]. Nothing indexes them yet,
//!    so nothing reads them. A row is written after its files, so a staged entry is whole unless
//!    Nx deleted or rewrote it during the copy, which changes its row.
//! 2. [`commit`], once the land has closed the target's database: an entry whose row is unchanged
//!    since phase 1 moves into place by `rename(2)`; one whose row changed or vanished is
//!    dropped. Entries the target indexed after phase 1 are copied now, nothing writing either
//!    side. Every placed entry's rows are inserted in one transaction, then the staging
//!    directory is deleted.
//!
//! The entries cross from one image to another, so they are copied, never cloned;
//! `copyfile(3)` keeps every file's bytes, mode and times. A crashed carry leaves only the
//! staging directory, which the next [`stage`] or [`unstage`] on that volume deletes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::BuildVolumeState;
use super::sqlite::Connection;

/// The landing volume's staging directory for entries not yet indexed, at its root and outside
/// every tool's namespace.
pub const STAGING: &str = ".carry";
/// Where stock Nx keeps each entry's terminal output, inside its cache directory.
const TERMINAL_OUTPUTS: &str = "terminalOutputs";
/// Data plus every attribute `copyfile(3)` carries, mode and times included.
const COPYFILE_ALL: libc::copyfile_flags_t = libc::COPYFILE_METADATA | libc::COPYFILE_DATA;

/// The target's entries the landing volume lacks, most recently used first, so a carry that
/// stops has carried the ones most likely to hit. Nx names an entry by a decimal hash
/// (`^\d+$`); any other row names no directory the carry could safely join.
const MISSING: &str = "SELECT p.hash, p.code, p.size, p.created_at FROM previous.cache_outputs p \
     WHERE p.hash <> '' AND p.hash NOT GLOB '*[^0-9]*' \
       AND NOT EXISTS (SELECT 1 FROM main.cache_outputs m WHERE m.hash = p.hash) \
     ORDER BY p.accessed_at DESC";
// CROSS JOIN fixes the loop order: wanted hashes probe p's primary key, never scan its history.
const SELECTED: &str = "SELECT p.hash, p.code, p.size, p.created_at FROM temp.wanted w \
     CROSS JOIN previous.cache_outputs p \
     WHERE p.hash = w.hash AND p.hash <> '' AND p.hash NOT GLOB '*[^0-9]*' \
       AND NOT EXISTS (SELECT 1 FROM main.cache_outputs m WHERE m.hash = p.hash) \
     ORDER BY p.accessed_at DESC";

/// One entry's row in the target's database, as phase 1 read it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Row {
    hash: String,
    code: i64,
    size: i64,
    created_at: String,
}

/// Phase 1's work for one pair of task databases.
#[derive(Debug)]
struct StagedDatabase {
    selection: OwnedSelection,
    from_database: PathBuf,
    from_cache: PathBuf,
    into_database: PathBuf,
    into_cache: PathBuf,
    /// This pair's directory under [`STAGING`].
    staging: PathBuf,
    /// The entries whose files were staged, each with the row read before its copy.
    rows: Vec<Row>,
}

/// What phase 1 staged, for [`commit`].
#[derive(Debug, Default)]
pub struct Staged {
    databases: Vec<StagedDatabase>,
    budget: Option<Budget>,
    /// Why staging stopped short. The entries staged until then are committed; nothing more is
    /// copied while the target is closed.
    stopped: Option<String>,
}

/// What a carry put into the landing volume.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Carried {
    pub entries: u64,
    /// The bytes Nx recorded for the carried entries.
    pub bytes: u64,
    /// Why the carry stopped short, naming the database or entry.
    pub stopped: Option<String>,
}

/// One Nx state in a volume: its cache directory and its `workspace-data` directory, both
/// relative to the volume's root, for the checkout's `.nx` directory `checkout`.
struct NxState<'state> {
    checkout: &'state Path,
    cache: &'state Path,
    data: &'state Path,
}

/// Phase 1: stage every Nx cache entry the volume at `from` indexes and the quiet volume at
/// `into` does not, for each Nx state both volumes hold under the same checkout path and each
/// task database both hold under the same name. A landing Nx state with a task database of
/// another name belongs to an Nx of another schema, so the target's is not carried into it; one
/// with none never ran Nx, and takes the target's database without its cache rows first.
pub fn stage(
    from: &Path,
    from_state: &BuildVolumeState,
    into: &Path,
    into_state: &BuildVolumeState,
) -> Staged {
    stage_with(from, from_state, into, into_state, None, Staged::default())
}

/// Rebase carry admits only hashes Nx computed for the rebased tree, across both phases.
/// Historical entries consume no copy bandwidth or destination space. The entire carry shares
/// one row and byte budget; missing selection information stops it, never widens it.
pub fn stage_rebase(
    from: &Path,
    from_state: &BuildVolumeState,
    into: &Path,
    into_state: &BuildVolumeState,
    selections: &BTreeMap<PathBuf, BTreeSet<String>>,
    backing_store: &Path,
) -> Staged {
    let rows = selections
        .values()
        .try_fold(0_usize, |sum, hashes| sum.checked_add(hashes.len()));
    let budget = rows
        .ok_or_else(|| io::Error::other("rebase carry task-hash count overflow"))
        .and_then(|rows| Budget::new(into, backing_store, rows));
    let budget = match budget {
        Ok(budget) => budget,
        Err(error) => {
            return Staged {
                stopped: Some(format!(
                    "budget rebase carry into {}: {error}",
                    into.display()
                )),
                ..Staged::default()
            };
        }
    };
    stage_with(
        from,
        from_state,
        into,
        into_state,
        Some(selections),
        Staged {
            budget: Some(budget),
            ..Staged::default()
        },
    )
}

fn stage_with(
    from: &Path,
    from_state: &BuildVolumeState,
    into: &Path,
    into_state: &BuildVolumeState,
    selections: Option<&BTreeMap<PathBuf, BTreeSet<String>>>,
    mut staged: Staged,
) -> Staged {
    let staging = into.join(STAGING);
    if let Err(error) = remove(&staging) {
        staged.stopped = Some(format!("clear {}: {error}", staging.display()));
        return staged;
    }
    let targets = nx_states(into_state);
    for target in &targets {
        if let Err(error) = selection(selections, target.checkout) {
            staged.stopped = Some(error.to_string());
            return staged;
        }
    }
    for source in nx_states(from_state) {
        let Some(destination) = targets
            .iter()
            .find(|state| state.checkout == source.checkout)
        else {
            continue;
        };
        let selected = selection(selections, source.checkout)
            .expect("each destination selection was checked before staging");
        if matches!(selected, Selection::Only(hashes) if hashes.is_empty()) {
            continue;
        }
        let owned = match selected {
            Selection::All => OwnedSelection::All,
            Selection::Only(hashes) => OwnedSelection::Only(Arc::new(hashes.clone())),
        };
        let databases = match databases_in(&from.join(source.data)) {
            Ok(databases) => databases,
            Err(error) => {
                staged.stopped = Some(format!(
                    "list {}: {error}",
                    from.join(source.data).display()
                ));
                return staged;
            }
        };
        let ran_nx = match databases_in(&into.join(destination.data)) {
            Ok(databases) => !databases.is_empty(),
            Err(error) => {
                staged.stopped = Some(format!(
                    "list {}: {error}",
                    into.join(destination.data).display()
                ));
                return staged;
            }
        };
        for name in databases {
            let from_database = from.join(source.data).join(&name);
            let into_database = into.join(destination.data).join(&name);
            if !into_database.is_file() {
                if ran_nx {
                    continue;
                }
                if matches!(selected, Selection::Only(_)) {
                    staged.stopped = Some(format!(
                        "rebase carry destination {} has no task database after current-tree hashing; refusing to copy the target's unbounded task history",
                        into_database.display()
                    ));
                    return staged;
                }
                if let Err(error) = empty_copy(&from_database, &into_database) {
                    staged.stopped = Some(format!(
                        "copy {} without its cache rows to {}: {error}",
                        from_database.display(),
                        into_database.display()
                    ));
                    return staged;
                }
            }
            let mut database = StagedDatabase {
                selection: owned.clone(),
                from_database,
                from_cache: from.join(source.cache),
                into_database,
                into_cache: into.join(destination.cache),
                staging: staging.join(staged.databases.len().to_string()),
                rows: Vec::new(),
            };
            let result = stage_database(&mut database, selected, staged.budget.as_mut());
            staged.databases.push(database);
            if let Err(error) = result {
                staged.stopped = Some(error);
                return staged;
            }
        }
    }
    staged
}

#[derive(Clone, Copy)]
enum Selection<'a> {
    All,
    Only(&'a BTreeSet<String>),
}

#[derive(Clone, Debug)]
enum OwnedSelection {
    All,
    Only(Arc<BTreeSet<String>>),
}

impl OwnedSelection {
    fn borrowed(&self) -> Selection<'_> {
        match self {
            Self::All => Selection::All,
            Self::Only(hashes) => Selection::Only(hashes),
        }
    }
}

fn selection<'a>(
    selections: Option<&'a BTreeMap<PathBuf, BTreeSet<String>>>,
    checkout: &Path,
) -> io::Result<Selection<'a>> {
    match selections {
        None => Ok(Selection::All),
        Some(selections) => selections
            .get(checkout)
            .map(Selection::Only)
            .ok_or_else(|| {
                io::Error::other(format!(
                    "rebase carry has no current-tree task hashes for {}",
                    checkout.display()
                ))
            }),
    }
}

/// Write a consistent copy of the task database `from` at `into` (`VACUUM INTO`, which reads
/// one snapshot of a database another process may be writing), then delete its cache rows and
/// its records of running processes: what remains is the target's task details and history,
/// in its own Nx's schema, indexing no entry the landing volume lacks. Built beside `into` and
/// renamed onto it, so `into` is either absent or whole.
fn empty_copy(from: &Path, into: &Path) -> io::Result<()> {
    let mut partial = into.as_os_str().to_owned();
    partial.push(".carry");
    let partial = PathBuf::from(partial);
    remove(&partial)?;
    if let Some(parent) = into.parent() {
        fs::create_dir_all(parent)?;
    }
    let result = (|| {
        Connection::open(from)?.execute("VACUUM INTO ?1", &[&partial.to_string_lossy()])?;
        let copy = Connection::open(&partial)?;
        for table in ["cache_outputs", "running_tasks", "task_invocations"] {
            if table_definition(&copy, "main", table)?.is_some() {
                copy.execute(&format!("DELETE FROM {table}"), &[])?;
            }
        }
        drop(copy);
        fs::rename(&partial, into)
    })();
    if result.is_err() {
        remove(&partial)?;
    }
    result
}

/// Phase 2, once nothing holds the target's task database: move each staged entry whose row is
/// unchanged into place, copy what the target indexed since phase 1, and index it all.
pub fn commit(mut staged: Staged, into: &Path) -> Carried {
    let mut carried = Carried {
        stopped: staged.stopped.clone(),
        ..Carried::default()
    };
    let complete = staged.stopped.is_none();
    for database in &staged.databases {
        let result = commit_database(
            database,
            complete,
            database.selection.borrowed(),
            staged.budget.as_mut(),
        );
        match result {
            Ok((entries, bytes, stopped)) => {
                carried.entries += entries;
                carried.bytes += bytes;
                if let Some(stopped) = stopped {
                    carried.stopped.get_or_insert(stopped);
                    break;
                }
            }
            Err(error) => {
                carried.stopped.get_or_insert(format!(
                    "carry {} into {}: {error}",
                    database.from_database.display(),
                    database.into_database.display()
                ));
                break;
            }
        }
    }
    if let Err(error) = unstage(into) {
        carried.stopped.get_or_insert(error.to_string());
    }
    carried
}

/// Delete what phase 1 staged in the volume at `into`, for a carry that will not commit.
pub fn unstage(into: &Path) -> io::Result<()> {
    let staging = into.join(STAGING);
    remove(&staging).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("delete {}: {error}", staging.display()),
        )
    })
}

/// Read the rows `database.into_database` lacks from `database.from_database`, then copy each
/// entry's files into the staging directory, recording each entry staged whole.
fn stage_database(
    database: &mut StagedDatabase,
    selected: Selection<'_>,
    mut budget: Option<&mut Budget>,
) -> Result<(), String> {
    let context = |error: io::Error| {
        format!(
            "stage {} into {}: {error}",
            database.from_database.display(),
            database.into_database.display()
        )
    };
    // The connection closes before the copies: phase 1 holds the target's database only for
    // the one read.
    let missing =
        missing(&database.into_database, &database.from_database, selected).map_err(context)?;
    for row in missing {
        match copy_row(
            &database.from_cache,
            &database.staging,
            &row,
            budget.as_deref_mut(),
        ) {
            Ok(true) => database.rows.push(row),
            Ok(false) => {}
            Err(error) => {
                return Err(format!(
                    "stage entry {} from {}: {error}",
                    row.hash,
                    database.from_cache.display()
                ));
            }
        }
    }
    Ok(())
}

/// Open the landing database with the target's attached as `previous`. Stock Nx creates
/// `cache_outputs` only once a task runs through its cache (`CREATE TABLE IF NOT EXISTS` in
/// `native/cache/cache.rs`), so a database whose checkout never ran one lacks it: the landing
/// side then takes each table the carry writes from the target's own definition, which is the
/// one its Nx wrote.
fn attached(into_database: &Path, from_database: &Path) -> io::Result<Connection> {
    let connection = Connection::open(into_database)?;
    connection.execute(
        "ATTACH DATABASE ?1 AS previous",
        &[&from_database.to_string_lossy()],
    )?;
    for table in ["task_details", "cache_outputs"] {
        if table_definition(&connection, "main", table)?.is_none()
            && let Some(definition) = table_definition(&connection, "previous", table)?
        {
            connection.execute(&definition, &[])?;
        }
    }
    Ok(connection)
}

/// The `CREATE TABLE` statement of `table` in the attached `schema`, if it has the table.
fn table_definition(
    connection: &Connection,
    schema: &str,
    table: &str,
) -> io::Result<Option<String>> {
    let mut statement = connection.prepare(&format!(
        "SELECT sql FROM {schema}.sqlite_master WHERE type = 'table' AND name = ?1"
    ))?;
    statement.bind(&[table])?;
    Ok(statement.step()?.then(|| statement.text(0)))
}

/// The target's rows the landing database lacks ([`MISSING`]).
fn missing(
    into_database: &Path,
    from_database: &Path,
    selected: Selection<'_>,
) -> io::Result<Vec<Row>> {
    let connection = attached(into_database, from_database)?;
    rows(&connection, selected)
}

/// [`MISSING`]: none when the target's database has no `cache_outputs`, so indexes nothing.
fn rows(connection: &Connection, selected: Selection<'_>) -> io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    if table_definition(connection, "previous", "cache_outputs")?.is_none() {
        return Ok(rows);
    }
    let query = match selected {
        Selection::All => MISSING,
        Selection::Only(hashes) => {
            connection.execute("CREATE TEMP TABLE wanted (hash TEXT PRIMARY KEY)", &[])?;
            let mut insert = connection.prepare("INSERT INTO temp.wanted (hash) VALUES (?1)")?;
            for hash in hashes {
                insert.bind(&[hash])?;
                while insert.step()? {}
            }
            SELECTED
        }
    };
    let mut statement = connection.prepare(query)?;
    while statement.step()? {
        rows.push(Row {
            hash: statement.text(0),
            code: statement.integer(1),
            size: statement.integer(2),
            created_at: statement.text(3),
        });
    }
    Ok(rows)
}

/// Phase 2 for one pair of databases: answers the entries indexed and their recorded bytes.
fn commit_database(
    database: &StagedDatabase,
    complete: bool,
    selected: Selection<'_>,
    mut budget: Option<&mut Budget>,
) -> io::Result<(u64, u64, Option<String>)> {
    let connection = attached(&database.into_database, &database.from_database)?;
    // Rebase selection also fences entries indexed since phase 1.
    let now = rows(&connection, selected)?;
    let current: BTreeSet<&Row> = now.iter().collect();
    let unchanged: BTreeSet<&str> = database
        .rows
        .iter()
        .filter(|staged| current.contains(staged))
        .map(|staged| staged.hash.as_str())
        .collect();
    // Each entry is named here before its files move, so a failure removes it with the rest.
    let mut placed: BTreeSet<&str> = BTreeSet::new();
    let mut stopped = None;
    let result = (|| {
        for &hash in &unchanged {
            placed.insert(hash);
            place(&database.staging, &database.into_cache, hash)?;
        }
        if complete {
            for row in now
                .iter()
                .filter(|row| !unchanged.contains(row.hash.as_str()))
            {
                match copy_row(
                    &database.from_cache,
                    &database.into_cache,
                    row,
                    budget.as_deref_mut(),
                ) {
                    Ok(true) => {
                        placed.insert(&row.hash);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        stopped = Some(format!(
                            "copy entry {} from {}: {error}",
                            row.hash,
                            database.from_cache.display()
                        ));
                        break;
                    }
                }
            }
        }
        index(&connection, &placed)
    })();
    if let Err(error) = result {
        // Files no row indexes would stay in the cache for good: Nx evicts by row.
        for hash in &placed {
            if let Err(cleanup) = remove_entry(&database.into_cache, hash) {
                return Err(io::Error::new(
                    error.kind(),
                    format!("{error}; removing carried entry {hash} also failed: {cleanup}"),
                ));
            }
        }
        return Err(error);
    }
    let bytes = now
        .iter()
        .filter(|row| placed.contains(row.hash.as_str()))
        .map(|row| u64::try_from(row.size).unwrap_or(0))
        .sum();
    let entries = u64::try_from(placed.len())
        .map_err(|error| io::Error::other(format!("carried entry count: {error}")))?;
    Ok((entries, bytes, stopped))
}

/// Move staged entry `hash` from `staging` into the cache `cache`, replacing whatever stood
/// under its names: no row indexed that, and Nx's own `put` would have replaced it.
fn place(staging: &Path, cache: &Path, hash: &str) -> io::Result<()> {
    remove_entry(cache, hash)?;
    fs::create_dir_all(cache)?;
    fs::rename(staging.join(hash), cache.join(hash))?;
    let output = staging.join(TERMINAL_OUTPUTS).join(hash);
    match fs::symlink_metadata(&output) {
        Ok(_) => {
            fs::create_dir_all(cache.join(TERMINAL_OUTPUTS))?;
            fs::rename(output, cache.join(TERMINAL_OUTPUTS).join(hash))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Insert the rows of the `placed` entries, as the target's database holds them, in one
/// transaction: an entry is a hit from the moment its row commits, and only whole entries
/// have one. Foreign keys are enforced, as stock Nx's own connection enforces them (its daemon
/// answers `FOREIGN KEY constraint failed`): a `cache_outputs` row with no `task_details` row in
/// the landing database stops the carry instead of committing an index Nx could not have written.
fn index(connection: &Connection, placed: &BTreeSet<&str>) -> io::Result<()> {
    if placed.is_empty() {
        return Ok(());
    }
    connection.execute("PRAGMA foreign_keys = ON", &[])?;
    let mut enforced = connection.prepare("PRAGMA foreign_keys")?;
    if !(enforced.step()? && enforced.integer(0) == 1) {
        return Err(io::Error::other(
            "SQLite did not turn foreign keys on for the carry",
        ));
    }
    drop(enforced);
    connection.execute("BEGIN IMMEDIATE", &[])?;
    connection.execute("CREATE TEMP TABLE carried (hash TEXT PRIMARY KEY)", &[])?;
    {
        let mut insert = connection.prepare("INSERT INTO temp.carried (hash) VALUES (?1)")?;
        for &hash in placed {
            insert.bind(&[hash])?;
            while insert.step()? {}
        }
    }
    connection.execute(
        "INSERT OR IGNORE INTO main.task_details (hash, project, target, configuration) \
         SELECT d.hash, d.project, d.target, d.configuration \
         FROM previous.task_details d JOIN temp.carried c ON c.hash = d.hash",
        &[],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO main.cache_outputs (hash, code, size, created_at, accessed_at) \
         SELECT p.hash, p.code, p.size, p.created_at, p.accessed_at \
         FROM previous.cache_outputs p JOIN temp.carried c ON c.hash = p.hash",
        &[],
    )?;
    connection.execute("COMMIT", &[])
}

/// Rebase's bound is shared by both phases and all Nx states. Both the destination image and
/// its backing store keep a tenth of their capacity free; the carry admits no more than a tenth
/// of either capacity or the available space above either reserve. An image's virtual free
/// space alone says nothing about whether its sparse backing store can accept these writes.
#[derive(Debug)]
struct Budget {
    roots: [BudgetRoot; 2],
    block_size: u64,
    /// Defensive cap across database pairs, even when multiple databases index the same hash.
    rows: usize,
    recorded_bytes: u64,
    copy_bytes: u64,
}

impl Budget {
    fn new(volume: &Path, backing_store: &Path, rows: usize) -> io::Result<Self> {
        let (volume, volume_space) = BudgetRoot::new(volume, "build volume")?;
        let (store, store_space) = BudgetRoot::new(backing_store, "backing store")?;
        let bytes = volume
            .reserve
            .min(volume_space.available.saturating_sub(volume.reserve))
            .min(store.reserve)
            .min(store_space.available.saturating_sub(store.reserve));
        Ok(Self {
            roots: [volume, store],
            block_size: volume_space.block_size.max(store_space.block_size),
            rows,
            recorded_bytes: bytes,
            copy_bytes: bytes,
        })
    }

    fn admit(&mut self, row: &Row) -> io::Result<()> {
        let bytes = u64::try_from(row.size).map_err(|error| {
            io::Error::other(format!(
                "rebase carry entry {} has invalid size: {error}",
                row.hash
            ))
        })?;
        if self.rows == 0 || bytes > self.recorded_bytes {
            return Err(io::Error::other(format!(
                "rebase carry bound: entry {} needs {bytes} recorded bytes; {} rows and {} recorded bytes remain",
                row.hash, self.rows, self.recorded_bytes
            )));
        }
        self.rows -= 1;
        self.recorded_bytes -= bytes;
        Ok(())
    }

    fn charge(&mut self, bytes: u64) -> io::Result<()> {
        // Round data to filesystem blocks and charge one more block for each object's metadata.
        let charged = bytes
            .div_ceil(self.block_size)
            .checked_add(1)
            .and_then(|blocks| blocks.checked_mul(self.block_size))
            .ok_or_else(|| io::Error::other("rebase carry copy-size overflow"))?;
        if charged > self.copy_bytes {
            return Err(io::Error::other(format!(
                "rebase carry bound: next object needs {charged} bytes; {} copy bytes remain",
                self.copy_bytes
            )));
        }
        for root in &self.roots {
            let available = root.space()?.available.saturating_sub(root.reserve);
            if charged > available {
                return Err(io::Error::other(format!(
                    "rebase carry bound: {} has {available} bytes above its {}-byte free-space reserve; next object needs {charged} bytes",
                    root.label, root.reserve
                )));
            }
        }
        self.copy_bytes -= charged;
        Ok(())
    }

    /// Walk once, charging each object before it is written. Recursive copyfile would hide a
    /// huge file behind a small cache row; a preliminary whole-tree size scan would walk twice.
    /// Directory metadata is copied last so child creation cannot change its preserved times.
    fn copy(
        &mut self,
        source: &Path,
        destination: &Path,
        metadata: &fs::Metadata,
    ) -> io::Result<()> {
        if metadata.is_dir() {
            self.charge(0)?;
            fs::create_dir(destination)?;
            for entry in fs::read_dir(source)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                self.copy(
                    &entry.path(),
                    &destination.join(entry.file_name()),
                    &metadata,
                )?;
            }
            copyfile(
                source,
                destination,
                libc::COPYFILE_METADATA | libc::COPYFILE_NOFOLLOW,
            )
        } else if metadata.is_file() {
            let mut input = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(source)?;
            let bytes = input.metadata()?.len();
            self.charge(bytes)?;
            let mut output = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)?;
            copy_data(&mut input, &mut output, bytes)?;
            drop(output);
            copyfile(
                source,
                destination,
                libc::COPYFILE_METADATA | libc::COPYFILE_NOFOLLOW,
            )
        } else {
            self.charge(metadata.len())?;
            copyfile(source, destination, COPYFILE_ALL | libc::COPYFILE_NOFOLLOW)
        }
    }
}

/// Never let a source rewritten during staging write more than the admitted length. A growth
/// probe reads one byte without writing it; the caller removes the rejected partial entry.
fn copy_data(input: &mut fs::File, output: &mut fs::File, bytes: u64) -> io::Result<()> {
    let copied = io::copy(&mut (&mut *input).take(bytes), output)?;
    if copied != bytes {
        return Err(io::Error::other(
            "rebase carry source shrank below its admitted copy size",
        ));
    }
    let mut extra = [0];
    if input.read(&mut extra)? != 0 {
        return Err(io::Error::other(
            "rebase carry source grew beyond its admitted copy size",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct BudgetRoot {
    path: std::ffi::CString,
    reserve: u64,
    label: &'static str,
}

impl BudgetRoot {
    fn new(path: &Path, label: &'static str) -> io::Result<(Self, Space)> {
        let path = super::sqlite::c_path(path)?;
        let space = Space::read(&path, label)?;
        Ok((
            Self {
                path,
                reserve: space.capacity / 10,
                label,
            },
            space,
        ))
    }

    fn space(&self) -> io::Result<Space> {
        Space::read(&self.path, self.label)
    }
}

struct Space {
    capacity: u64,
    available: u64,
    block_size: u64,
}

impl Space {
    fn read(volume: &std::ffi::CStr, label: &str) -> io::Result<Self> {
        let mut space = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: volume is NUL-terminated and statfs initializes the output on success.
        if unsafe { libc::statfs(volume.as_ptr(), space.as_mut_ptr()) } != 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("inspect {label} at {}: {error}", volume.to_string_lossy()),
            ));
        }
        // SAFETY: the successful statfs call initialized every field read below.
        let space = unsafe { space.assume_init() };
        let block_size = u64::from(space.f_bsize);
        if block_size == 0 {
            return Err(io::Error::other(
                "rebase carry filesystem has zero-sized blocks",
            ));
        }
        let bytes = |blocks: u64| {
            blocks
                .checked_mul(block_size)
                .ok_or_else(|| io::Error::other("rebase carry filesystem-size overflow"))
        };
        Ok(Self {
            capacity: bytes(space.f_blocks)?,
            available: bytes(space.f_bavail)?,
            block_size,
        })
    }
}

/// Copy entry `hash`'s output directory and terminal output from the cache `from` to the same
/// names under `into`. `false` when `from` holds no output directory for it: Nx would answer
/// that row with a hit that restores nothing, so it is not carried. A copy that fails removes
/// what it wrote.
fn copy_row(
    from: &Path,
    into: &Path,
    row: &Row,
    mut budget: Option<&mut Budget>,
) -> io::Result<bool> {
    let hash = &row.hash;
    let source = from.join(hash);
    let metadata = match fs::symlink_metadata(&source) {
        Ok(metadata) if metadata.is_dir() => metadata,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if let Some(budget) = budget.as_deref_mut() {
        budget.admit(row)?;
    }
    remove_entry(into, hash)?;
    fs::create_dir_all(into)?;
    let output = from.join(TERMINAL_OUTPUTS).join(hash);
    let copied = match budget.as_deref_mut() {
        Some(budget) => budget.copy(&source, &into.join(hash), &metadata),
        None => copyfile(
            &source,
            &into.join(hash),
            COPYFILE_ALL | libc::COPYFILE_RECURSIVE | libc::COPYFILE_NOFOLLOW,
        ),
    }
    .and_then(|()| match fs::symlink_metadata(&output) {
        Ok(metadata) => {
            fs::create_dir_all(into.join(TERMINAL_OUTPUTS))?;
            let destination = into.join(TERMINAL_OUTPUTS).join(hash);
            match budget {
                Some(budget) => budget.copy(&output, &destination, &metadata),
                None => copyfile(
                    &output,
                    &destination,
                    COPYFILE_ALL | libc::COPYFILE_NOFOLLOW,
                ),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    });
    if let Err(error) = copied {
        return Err(match remove_entry(into, hash) {
            Ok(()) => error,
            Err(cleanup) => io::Error::new(
                error.kind(),
                format!("{error}; removing the partial copy also failed: {cleanup}"),
            ),
        });
    }
    Ok(true)
}

/// Remove entry `hash`'s files under the cache-shaped directory `root`.
fn remove_entry(root: &Path, hash: &str) -> io::Result<()> {
    remove(&root.join(hash))?;
    remove(&root.join(TERMINAL_OUTPUTS).join(hash))
}

/// Remove `path`, a directory tree or a file, if it exists.
fn remove(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Each `.nx` directory's cache and `workspace-data` the state names, paired by the checkout
/// directory both sit in.
fn nx_states(state: &BuildVolumeState) -> Vec<NxState<'_>> {
    let nx = |name: &str| {
        state
            .paths
            .iter()
            .filter(move |path| {
                super::BuildStateTool::of(path) == super::BuildStateTool::Nx
                    && path.checkout.as_path().file_name() == Some(std::ffi::OsStr::new(name))
            })
            .filter_map(|path| Some((path.checkout.as_path().parent()?, path.volume.as_path())))
            .collect::<Vec<_>>()
    };
    let caches = nx("cache");
    nx("workspace-data")
        .into_iter()
        .filter_map(|(checkout, data)| {
            let (_, cache) = caches.iter().find(|(at, _)| *at == checkout)?;
            Some(NxState {
                checkout,
                cache,
                data,
            })
        })
        .collect()
}

/// The file names of the task databases directly in `data`.
fn databases_in(data: &Path) -> io::Result<Vec<std::ffi::OsString>> {
    let mut names = Vec::new();
    let entries = match fs::read_dir(data) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(names),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "db") && path.is_file() {
            names.push(entry.file_name());
        }
    }
    names.sort();
    Ok(names)
}

fn copyfile(source: &Path, destination: &Path, flags: libc::copyfile_flags_t) -> io::Result<()> {
    let source = super::sqlite::c_path(source)?;
    let destination = super::sqlite::c_path(destination)?;
    // SAFETY: both C strings outlive the call; a null state requests a one-shot copy.
    let status = unsafe {
        libc::copyfile(
            source.as_ptr(),
            destination.as_ptr(),
            std::ptr::null_mut(),
            flags,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stock Nx 23.2.1's two tables the carry writes, as its own `CREATE` statements read.
    const SCHEMA: &str = "CREATE TABLE task_details (hash TEXT PRIMARY KEY NOT NULL, \
         project TEXT NOT NULL, target TEXT NOT NULL, configuration TEXT); \
         CREATE TABLE cache_outputs (hash TEXT PRIMARY KEY NOT NULL, code INTEGER NOT NULL, \
         size INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
         accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
         FOREIGN KEY (hash) REFERENCES task_details (hash))";

    fn database(path: &Path, sql: &str) {
        fs::write(path, b"").unwrap();
        let connection = Connection::open(path).unwrap();
        for statement in sql.split(';') {
            connection.execute(statement, &[]).unwrap();
        }
    }

    fn indexed(path: &Path) -> i64 {
        let connection = Connection::open(path).unwrap();
        let mut count = connection
            .prepare("SELECT count(*) FROM cache_outputs")
            .unwrap();
        assert!(count.step().unwrap());
        count.integer(0)
    }

    struct Fixture {
        root: PathBuf,
        from: PathBuf,
        into: PathBuf,
        state: BuildVolumeState,
    }

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-carry-bound-{}",
                uuid::Uuid::new_v4().simple()
            ));
            let from = root.join("from");
            let into = root.join("into");
            for volume in [&from, &into] {
                fs::create_dir_all(volume.join("nx/workspace-data")).unwrap();
                database(&volume.join("nx/workspace-data/task.db"), SCHEMA);
            }
            Self {
                root,
                from,
                into,
                state: BuildVolumeState {
                    paths: vec![
                        super::super::BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                        super::super::BuildStatePath::new(
                            ".nx/workspace-data",
                            "nx/workspace-data",
                        )
                        .unwrap(),
                    ],
                    fingerprint: None,
                },
            }
        }

        fn entry(&self, hash: &str, size: i64, output: &[u8]) {
            self.entry_at(Path::new("nx"), hash, size, output);
        }

        fn entry_at(&self, namespace: &Path, hash: &str, size: i64, output: &[u8]) {
            let database = self.from.join(namespace).join("workspace-data/task.db");
            let connection = Connection::open(&database).unwrap();
            connection
                .execute(
                    "INSERT INTO task_details VALUES (?1, 'a', 'build', NULL)",
                    &[hash],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO cache_outputs (hash, code, size) VALUES (?1, 0, ?2)",
                    &[hash, &size.to_string()],
                )
                .unwrap();
            let entry = self.from.join(namespace).join("cache").join(hash);
            fs::create_dir_all(&entry).unwrap();
            fs::write(entry.join("output"), output).unwrap();
        }

        fn selections(hashes: &[&str]) -> BTreeMap<PathBuf, BTreeSet<String>> {
            BTreeMap::from([(
                PathBuf::from(".nx"),
                hashes.iter().map(|hash| (*hash).to_owned()).collect(),
            )])
        }

        fn backing_store(&self) -> &Path {
            &self.root
        }

        fn stage(&self, hashes: &[&str]) -> Staged {
            stage_rebase(
                &self.from,
                &self.state,
                &self.into,
                &self.state,
                &Self::selections(hashes),
                self.backing_store(),
            )
        }

        fn limited(&self, hashes: &[&str], adjust: impl FnOnce(&mut Budget)) -> Staged {
            let mut budget = Budget::new(&self.into, self.backing_store(), hashes.len()).unwrap();
            adjust(&mut budget);
            stage_with(
                &self.from,
                &self.state,
                &self.into,
                &self.state,
                Some(&Self::selections(hashes)),
                Staged {
                    budget: Some(budget),
                    ..Staged::default()
                },
            )
        }

        fn carried_hashes(&self) -> BTreeSet<String> {
            let connection =
                Connection::open(&self.into.join("nx/workspace-data/task.db")).unwrap();
            let mut statement = connection
                .prepare("SELECT hash FROM cache_outputs")
                .unwrap();
            let mut hashes = BTreeSet::new();
            while statement.step().unwrap() {
                hashes.insert(statement.text(0));
            }
            hashes
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).expect("remove carry fixture");
        }
    }

    #[test]
    fn bounded_copy_preserves_modes_times_and_symlinks() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let source = fixture.from.join("nx/cache/1");
        fs::set_permissions(source.join("output"), fs::Permissions::from_mode(0o440)).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o750)).unwrap();
        std::os::unix::fs::symlink("output", source.join("alias")).unwrap();
        let source_time = fs::metadata(source.join("output"))
            .unwrap()
            .modified()
            .unwrap();
        let carried = commit(fixture.stage(&["1"]), &fixture.into);
        assert_eq!(carried.entries, 1);
        assert!(carried.stopped.is_none(), "{carried:?}");
        let destination = fixture.into.join("nx/cache/1");
        assert_eq!(
            fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
            0o750
        );
        let output = fs::metadata(destination.join("output")).unwrap();
        assert_eq!(output.permissions().mode() & 0o777, 0o440);
        assert_eq!(output.modified().unwrap(), source_time);
        assert_eq!(
            fs::read_link(destination.join("alias")).unwrap(),
            PathBuf::from("output")
        );
        assert_eq!(fs::read(destination.join("output")).unwrap(), b"warm");
    }

    #[test]
    fn a_growing_source_cannot_write_past_its_admitted_length() {
        let fixture = Fixture::new();
        let source = fixture.from.join("growing");
        let destination = fixture.into.join("bounded");
        fs::write(&source, b"larger than the admitted length").unwrap();
        let error = copy_data(
            &mut fs::File::open(&source).unwrap(),
            &mut fs::File::create(&destination).unwrap(),
            4,
        )
        .unwrap_err();
        assert!(error.to_string().contains("grew beyond"));
        assert_eq!(fs::read(&destination).unwrap(), b"larg");
    }

    #[test]
    fn a_shrinking_source_is_not_treated_as_a_whole_staged_file() {
        let fixture = Fixture::new();
        let source = fixture.from.join("shrinking");
        let destination = fixture.into.join("partial");
        fs::write(&source, b"short").unwrap();
        let error = copy_data(
            &mut fs::File::open(&source).unwrap(),
            &mut fs::File::create(&destination).unwrap(),
            10,
        )
        .unwrap_err();
        assert!(error.to_string().contains("shrank below"));
    }

    #[test]
    fn rebase_commit_filters_late_entries_without_charging_staged_entries_twice() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let mut staged = fixture.stage(&["1", "2"]);
        assert!(staged.stopped.is_none(), "{:?}", staged.stopped);
        // Only the late requested entry fits; moving the staged entry must not charge it again.
        staged.budget.as_mut().unwrap().recorded_bytes = 4;
        fixture.entry("2", 4, b"late");
        fixture.entry("999", 1_000_000, b"old history");
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 2);
        assert_eq!(carried.bytes, 8);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!(
            fixture.carried_hashes(),
            BTreeSet::from(["1".into(), "2".into()])
        );
        assert!(!fixture.into.join("nx/cache/999").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_row_bound_keeps_the_whole_staged_prefix() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        fixture.entry("2", 4, b"more");
        let staged = fixture.limited(&["1", "2"], |budget| budget.rows = 1);
        assert_eq!(staged.databases[0].rows.len(), 1);
        assert!(staged.stopped.as_ref().unwrap().contains("0 rows"));
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(fixture.carried_hashes().len(), 1);
        assert!(
            carried
                .stopped
                .as_ref()
                .unwrap()
                .contains("rebase carry bound")
        );
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_recorded_byte_bound_keeps_the_whole_staged_prefix() {
        let fixture = Fixture::new();
        fixture.entry("1", 1024, &[42; 1024]);
        fixture.entry("2", 1024, &[43; 1024]);
        let staged = fixture.limited(&["1", "2"], |budget| budget.recorded_bytes = 1024);
        assert_eq!(staged.databases[0].rows.len(), 1);
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("0 recorded bytes remain")
        );
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(carried.bytes, 1024);
        assert_eq!(fixture.carried_hashes().len(), 1);
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_actual_byte_bound_removes_a_partial_entry_with_an_understated_row() {
        let fixture = Fixture::new();
        fixture.entry("1", 1, &[42; 65536]);
        let staged = fixture.limited(&["1"], |budget| budget.copy_bytes = budget.block_size * 2);
        assert!(staged.databases[0].rows.is_empty());
        assert!(staged.stopped.as_ref().unwrap().contains("copy bytes"));
        assert!(!fixture.into.join(STAGING).join("0/1").exists());
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(fixture.carried_hashes().is_empty());
        assert!(!fixture.into.join("nx/cache/1").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_free_space_reserve_stops_before_writing_an_entry() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let staged = fixture.limited(&["1"], |budget| budget.roots[0].reserve = u64::MAX);
        assert!(staged.databases[0].rows.is_empty());
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("free-space reserve")
        );
        assert!(!fixture.into.join(STAGING).join("0/1").exists());
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(fixture.carried_hashes().is_empty());
    }

    #[test]
    fn rebase_backing_store_reserve_stops_even_when_the_image_has_free_space() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let staged = fixture.limited(&["1"], |budget| budget.roots[1].reserve = u64::MAX);
        assert!(staged.databases[0].rows.is_empty());
        assert!(staged.stopped.as_ref().unwrap().contains("backing store"));
        assert!(!fixture.into.join(STAGING).join("0/1").exists());
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(fixture.carried_hashes().is_empty());
    }

    #[test]
    fn rebase_backing_store_statfs_failure_never_falls_back_to_image_space() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let store = fixture.root.join("store");
        fs::create_dir(&store).unwrap();
        let budget = Budget::new(&fixture.into, &store, 1).unwrap();
        fs::remove_dir(&store).unwrap();
        let staged = stage_with(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            Some(&Fixture::selections(&["1"])),
            Staged {
                budget: Some(budget),
                ..Staged::default()
            },
        );
        assert!(staged.databases[0].rows.is_empty());
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("inspect backing store")
        );
        assert!(!fixture.into.join(STAGING).join("0/1").exists());
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(fixture.carried_hashes().is_empty());
    }

    #[test]
    fn rebase_missing_backing_store_stops_before_any_entry_is_staged() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let staged = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &Fixture::selections(&["1"]),
            &fixture.root.join("missing-store"),
        );
        assert!(staged.databases.is_empty());
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("inspect backing store")
        );
        assert!(!fixture.into.join(STAGING).exists());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    #[test]
    fn rebase_commit_bound_indexes_the_staged_prefix_before_stopping_a_late_copy() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let mut staged = fixture.stage(&["1", "2"]);
        staged.budget.as_mut().unwrap().recorded_bytes = 0;
        fixture.entry("2", 4, b"late");
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["1".into()]));
        assert!(
            carried
                .stopped
                .as_ref()
                .unwrap()
                .contains("rebase carry bound")
        );
        assert!(!fixture.into.join("nx/cache/2").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_changed_row_never_indexes_its_stale_staged_files() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        fixture.entry("2", 4, b"old!");
        let staged = fixture.stage(&["1", "2"]);
        Connection::open(&fixture.from.join("nx/workspace-data/task.db"))
            .unwrap()
            .execute("UPDATE cache_outputs SET code = 1 WHERE hash = '2'", &[])
            .unwrap();
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["1".into()]));
        assert!(carried.stopped.as_ref().unwrap().contains("0 rows"));
        assert!(!fixture.into.join("nx/cache/2").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn rebase_empty_selection_carries_nothing_and_missing_selection_never_widens() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let empty = fixture.stage(&[]);
        assert!(empty.stopped.is_none());
        let carried = commit(empty, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(carried.stopped.is_none());
        let missing = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &BTreeMap::new(),
            fixture.backing_store(),
        );
        assert!(missing.databases.is_empty());
        assert!(
            missing
                .stopped
                .as_ref()
                .unwrap()
                .contains("no current-tree task hashes")
        );
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    #[test]
    fn rebase_nested_nx_states_use_their_own_hash_selection() {
        let mut fixture = Fixture::new();
        for volume in [&fixture.from, &fixture.into] {
            let data = volume.join("tools/widget/nx/workspace-data");
            fs::create_dir_all(&data).unwrap();
            database(&data.join("task.db"), SCHEMA);
        }
        fixture.state.paths.extend([
            super::super::BuildStatePath::new("tools/widget/.nx/cache", "tools/widget/nx/cache")
                .unwrap(),
            super::super::BuildStatePath::new(
                "tools/widget/.nx/workspace-data",
                "tools/widget/nx/workspace-data",
            )
            .unwrap(),
        ]);
        fixture.entry("1", 4, b"root");
        fixture.entry("2", 4, b"old!");
        fixture.entry_at(Path::new("tools/widget/nx"), "2", 4, b"nest");
        let mut selections = Fixture::selections(&["1"]);
        selections.insert(
            PathBuf::from("tools/widget/.nx"),
            BTreeSet::from(["2".into()]),
        );
        let staged = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &selections,
            fixture.backing_store(),
        );
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 2);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["1".into()]));
        assert_eq!(
            fs::read(fixture.into.join("tools/widget/nx/cache/2/output")).unwrap(),
            b"nest"
        );
        assert!(!fixture.into.join("nx/cache/2").exists());
    }

    #[test]
    fn selected_hashes_probe_the_source_index_without_scanning_historical_rows() {
        let fixture = Fixture::new();
        let source = Connection::open(&fixture.from.join("nx/workspace-data/task.db")).unwrap();
        source
            .execute(
                "WITH RECURSIVE hashes(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM hashes WHERE n < 10000) \
                 INSERT INTO task_details SELECT CAST(n AS TEXT), 'a', 'build', NULL FROM hashes",
                &[],
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO cache_outputs (hash, code, size) SELECT hash, 0, 1024 FROM task_details",
                &[],
            )
            .unwrap();
        source.execute("ANALYZE", &[]).unwrap();
        drop(source);
        let connection = attached(
            &fixture.into.join("nx/workspace-data/task.db"),
            &fixture.from.join("nx/workspace-data/task.db"),
        )
        .unwrap();
        let selected = BTreeSet::from(["1".to_owned()]);
        assert_eq!(
            rows(&connection, Selection::Only(&selected)).unwrap().len(),
            1
        );
        let mut explain = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {SELECTED}"))
            .unwrap();
        let mut plan = Vec::new();
        while explain.step().unwrap() {
            plan.push(explain.text(3));
        }
        assert!(
            plan.iter().any(|detail| {
                detail.starts_with("SEARCH p USING ") && detail.contains("(hash=?)")
            }),
            "{plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.starts_with("SCAN p")),
            "{plan:?}"
        );
    }

    #[test]
    fn rebase_carry_stages_only_current_tree_hashes_even_with_large_history() {
        let fixture = Fixture::new();
        for hash in 1..=64 {
            fixture.entry(&hash.to_string(), 1024, &[42; 1024]);
        }
        let staged = fixture.stage(&["1"]);
        let copied: BTreeSet<_> = staged
            .databases
            .iter()
            .flat_map(|database| database.rows.iter().map(|row| row.hash.clone()))
            .collect();
        let copied_bytes: u64 = copied
            .iter()
            .map(|hash| {
                fs::metadata(
                    fixture
                        .into
                        .join(STAGING)
                        .join("0")
                        .join(hash)
                        .join("output"),
                )
                .unwrap()
                .len()
            })
            .sum();
        assert_eq!(
            copied,
            BTreeSet::from(["1".to_owned()]),
            "historical hashes cannot hit the rebased tree"
        );
        assert_eq!(
            copied_bytes, 1024,
            "carry writes only the tree's needed entry"
        );
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(carried.bytes, 1024);
        assert!(carried.stopped.is_none(), "{carried:?}");
    }

    #[test]
    fn rebase_never_copies_an_unbounded_task_database_to_initialize_a_new_cache() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        fs::remove_file(fixture.into.join("nx/workspace-data/task.db")).unwrap();
        let staged = fixture.stage(&["1"]);
        assert!(staged.databases.is_empty());
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("unbounded task history")
        );
        assert!(!fixture.into.join("nx/workspace-data/task.db").exists());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    /// A target row the carry would index without its task details stops the carry and
    /// indexes nothing, where it used to commit an index stock Nx could not have written.
    #[test]
    fn a_cache_row_without_its_task_details_fails_the_carry_and_indexes_nothing() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-carry-keys-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        let from = root.join("from.db");
        let into = root.join("into.db");
        // Foreign keys are off on this connection, as on any SQLite connection by default.
        database(
            &from,
            &format!(
                "{SCHEMA}; INSERT INTO task_details VALUES ('whole', 'a', 'build', NULL); \
                 INSERT INTO cache_outputs (hash, code, size) VALUES ('whole', 0, 1); \
                 INSERT INTO cache_outputs (hash, code, size) VALUES ('orphan', 0, 1)"
            ),
        );
        database(&into, SCHEMA);

        let connection = attached(&into, &from).unwrap();
        let refused = index(&connection, &BTreeSet::from(["orphan", "whole"])).unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("FOREIGN KEY constraint failed"),
            "{refused}"
        );
        drop(connection);
        assert_eq!(
            indexed(&into),
            0,
            "the failed transaction committed nothing"
        );

        let connection = attached(&into, &from).unwrap();
        index(&connection, &BTreeSet::from(["whole"])).unwrap();
        drop(connection);
        assert_eq!(indexed(&into), 1);
        fs::remove_dir_all(root).unwrap();
    }
}
