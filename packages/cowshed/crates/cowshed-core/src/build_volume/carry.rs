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
//! Rebase uses [`stage_rebase`] instead: only what the rebased tree looks up is selected, by
//! current task hash and, for the tasks Nx hashes only after their dependencies ran, by task
//! (its latest entry), including during commit; one row/byte/free-space budget bounds all copies
//! in both phases. Regular file data is copied through a length-limited reader, with
//! `fcopyfile(3)` preserving metadata on the same pinned file descriptors.
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
//! `fcopyfile(3)` on descriptors this process opens close-on-exec keeps every object's mode,
//! times and attributes (`crate::copy::native`). A crashed carry leaves only the
//! staging directory, which the next [`stage`] or [`unstage`] on that volume deletes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::BuildVolumeState;
use super::nx::TaskSelection;
use super::sqlite::Connection;

/// The landing volume's staging directory for entries not yet indexed, at its root and outside
/// every tool's namespace.
pub const STAGING: &str = ".carry";
/// Where stock Nx keeps each entry's terminal output, inside its cache directory.
const TERMINAL_OUTPUTS: &str = "terminalOutputs";

/// The target's entries the landing volume lacks, most recently used first, so a carry that
/// stops has carried the ones most likely to hit. Nx names an entry by a decimal hash
/// (`^\d+$`); any other row names no directory the carry could safely join. Every row reads
/// `hash, code, size, created_at, by task, accessed_at`, as [`SELECTED`]'s do.
const MISSING: &str = "SELECT p.hash, p.code, p.size, p.created_at, 0, p.accessed_at \
     FROM previous.cache_outputs p \
     WHERE p.hash <> '' AND p.hash NOT GLOB '*[^0-9]*' \
       AND NOT EXISTS (SELECT 1 FROM main.cache_outputs m WHERE m.hash = p.hash) \
     ORDER BY p.accessed_at DESC";
/// Rebase's selection (16_build_volumes.md, "Rebase carry"), both classes, most recently used
/// first. By hash: CROSS JOIN fixes the loop order, so the wanted hashes probe `p`'s primary key
/// and never scan its history. By task: the one entry of each deferred task with the latest use.
/// `task_details` has no index by task and the target's database is never written, so the
/// cache `c`, which Nx's eviction bounds, is read once, probing `d`'s primary key and the
/// deferred tasks' index. The latest entry is ranked before the landing side is consulted: a
/// landing database that already holds it takes nothing for that task, never an older entry.
const SELECTED: &str = "SELECT p.hash, p.code, p.size, p.created_at, 0, p.accessed_at \
     FROM temp.wanted w CROSS JOIN previous.cache_outputs p \
     WHERE p.hash = w.hash AND p.hash <> '' AND p.hash NOT GLOB '*[^0-9]*' \
       AND NOT EXISTS (SELECT 1 FROM main.cache_outputs m WHERE m.hash = p.hash) \
     UNION ALL \
     SELECT l.hash, l.code, l.size, l.created_at, 1, l.accessed_at FROM ( \
       SELECT c.hash, c.code, c.size, c.created_at, c.accessed_at, row_number() OVER ( \
           PARTITION BY d.project, d.target, d.configuration \
           ORDER BY c.accessed_at DESC, c.created_at DESC, c.hash DESC) AS rank \
       FROM previous.cache_outputs c CROSS JOIN previous.task_details d \
         CROSS JOIN temp.deferred t \
       WHERE d.hash = c.hash AND t.project = d.project AND t.target = d.target \
         AND t.configuration IS d.configuration) l \
     WHERE l.rank = 1 AND l.hash <> '' AND l.hash NOT GLOB '*[^0-9]*' \
       AND NOT EXISTS (SELECT 1 FROM main.cache_outputs m WHERE m.hash = l.hash) \
       AND NOT EXISTS (SELECT 1 FROM temp.wanted w WHERE w.hash = l.hash) \
     ORDER BY 6 DESC";

// Publication must also probe by the small carried set, not walk either historical table.
const INDEX_DETAILS: &str = "INSERT OR IGNORE INTO main.task_details (hash, project, target, configuration) \
     SELECT d.hash, d.project, d.target, d.configuration \
     FROM temp.carried c CROSS JOIN previous.task_details d WHERE d.hash = c.hash";
const INDEX_OUTPUTS: &str = "INSERT OR IGNORE INTO main.cache_outputs (hash, code, size, created_at, accessed_at) \
     SELECT p.hash, p.code, p.size, p.created_at, p.accessed_at \
     FROM temp.carried c CROSS JOIN previous.cache_outputs p WHERE p.hash = c.hash";

// The only schema this row shim understands: pinned Nx 23.2.1's cache foreign-key pair.
// These statements are recognized, never executed. The checkout's Nx owns initialization.
const TASK_DETAILS_SCHEMA: &str = "CREATE TABLE task_details (hash TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, target TEXT NOT NULL, configuration TEXT)";
const CACHE_OUTPUTS_SCHEMA: &str = "CREATE TABLE cache_outputs (hash TEXT PRIMARY KEY NOT NULL, code INTEGER NOT NULL, size INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (hash) REFERENCES task_details (hash))";

/// One entry's row in the target's database, as phase 1 read it.
#[derive(Debug, Eq, PartialEq)]
struct Row {
    hash: String,
    code: i64,
    size: i64,
    created_at: String,
    /// Selected by its deferred task's identity rather than by its hash (rebase only).
    by_task: bool,
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
    /// Each Nx state (its checkout-relative `.nx`) whose selection reached the target's task
    /// databases, and whether one of them indexes any cache entry.
    consulted: Vec<(PathBuf, bool)>,
}

impl Staged {
    /// The Nx states consulted, when staging did not stop and none of the target's task
    /// databases they reached indexes a single cache entry: the target had nothing to select
    /// from, which is not the same as the workspace missing nothing (16_build_volumes.md,
    /// "Rebase carry").
    pub fn unindexed(&self) -> Option<Vec<PathBuf>> {
        let unindexed = self.stopped.is_none()
            && !self.consulted.is_empty()
            && self.consulted.iter().all(|(_, indexed)| !indexed);
        unindexed.then(|| {
            self.consulted
                .iter()
                .map(|(state, _)| state.clone())
                .collect()
        })
    }
}

impl Staged {
    /// Staging that stopped before any entry was staged, naming why.
    pub fn stopped(reason: String) -> Self {
        Self {
            stopped: Some(reason),
            ..Self::default()
        }
    }
}

/// What a carry put into the landing volume.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Carried {
    pub entries: u64,
    /// How many of `entries` a rebase selected by deferred task rather than by hash.
    pub deferred: u64,
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
/// another name belongs to an Nx of another schema, so the target's is not carried into it.
/// One with none is left untouched: the carry creates no schema, so the checkout's Nx must
/// initialize it first. A failed initialization is the land's carry stop reason, not a refused land.
pub fn stage(
    from: &Path,
    from_state: &BuildVolumeState,
    into: &Path,
    into_state: &BuildVolumeState,
) -> Staged {
    stage_with(from, from_state, into, into_state, None, Staged::default())
}

/// Rebase carry admits only what Nx names for the rebased tree, across both phases: each current
/// hash, and each deferred task's latest entry. Historical entries consume no copy bandwidth or
/// destination space. The entire carry shares one row and byte budget; missing selection
/// information stops it, never widens it.
pub fn stage_rebase(
    from: &Path,
    from_state: &BuildVolumeState,
    into: &Path,
    into_state: &BuildVolumeState,
    selections: &BTreeMap<PathBuf, TaskSelection>,
    backing_store: &Path,
) -> Staged {
    let rows = selections
        .values()
        .try_fold(0_usize, |sum, selection| sum.checked_add(selection.len()));
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
    selections: Option<&BTreeMap<PathBuf, TaskSelection>>,
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
        if matches!(selected, Selection::Only(selection) if selection.is_empty()) {
            continue;
        }
        let owned = match selected {
            Selection::All => OwnedSelection::All,
            Selection::Only(selection) => OwnedSelection::Only(Arc::new(selection.clone())),
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
        // A target state with no task database indexes nothing; one whose every database is of
        // another Nx schema than the workspace's is not consulted at all.
        let mut indexed = databases.is_empty().then_some(false);
        for name in databases {
            let from_database = from.join(source.data).join(&name);
            let into_database = into.join(destination.data).join(&name);
            if !into_database.is_file() {
                staged.stopped = Some(format!(
                    "carry from {} into {} stopped: destination task database is missing; refusing to skip it or copy the target's unbounded task history",
                    from_database.display(),
                    into_database.display()
                ));
                return staged;
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
            match result {
                Ok(database_indexed) => {
                    indexed = Some(indexed.unwrap_or(false) || database_indexed);
                }
                Err(error) => {
                    staged.stopped = Some(error);
                    return staged;
                }
            }
        }
        if let Some(indexed) = indexed {
            staged
                .consulted
                .push((source.checkout.to_path_buf(), indexed));
        }
    }
    staged
}

#[derive(Clone, Copy)]
enum Selection<'a> {
    All,
    Only(&'a TaskSelection),
}

#[derive(Clone, Debug)]
enum OwnedSelection {
    All,
    Only(Arc<TaskSelection>),
}

impl OwnedSelection {
    fn borrowed(&self) -> Selection<'_> {
        match self {
            Self::All => Selection::All,
            Self::Only(selection) => Selection::Only(selection),
        }
    }
}

fn selection<'a>(
    selections: Option<&'a BTreeMap<PathBuf, TaskSelection>>,
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
            Ok(committed) => {
                carried.entries += committed.entries;
                carried.deferred += committed.deferred;
                carried.bytes += committed.bytes;
                if let Some(stopped) = committed.stopped {
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
/// entry's files into the staging directory, recording each entry staged whole. Answers whether
/// the target's database indexes any cache entry at all.
fn stage_database(
    database: &mut StagedDatabase,
    selected: Selection<'_>,
    mut budget: Option<&mut Budget>,
) -> Result<bool, String> {
    let context = |error: io::Error| {
        format!(
            "stage {} into {}: {error}",
            database.from_database.display(),
            database.into_database.display()
        )
    };
    // The connection closes before the copies: phase 1 holds the target's database only for
    // the one read.
    let Some(missing) =
        missing(&database.into_database, &database.from_database, selected).map_err(context)?
    else {
        return Ok(false);
    };
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
    Ok(true)
}

/// Open two existing Nx-owned v3 databases. The pinned adapter already created the destination:
/// this minimal compatibility shim recognizes its row schema, but never manufactures one.
fn attached(into_database: &Path, from_database: &Path) -> io::Result<Connection> {
    for path in [into_database, from_database] {
        if !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-v3.db"))
        {
            return Err(io::Error::other(format!(
                "Nx 23.2.1 carry row shim requires a v3 database, not {}",
                path.display()
            )));
        }
    }
    let connection = Connection::open(into_database)?;
    connection.execute(
        "ATTACH DATABASE ?1 AS previous",
        &[&from_database.to_string_lossy()],
    )?;
    validate_row_schema(&connection, "main", true)?;
    validate_row_schema(&connection, "previous", false)?;
    Ok(connection)
}

fn validate_row_schema(
    connection: &Connection,
    schema: &str,
    require_cache: bool,
) -> io::Result<()> {
    let mut definitions = connection.prepare(&format!(
        "SELECT sql FROM {schema}.sqlite_master \
         WHERE tbl_name IN ('task_details', 'cache_outputs') AND sql IS NOT NULL"
    ))?;
    let same = |left: &str, right: &str| {
        left.bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .map(|byte| byte.to_ascii_lowercase())
            .eq(right
                .bytes()
                .filter(|byte| !byte.is_ascii_whitespace())
                .map(|byte| byte.to_ascii_lowercase()))
    };
    let mut details = false;
    let mut cache = false;
    while definitions.step()? {
        let definition = definitions.text(0);
        if same(&definition, TASK_DETAILS_SCHEMA) {
            details = true;
        } else if same(&definition, CACHE_OUTPUTS_SCHEMA) {
            cache = true;
        } else {
            return Err(io::Error::other(format!(
                "Nx 23.2.1 carry row schema changed in {schema}: {definition}"
            )));
        }
    }
    if !details || (require_cache && !cache) {
        return Err(io::Error::other(format!(
            "Nx 23.2.1 carry needs Nx-initialized task_details/cache_outputs in {schema}"
        )));
    }
    Ok(())
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

/// The target's rows the landing database lacks ([`MISSING`]), or none when the target's
/// database indexes no cache entry.
fn missing(
    into_database: &Path,
    from_database: &Path,
    selected: Selection<'_>,
) -> io::Result<Option<Vec<Row>>> {
    let connection = attached(into_database, from_database)?;
    if !indexes(&connection)? {
        return Ok(None);
    }
    rows(&connection, selected).map(Some)
}

/// Whether the target's database (`previous`) indexes any cache entry. Stock Nx creates
/// `cache_outputs` only once a task runs through its cache, and an Nx whose cache is elsewhere
/// never fills this one.
fn indexes(connection: &Connection) -> io::Result<bool> {
    if table_definition(connection, "previous", "cache_outputs")?.is_none() {
        return Ok(false);
    }
    let mut statement =
        connection.prepare("SELECT EXISTS (SELECT 1 FROM previous.cache_outputs)")?;
    Ok(statement.step()? && statement.integer(0) == 1)
}

/// [`MISSING`], or [`SELECTED`] for a rebase: none when the target's database has no
/// `cache_outputs`, so indexes nothing.
fn rows(connection: &Connection, selected: Selection<'_>) -> io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    if table_definition(connection, "previous", "cache_outputs")?.is_none() {
        return Ok(rows);
    }
    let query = match selected {
        Selection::All => MISSING,
        Selection::Only(selection) => {
            connection.execute("CREATE TEMP TABLE wanted (hash TEXT PRIMARY KEY)", &[])?;
            let mut insert = connection.prepare("INSERT INTO temp.wanted (hash) VALUES (?1)")?;
            for hash in &selection.hashes {
                insert.bind(&[hash])?;
                while insert.step()? {}
            }
            drop(insert);
            connection.execute(
                "CREATE TEMP TABLE deferred (project TEXT NOT NULL, target TEXT NOT NULL, \
                 configuration TEXT, UNIQUE (project, target, configuration))",
                &[],
            )?;
            // The binding speaks only text, so a task without a configuration is its own
            // statement, writing the `NULL` its `task_details` rows hold.
            let mut configured = connection.prepare(
                "INSERT INTO temp.deferred (project, target, configuration) VALUES (?1, ?2, ?3)",
            )?;
            let mut unconfigured = connection.prepare(
                "INSERT INTO temp.deferred (project, target, configuration) VALUES (?1, ?2, NULL)",
            )?;
            for task in &selection.deferred {
                let insert = match &task.configuration {
                    Some(configuration) => {
                        configured.bind(&[&task.project, &task.target, configuration])?;
                        &mut configured
                    }
                    None => {
                        unconfigured.bind(&[&task.project, &task.target])?;
                        &mut unconfigured
                    }
                };
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
            by_task: statement.integer(4) == 1,
        });
    }
    Ok(rows)
}

/// What phase 2 indexed for one pair of databases.
struct Committed {
    entries: u64,
    /// How many of `entries` were selected by deferred task.
    deferred: u64,
    /// The bytes Nx recorded for them.
    bytes: u64,
    stopped: Option<String>,
}

/// Phase 2 for one pair of databases.
fn commit_database(
    database: &StagedDatabase,
    complete: bool,
    selected: Selection<'_>,
    mut budget: Option<&mut Budget>,
) -> io::Result<Committed> {
    let connection = attached(&database.into_database, &database.from_database)?;
    // Rebase selection also fences entries indexed since phase 1.
    let now = rows(&connection, selected)?;
    let staged_rows: BTreeMap<&str, &Row> = database
        .rows
        .iter()
        .map(|row| (row.hash.as_str(), row))
        .collect();
    // Each entry is named here before its files move, so a failure removes it with the rest.
    let mut placed: BTreeSet<&str> = BTreeSet::new();
    let mut stopped = None;
    let result = (|| {
        for row in &now {
            if staged_rows
                .get(row.hash.as_str())
                .is_some_and(|staged| *staged == row)
            {
                placed.insert(row.hash.as_str());
                place(&database.staging, &database.into_cache, &row.hash)?;
            }
        }
        if complete {
            for row in now
                .iter()
                .filter(|row| !staged_rows.contains_key(row.hash.as_str()))
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
    let placed_rows = || now.iter().filter(|row| placed.contains(row.hash.as_str()));
    let bytes = placed_rows()
        .map(|row| u64::try_from(row.size).unwrap_or(0))
        .sum();
    let count = |count: usize| {
        u64::try_from(count)
            .map_err(|error| io::Error::other(format!("carried entry count: {error}")))
    };
    Ok(Committed {
        entries: count(placed.len())?,
        deferred: count(placed_rows().filter(|row| row.by_task).count())?,
        bytes,
        stopped,
    })
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
    connection.execute(INDEX_DETAILS, &[])?;
    connection.execute(INDEX_OUTPUTS, &[])?;
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
}

/// Walk once, charging each object to `budget`, when the carry is bounded, before it is written.
/// A recursive copy would hide a huge file behind a small cache row, and a preliminary
/// whole-tree size scan would walk twice. Every object is copied through descriptors this
/// process opens close-on-exec (`crate::copy::native`). Directory metadata is copied last so
/// child creation cannot change its preserved times.
fn copy_tree(
    mut budget: Option<&mut Budget>,
    source: &Path,
    destination: &Path,
    metadata: &fs::Metadata,
) -> io::Result<()> {
    #[cfg(test)]
    tests::record_walk();
    if metadata.is_dir() {
        charge(&mut budget, 0)?;
        fs::create_dir(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            copy_tree(
                budget.as_deref_mut(),
                &entry.path(),
                &destination.join(entry.file_name()),
                &metadata,
            )?;
        }
        copied_metadata(crate::copy::native::directory_metadata(source, destination))
    } else if metadata.is_file() {
        let mut input = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(source)?;
        let bytes = input.metadata()?.len();
        charge(&mut budget, bytes)?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        copy_data(&mut input, &mut output, bytes)?;
        copy_metadata(&input, &output)
    } else if metadata.file_type().is_symlink() {
        charge(&mut budget, metadata.len())?;
        copied_metadata(crate::copy::native::copy_symlink(source, destination))
    } else {
        Err(io::Error::other(format!(
            "{} is neither a directory, a regular file nor a symlink",
            source.display()
        )))
    }
}

fn charge(budget: &mut Option<&mut Budget>, bytes: u64) -> io::Result<()> {
    budget
        .as_deref_mut()
        .map_or(Ok(()), |budget| budget.charge(bytes))
}

/// `copied`, counted in tests as one metadata copy when it succeeded.
fn copied_metadata(copied: io::Result<()>) -> io::Result<()> {
    #[cfg(test)]
    if copied.is_ok() {
        tests::record_metadata();
    }
    copied
}

/// Never let a source rewritten during staging write more than the admitted length. A growth
/// probe reads one byte without writing it; the caller removes the rejected partial entry.
fn copy_data(input: &mut fs::File, output: &mut fs::File, bytes: u64) -> io::Result<()> {
    let copied = io::copy(&mut (&mut *input).take(bytes), output)?;
    #[cfg(test)]
    tests::record_bytes(copied);
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
    let copied =
        copy_tree(budget.as_deref_mut(), &source, &into.join(hash), &metadata).and_then(|()| {
            match fs::symlink_metadata(&output) {
                Ok(metadata) => {
                    fs::create_dir_all(into.join(TERMINAL_OUTPUTS))?;
                    let destination = into.join(TERMINAL_OUTPUTS).join(hash);
                    copy_tree(budget, &output, &destination, &metadata)
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
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

/// Keep metadata tied to the same inodes as the bounded data copy. Reopening either path
/// would add work and could copy attributes from a replacement made during staging.
fn copy_metadata(input: &fs::File, output: &fs::File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: both files keep their descriptors open for the entire metadata-only call.
    let status = unsafe {
        libc::fcopyfile(
            input.as_raw_fd(),
            output.as_raw_fd(),
            std::ptr::null_mut(),
            libc::COPYFILE_METADATA,
        )
    };
    if status == 0 {
        #[cfg(test)]
        tests::record_metadata();
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::super::nx::TaskIdentity;
    use super::*;

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct CopyWork {
        objects: u64,
        bytes: u64,
        metadata_calls: u64,
    }

    thread_local! {
        static WORK: std::cell::Cell<CopyWork> = const { std::cell::Cell::new(CopyWork {
            objects: 0, bytes: 0, metadata_calls: 0,
        }) };
    }

    pub(super) fn record_walk() {
        WORK.with(|work| {
            work.set(CopyWork {
                objects: work.get().objects + 1,
                ..work.get()
            })
        });
    }

    pub(super) fn record_bytes(bytes: u64) {
        WORK.with(|work| {
            work.set(CopyWork {
                bytes: work.get().bytes + bytes,
                ..work.get()
            })
        });
    }

    pub(super) fn record_metadata() {
        WORK.with(|work| {
            work.set(CopyWork {
                metadata_calls: work.get().metadata_calls + 1,
                ..work.get()
            })
        });
    }

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

    fn task(project: &str, target: &str, configuration: Option<&str>) -> TaskIdentity {
        TaskIdentity {
            project: project.to_owned(),
            target: target.to_owned(),
            configuration: configuration.map(str::to_owned),
        }
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
                database(&volume.join("nx/workspace-data/task-v3.db"), SCHEMA);
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
            let database = self.from.join(namespace).join("workspace-data/task-v3.db");
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

        /// An entry of `task` that Nx last used at `accessed_at` (`YYYY-MM-DD HH:MM:SS`, as
        /// SQLite's `CURRENT_TIMESTAMP` stamps it), in the target's root Nx state.
        fn task_entry(&self, hash: &str, task: &TaskIdentity, accessed_at: &str, output: &[u8]) {
            let connection =
                Connection::open(&self.from.join("nx/workspace-data/task-v3.db")).unwrap();
            match &task.configuration {
                Some(configuration) => connection.execute(
                    "INSERT INTO task_details VALUES (?1, ?2, ?3, ?4)",
                    &[hash, &task.project, &task.target, configuration],
                ),
                None => connection.execute(
                    "INSERT INTO task_details VALUES (?1, ?2, ?3, NULL)",
                    &[hash, &task.project, &task.target],
                ),
            }
            .unwrap();
            connection
                .execute(
                    "INSERT INTO cache_outputs (hash, code, size, accessed_at) VALUES (?1, 0, ?2, ?3)",
                    &[hash, &output.len().to_string(), accessed_at],
                )
                .unwrap();
            let entry = self.from.join("nx/cache").join(hash);
            fs::create_dir_all(&entry).unwrap();
            fs::write(entry.join("output"), output).unwrap();
        }

        fn selections(hashes: &[&str]) -> BTreeMap<PathBuf, TaskSelection> {
            Self::selecting(hashes, &[])
        }

        fn selecting(
            hashes: &[&str],
            deferred: &[TaskIdentity],
        ) -> BTreeMap<PathBuf, TaskSelection> {
            BTreeMap::from([(
                PathBuf::from(".nx"),
                TaskSelection {
                    hashes: hashes.iter().map(|hash| (*hash).to_owned()).collect(),
                    deferred: deferred.iter().cloned().collect(),
                },
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
                Connection::open(&self.into.join("nx/workspace-data/task-v3.db")).unwrap();
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
            remove(&self.into).expect("remove carry destination fixture");
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
    fn stage_and_commit_walk_and_copy_each_selected_file_once() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        WORK.with(|work| work.set(CopyWork::default()));
        let staged = fixture.stage(&["1", "2"]);
        let stage_work = WORK.with(|work| work.get());
        assert_eq!(
            stage_work,
            CopyWork {
                objects: 2,
                bytes: 4,
                metadata_calls: 2
            }
        );
        assert!(
            fixture.carried_hashes().is_empty(),
            "stage publishes no cache rows"
        );
        fixture.entry("2", 4, b"late");
        let carried = commit(staged, &fixture.into);
        let all_work = WORK.with(|work| work.get());
        assert_eq!(
            all_work,
            CopyWork {
                objects: 4,
                bytes: 8,
                metadata_calls: 4
            }
        );
        assert_eq!(carried.entries, 2);
        assert!(carried.stopped.is_none(), "{carried:?}");
        cowshed_os::eprintln!(
            "carry measured work: stage={stage_work:?}, both phases={all_work:?}"
        );
    }

    #[test]
    fn changed_and_evicted_staged_rows_are_dropped_without_recopying_but_late_hashes_survive() {
        let fixture = Fixture::new();
        for hash in ["1", "2", "3"] {
            fixture.entry(hash, 4, b"warm");
        }
        WORK.with(|work| work.set(CopyWork::default()));
        let mut staged = fixture.stage(&["1", "2", "3", "4"]);
        // A recopy would fit: exhaustion cannot be what prevents stale-entry publication.
        staged.budget.as_mut().unwrap().rows = 10;
        let before = WORK.with(|work| work.get());
        let source = Connection::open(&fixture.from.join("nx/workspace-data/task-v3.db")).unwrap();
        source
            .execute(
                "UPDATE cache_outputs SET created_at = 'rewritten' WHERE hash = '2'",
                &[],
            )
            .unwrap();
        source
            .execute("DELETE FROM cache_outputs WHERE hash = '3'", &[])
            .unwrap();
        drop(source);
        fixture.entry("4", 4, b"late");
        let carried = commit(staged, &fixture.into);
        let after = WORK.with(|work| work.get());
        assert_eq!(after.bytes - before.bytes, 4);
        assert_eq!(after.objects - before.objects, 2);
        assert_eq!(
            fixture.carried_hashes(),
            BTreeSet::from(["1".into(), "4".into()])
        );
        assert_eq!(carried.entries, 2);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert!(!fixture.into.join("nx/cache/2").exists());
        assert!(!fixture.into.join("nx/cache/3").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn legacy_selection_all_stage_and_commit_drops_changed_stages_and_keeps_late_hashes() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"whole");
        fixture.entry("2", 4, b"stale");
        let staged = stage_with(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            None,
            Staged::default(),
        );
        assert!(staged.stopped.is_none(), "{:?}", staged.stopped);
        let source = Connection::open(&fixture.from.join("nx/workspace-data/task-v3.db")).unwrap();
        source
            .execute(
                "UPDATE cache_outputs SET created_at = 'rewritten' WHERE hash = '2'",
                &[],
            )
            .unwrap();
        drop(source);
        fixture.entry("3", 4, b"late");
        let carried = commit(staged, &fixture.into);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!(
            fixture.carried_hashes(),
            BTreeSet::from(["1".into(), "3".into()])
        );
        assert!(!fixture.into.join("nx/cache/2").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    #[test]
    fn bounded_metadata_stays_with_the_pinned_data_inode_when_source_is_replaced() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = Fixture::new();
        let source = fixture.from.join("source");
        let destination = fixture.into.join("copy");
        fs::write(&source, b"old!").unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o440)).unwrap();
        let mut input = fs::File::open(&source).unwrap();
        let original_time = input.metadata().unwrap().modified().unwrap();
        let mut output = fs::File::create(&destination).unwrap();
        copy_data(&mut input, &mut output, 4).unwrap();
        fs::rename(&source, fixture.from.join("original")).unwrap();
        fs::write(&source, b"replacement").unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o777)).unwrap();
        copy_metadata(&input, &output).unwrap();
        let copied = output.metadata().unwrap();
        assert_eq!(copied.permissions().mode() & 0o777, 0o440);
        assert_eq!(copied.modified().unwrap(), original_time);
        assert_eq!(fs::read(&destination).unwrap(), b"old!");
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
        Connection::open(&fixture.from.join("nx/workspace-data/task-v3.db"))
            .unwrap()
            .execute("UPDATE cache_outputs SET code = 1 WHERE hash = '2'", &[])
            .unwrap();
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 1);
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["1".into()]));
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert!(!fixture.into.join("nx/cache/2").exists());
        assert!(!fixture.into.join(STAGING).exists());
    }

    /// A deferred task carries the one entry of its identity the target used last, out of all
    /// its historical entries, and a configuration is part of the identity: three entries of
    /// `a:test`, two of `a:test:ci` and one of an unselected task carry two, beside the hash.
    #[test]
    fn rebase_carries_one_latest_entry_per_deferred_task() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"hash");
        let test = task("a", "test", None);
        let ci = task("a", "test", Some("ci"));
        fixture.task_entry("11", &test, "2026-10-11 10:00:01", b"old test");
        fixture.task_entry("13", &test, "2026-10-11 10:00:03", b"head test");
        fixture.task_entry("12", &test, "2026-10-11 10:00:02", b"mid test");
        fixture.task_entry("21", &ci, "2026-10-11 10:00:05", b"head ci");
        fixture.task_entry("22", &ci, "2026-10-11 10:00:04", b"old ci");
        fixture.task_entry(
            "31",
            &task("b", "test", None),
            "2026-10-11 10:00:09",
            b"other",
        );
        let staged = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &Fixture::selecting(&["1"], &[test, ci]),
            fixture.backing_store(),
        );
        let copied: BTreeSet<_> = staged
            .databases
            .iter()
            .flat_map(|database| database.rows.iter().map(|row| row.hash.clone()))
            .collect();
        assert_eq!(
            copied,
            BTreeSet::from(["1".into(), "13".into(), "21".into()]),
            "only one entry per deferred task is staged"
        );
        let carried = commit(staged, &fixture.into);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!((carried.entries, carried.deferred), (3, 2));
        assert_eq!(
            fixture.carried_hashes(),
            BTreeSet::from(["1".into(), "13".into(), "21".into()])
        );
        assert_eq!(
            fs::read(fixture.into.join("nx/cache/13/output")).unwrap(),
            b"head test"
        );
    }

    /// The latest entry is chosen before the workspace is consulted: a workspace that already
    /// holds it takes nothing for that task, never the entry before it.
    #[test]
    fn rebase_deferred_task_whose_latest_entry_the_workspace_holds_takes_nothing() {
        let fixture = Fixture::new();
        let test = task("a", "test", None);
        fixture.task_entry("12", &test, "2026-10-11 10:00:02", b"old test");
        fixture.task_entry("13", &test, "2026-10-11 10:00:03", b"head test");
        database(
            &fixture.into.join("nx/workspace-data/task-v3.db"),
            &format!(
                "{SCHEMA}; INSERT INTO task_details VALUES ('13', 'a', 'test', NULL); \
                 INSERT INTO cache_outputs (hash, code, size) VALUES ('13', 0, 9)"
            ),
        );
        let staged = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &Fixture::selecting(&[], &[test]),
            fixture.backing_store(),
        );
        let carried = commit(staged, &fixture.into);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!((carried.entries, carried.deferred), (0, 0));
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["13".into()]));
        assert!(!fixture.into.join("nx/cache/12").exists());
    }

    /// Commit keeps the by-task selection: a deferred task the target used again since the stage
    /// selects its new latest entry, so the staged one is never indexed, and the one row the task
    /// was budgeted stops the second copy, as for any changed row.
    #[test]
    fn rebase_deferred_task_used_again_after_stage_never_indexes_its_superseded_entry() {
        let fixture = Fixture::new();
        let test = task("a", "test", None);
        fixture.task_entry("13", &test, "2026-10-11 10:00:03", b"head test");
        let staged = stage_rebase(
            &fixture.from,
            &fixture.state,
            &fixture.into,
            &fixture.state,
            &Fixture::selecting(&[], std::slice::from_ref(&test)),
            fixture.backing_store(),
        );
        assert_eq!(staged.databases[0].rows.len(), 1);
        fixture.task_entry("14", &test, "2026-10-11 10:00:04", b"newer test");
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(fixture.carried_hashes().is_empty());
        assert!(
            carried.stopped.as_ref().unwrap().contains("entry 14"),
            "{carried:?}"
        );
        assert!(!fixture.into.join("nx/cache/13").exists());
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

    /// A target whose task database indexes no cache entry (no `cache_outputs` table, an empty
    /// one, or no database at all) had nothing to select from, so the carry says so instead of
    /// carrying 0; a target that indexes entries the rebased tree does not ask for, and an empty
    /// selection, carry 0 because nothing is missing.
    #[test]
    fn rebase_from_a_target_that_indexes_nothing_is_unindexed_not_carried_zero() {
        let target = |fixture: &Fixture| fixture.from.join("nx/workspace-data/task-v3.db");
        let empty = Fixture::new();
        assert_eq!(
            empty.stage(&["1"]).unindexed(),
            Some(vec![PathBuf::from(".nx")])
        );

        let tableless = Fixture::new();
        database(
            &target(&tableless),
            "CREATE TABLE task_details (hash TEXT PRIMARY KEY NOT NULL, \
             project TEXT NOT NULL, target TEXT NOT NULL, configuration TEXT); \
             INSERT INTO task_details VALUES ('1', 'a', 'build', NULL)",
        );
        assert_eq!(
            tableless.stage(&["1"]).unindexed(),
            Some(vec![PathBuf::from(".nx")])
        );

        let absent = Fixture::new();
        fs::remove_file(target(&absent)).unwrap();
        assert_eq!(
            absent.stage(&["1"]).unindexed(),
            Some(vec![PathBuf::from(".nx")])
        );

        let elsewhere = Fixture::new();
        elsewhere.entry("2", 4, b"other tree");
        let staged = elsewhere.stage(&["1"]);
        assert_eq!(staged.unindexed(), None);
        let carried = commit(staged, &elsewhere.into);
        assert_eq!((carried.entries, carried.stopped), (0, None));

        assert_eq!(Fixture::new().stage(&[]).unindexed(), None);
    }

    #[test]
    fn rebase_nested_nx_states_use_their_own_hash_selection() {
        let mut fixture = Fixture::new();
        for volume in [&fixture.from, &fixture.into] {
            let data = volume.join("tools/widget/nx/workspace-data");
            fs::create_dir_all(&data).unwrap();
            database(&data.join("task-v3.db"), SCHEMA);
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
        selections.extend(
            Fixture::selections(&["2"])
                .into_values()
                .map(|selection| (PathBuf::from("tools/widget/.nx"), selection)),
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

    /// Both classes read the target's database through its indexes: the hash class probes the
    /// cache's primary key, and the task class reads only the cache Nx's eviction bounds,
    /// probing `task_details` by primary key and the deferred tasks by their index. Neither
    /// scans the `task_details` history of entries Nx evicted.
    #[test]
    fn rebase_selection_probes_indexes_and_never_scans_task_history() {
        let fixture = Fixture::new();
        let source = Connection::open(&fixture.from.join("nx/workspace-data/task-v3.db")).unwrap();
        source
            .execute(
                "WITH RECURSIVE hashes(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM hashes WHERE n < 10000) \
                 INSERT INTO task_details SELECT CAST(n AS TEXT), 'a', 'build', NULL FROM hashes",
                &[],
            )
            .unwrap();
        source
            .execute(
                "INSERT INTO cache_outputs (hash, code, size) SELECT hash, 0, 1024 FROM task_details \
                 WHERE hash = '1' OR CAST(hash AS INTEGER) > 9900",
                &[],
            )
            .unwrap();
        source.execute("ANALYZE", &[]).unwrap();
        drop(source);
        let connection = attached(
            &fixture.into.join("nx/workspace-data/task-v3.db"),
            &fixture.from.join("nx/workspace-data/task-v3.db"),
        )
        .unwrap();
        let selected = TaskSelection {
            hashes: BTreeSet::from(["1".to_owned()]),
            deferred: BTreeSet::from([task("a", "build", None)]),
        };
        let selected = rows(&connection, Selection::Only(&selected)).unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|row| (row.hash.as_str(), row.by_task))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([("1", false), ("9999", true)]),
            "the hash, and the one latest entry of the deferred task"
        );
        let mut explain = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {SELECTED}"))
            .unwrap();
        let mut plan = Vec::new();
        while explain.step().unwrap() {
            plan.push(explain.text(3));
        }
        for searched in ["p", "d"] {
            assert!(
                plan.iter().any(|detail| {
                    detail.starts_with(&format!("SEARCH {searched} USING "))
                        && detail.contains("(hash=?)")
                }),
                "{searched}: {plan:?}"
            );
        }
        assert!(
            plan.iter()
                .any(|detail| detail.starts_with("SEARCH t USING ")),
            "{plan:?}"
        );
        for scanned in ["SCAN p", "SCAN d", "SCAN t"] {
            assert!(
                !plan.iter().any(|detail| detail.starts_with(scanned)),
                "{scanned}: {plan:?}"
            );
        }
    }

    #[test]
    fn publication_probes_both_source_indexes_instead_of_scanning_task_history() {
        let fixture = Fixture::new();
        let source = Connection::open(&fixture.from.join("nx/workspace-data/task-v3.db")).unwrap();
        source.execute(
            "WITH RECURSIVE hashes(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM hashes WHERE n < 10000) \
             INSERT INTO task_details SELECT CAST(n AS TEXT), 'a', 'build', NULL FROM hashes", &[],
        ).unwrap();
        source.execute(
            "INSERT INTO cache_outputs (hash, code, size) SELECT hash, 0, 1024 FROM task_details", &[],
        ).unwrap();
        let connection = attached(
            &fixture.into.join("nx/workspace-data/task-v3.db"),
            &fixture.from.join("nx/workspace-data/task-v3.db"),
        )
        .unwrap();
        index(&connection, &BTreeSet::from(["1"])).unwrap();
        for (query, alias) in [(INDEX_DETAILS, "d"), (INDEX_OUTPUTS, "p")] {
            let mut explain = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
                .unwrap();
            let mut plan = Vec::new();
            while explain.step().unwrap() {
                plan.push(explain.text(3));
            }
            assert!(
                plan.iter().any(
                    |detail| detail.starts_with(&format!("SEARCH {alias} USING "))
                        && detail.contains("(hash=?)")
                ),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|detail| detail.starts_with(&format!("SCAN {alias}"))),
                "{plan:?}"
            );
            cowshed_os::eprintln!("publication indexed plan: {plan:?}");
        }
        assert_eq!(fixture.carried_hashes(), BTreeSet::from(["1".into()]));
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
        fs::remove_file(fixture.into.join("nx/workspace-data/task-v3.db")).unwrap();
        let staged = fixture.stage(&["1"]);
        assert!(staged.databases.is_empty());
        assert!(
            staged
                .stopped
                .as_ref()
                .unwrap()
                .contains("unbounded task history")
        );
        assert!(!fixture.into.join("nx/workspace-data/task-v3.db").exists());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    #[test]
    fn a_differently_named_destination_database_stops_the_carry_and_names_both_paths() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        fs::rename(
            fixture.into.join("nx/workspace-data/task-v3.db"),
            fixture.into.join("nx/workspace-data/other-v3.db"),
        )
        .unwrap();
        let staged = fixture.stage(&["1"]);
        assert!(staged.databases.is_empty());
        let stopped = staged.stopped.as_ref().unwrap();
        assert!(
            stopped.contains(
                &fixture
                    .from
                    .join("nx/workspace-data/task-v3.db")
                    .display()
                    .to_string()
            ),
            "{stopped}"
        );
        assert!(
            stopped.contains(
                &fixture
                    .into
                    .join("nx/workspace-data/task-v3.db")
                    .display()
                    .to_string()
            ),
            "{stopped}"
        );
        let carried = commit(staged, &fixture.into);
        assert_eq!(carried.entries, 0);
        assert!(carried.stopped.is_some());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    #[test]
    fn staged_stop_reason_reaches_the_carried_report_verbatim() {
        let fixture = Fixture::new();
        let reason = "Nx cache directory changed: expected a, actual b";
        let carried = commit(Staged::stopped(reason.into()), &fixture.into);
        assert_eq!(carried.stopped.as_deref(), Some(reason));
        assert_eq!(carried.entries, 0);
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
        let from = root.join("from-v3.db");
        let into = root.join("into-v3.db");
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

    #[test]
    fn the_pinned_row_shim_refuses_schema_or_version_changes_before_publication() {
        let fixture = Fixture::new();
        let source = fixture.from.join("nx/workspace-data/task-v3.db");
        let destination = fixture.into.join("nx/workspace-data/task-v3.db");
        let changed = Connection::open(&source).unwrap();
        changed
            .execute(
                "ALTER TABLE cache_outputs ADD COLUMN future_required_data TEXT",
                &[],
            )
            .unwrap();
        drop(changed);
        let error = match attached(&destination, &source) {
            Ok(_) => panic!("a changed schema must not be admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("row schema changed"), "{error}");
        assert!(fixture.carried_hashes().is_empty());
        let future = fixture.from.join("nx/workspace-data/task-v4.db");
        fs::rename(&source, &future).unwrap();
        let error = match attached(&destination, &future) {
            Ok(_) => panic!("a changed schema version must not be admitted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("requires a v3 database"),
            "{error}"
        );
        assert!(fixture.carried_hashes().is_empty());
    }

    #[test]
    fn the_pinned_row_shim_refuses_source_indexes_and_triggers_before_any_cache_row() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let source = fixture.from.join("nx/workspace-data/task-v3.db");
        let destination = fixture.into.join("nx/workspace-data/task-v3.db");
        let changed = Connection::open(&source).unwrap();
        changed
            .execute(
                "CREATE INDEX cache_outputs_size ON cache_outputs (size)",
                &[],
            )
            .unwrap();
        drop(changed);
        let error = match attached(&destination, &source) {
            Ok(_) => panic!("an explicit index on cache_outputs must not be admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("row schema changed"), "{error}");
        assert!(fixture.carried_hashes().is_empty());
        let changed = Connection::open(&source).unwrap();
        changed
            .execute("DROP INDEX cache_outputs_size", &[])
            .unwrap();
        changed
            .execute(
                "CREATE TRIGGER cache_outputs_audit AFTER INSERT ON cache_outputs BEGIN SELECT 1; END",
                &[],
            )
            .unwrap();
        drop(changed);
        let error = match attached(&destination, &source) {
            Ok(_) => panic!("a trigger on cache_outputs must not be admitted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("row schema changed"), "{error}");
        assert!(fixture.carried_hashes().is_empty());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    #[test]
    fn the_destination_row_schema_change_refuses_attachment_before_any_publication() {
        let fixture = Fixture::new();
        fixture.entry("1", 4, b"warm");
        let source = fixture.from.join("nx/workspace-data/task-v3.db");
        let destination = fixture.into.join("nx/workspace-data/task-v3.db");
        let changed = Connection::open(&destination).unwrap();
        changed
            .execute(
                "ALTER TABLE task_details ADD COLUMN future_required_data TEXT",
                &[],
            )
            .unwrap();
        drop(changed);
        let error = match attached(&destination, &source) {
            Ok(_) => panic!("a changed destination row schema must not be admitted"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("row schema changed in main"),
            "{error}"
        );
        assert!(fixture.carried_hashes().is_empty());
        assert!(!fixture.into.join("nx/cache/1").exists());
    }

    /// Consumes the real existing pinned Nx product through the same adapter as production.
    #[test]
    #[ignore = "unsandboxed host-controller RAW/Nx compatibility proof; run with `nx run cowshed:host-controller-test`; requires the checkout's Bun-installed patched Nx"]
    fn host_controller_raw_carry_opens_restores_stores_and_evicts_with_native_nx() {
        use cowshed_os::fork_lock::Run as _;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        use std::process::Command;

        let checkout = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
            .join("../../../..")
            .canonicalize()
            .unwrap();
        let build = checkout
            .join(".cowshed/build")
            .canonicalize()
            .expect("run this proof from its owned warm RAW checkout");
        let proof = format!(".carry-proof-{}", uuid::Uuid::new_v4().simple());
        let from = std::env::temp_dir().join(&proof);
        let into = build.join(&proof);
        fs::create_dir_all(&from).unwrap();
        fs::create_dir_all(&into).unwrap();
        let fixture = Fixture {
            root: from.clone(),
            from: from.clone(),
            into: into.clone(),
            state: BuildVolumeState {
                paths: vec![
                    super::super::BuildStatePath::new(".nx/cache", ".nx/cache").unwrap(),
                    super::super::BuildStatePath::new(".nx/workspace-data", ".nx/workspace-data")
                        .unwrap(),
                ],
                fingerprint: None,
            },
        };
        assert_ne!(
            fs::metadata(&from).unwrap().dev(),
            fs::metadata(&into).unwrap().dev(),
            "copy source and destination must be separate mounted RAW volumes"
        );
        let product = checkout
            .join("node_modules/nx")
            .canonicalize()
            .expect("install the pinned Bun-patched Nx before the RAW carry proof");
        for root in [&from, &into] {
            fs::write(
                root.join("package.json"),
                br#"{"name":"carry-proof","private":true}"#,
            )
            .unwrap();
            fs::create_dir(root.join("node_modules")).unwrap();
            std::os::unix::fs::symlink(&product, root.join("node_modules/nx")).unwrap();
        }
        const BODY: &str = r#"
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = process.argv[1], phase = process.argv[2];
fs.writeFileSync(path.join(root, 'nx.json'), JSON.stringify({maxCacheSize: phase === 'verify' ? 1 : 0}));
const {requireNx, cache, cacheDir, getLocal} = openNxCache(root);
const nx = requireNx('nx/src/native');
const db = requireNx('nx/src/utils/db-connection').getDbConnection();
const details = new nx.TaskDetails(db);
const task = {id:'a:build', target:{project:'a',target:'build'}, projectRoot:'.',
  overrides:{}, outputs:['output'], cache:true, parallelism:true, continuous:false, inputs:[]};
async function main() {
  if (phase === 'create') {
    fs.writeFileSync(path.join(root, 'output'), 'warm');
    fs.chmodSync(path.join(root, 'output'), 0o440);
    details.recordTaskDetails([{hash:'1', project:'a', target:'build'}]);
    await cache.put({...task, hash:'1'}, 'terminal', ['output'], 0);
    new nx.NxTaskHistory(db).recordTaskRuns([{hash:'1', status:'success', code:0, start:1, end:2}]);
    assert.equal(getLocal('1').code, 0);
  } else if (phase === 'verify') {
    const hit = getLocal('1');
    assert.equal(hit.code, 0);
    assert.equal(hit.terminalOutput, 'terminal');
    assert.equal(hit.size, 12);
    const cachedEntryPath = path.join(cacheDir, '1/output');
    const cacheBytesBefore = fs.readFileSync(cachedEntryPath);
    const cachedBefore = fs.statSync(cachedEntryPath, {bigint:true});
    const beforeMs = Date.now();
    const copiedBytes = await cache.copyFilesFromCache({...task, hash:'1'}, hit, ['output']);
    const afterMs = Date.now();
    assert.equal(copiedBytes, 4);
    assert.equal(fs.readFileSync(path.join(root, 'output'), 'utf8'), 'warm');
    const restored = fs.statSync(path.join(root, 'output'));
    assert.equal(restored.mode & 0o777, 0o440);
    assert.ok(restored.mtimeMs >= beforeMs && restored.mtimeMs <= afterMs,
      `restored mtime ${restored.mtimeMs} outside [${beforeMs}, ${afterMs}]`);
    const cachedAfter = fs.statSync(cachedEntryPath, {bigint:true});
    assert.deepEqual(fs.readFileSync(cachedEntryPath), cacheBytesBefore);
    assert.equal(cachedAfter.mode, cachedBefore.mode);
    assert.equal(cachedAfter.mtimeNs, cachedBefore.mtimeNs);
    details.recordTaskDetails([{hash:'2', project:'a', target:'build'}]);
    fs.chmodSync(path.join(root, 'output'), 0o640);
    fs.writeFileSync(path.join(root, 'output'), 'new output');
    await cache.put({...task, hash:'2'}, 'new terminal', ['output'], 0);
    assert.equal(cache.getUsedCacheSpace(), 0);
    assert.equal(getLocal('1'), null);
    assert.equal(getLocal('2'), null);
    assert.equal(fs.existsSync(path.join(cacheDir, '1')), false);
    assert.equal(fs.existsSync(path.join(cacheDir, '2')), false);
    console.log('pinned DbCache open/get/restore/store/eviction: passed; restore copied 4 bytes');
  } else if (phase === 'add3' || phase === 'add4') {
    const hash = phase === 'add3' ? '3' : '4';
    details.recordTaskDetails([{hash, project:'a', target:'build'}]);
    const output = path.join(root, 'output');
    if (fs.existsSync(output)) fs.chmodSync(output, 0o640);
    fs.writeFileSync(output, 'warm');
    await cache.put({...task, hash}, 'terminal', ['output'], 0);
    assert.equal(getLocal(hash).code, 0);
  } else if (phase !== 'initialize') {
    throw new Error(`unknown test phase ${phase}`);
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
"#;
        let script = super::super::cache_adapter::script(BODY);
        let script = std::str::from_utf8(&script).unwrap();
        let run = |root: &Path, phase: &str| {
            let mut command = Command::new("node");
            command.args(["-e", script]).arg(root).arg(phase);
            // The host shell's Nx environment is not this proof's: its NX_MAX_CACHE_SIZE outranks
            // the nx.json maxCacheSize the verify phase evicts with (Nx resolveMaxCacheSize).
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                    command.env_remove(key);
                }
            }
            let output = command
                .env("NX_SKIP_NATIVE_FILE_CACHE", "true")
                .output_locked()
                .expect("spawn real Node with the accepted Nx product");
            assert!(
                output.status.success(),
                "pinned Nx {phase}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            cowshed_os::eprint!("{}", String::from_utf8_lossy(&output.stdout));
        };
        run(&from, "create");
        run(&into, "initialize");
        let names = databases_in(&from.join(".nx/workspace-data")).unwrap();
        assert_eq!(names.len(), 1);
        let source_db = from.join(".nx/workspace-data").join(&names[0]);
        let destination_db = into.join(".nx/workspace-data").join(&names[0]);
        assert_eq!(indexed(&destination_db), 0);
        let initialized = Connection::open(&destination_db).unwrap();
        for table in [
            "task_details",
            "task_history",
            "running_tasks",
            "task_invocations",
            "metadata",
        ] {
            let mut count = initialized
                .prepare(&format!("SELECT count(*) FROM {table}"))
                .unwrap();
            assert!(count.step().unwrap());
            assert_eq!(
                count.integer(0),
                0,
                "Nx initializes {table}, not a foreign-history copy"
            );
        }
        drop(initialized);
        let original = fs::metadata(from.join(".nx/cache/1/output")).unwrap();
        WORK.with(|work| work.set(CopyWork::default()));
        let started = std::time::Instant::now();
        let staged = stage_rebase(
            &from,
            &fixture.state,
            &into,
            &fixture.state,
            &Fixture::selections(&["1"]),
            Path::new(cowshed_gateway_types::STORE_ROOT),
        );
        assert!(staged.stopped.is_none(), "{staged:?}");
        assert_eq!(indexed(&destination_db), 0, "files first, row last");
        let stage_work = WORK.with(|work| work.get());
        let carried = commit(staged, &into);
        assert_eq!(carried.entries, 1);
        assert_eq!(carried.bytes, 12);
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!(
            WORK.with(|work| work.get()),
            stage_work,
            "commit renames without a second walk or copy"
        );
        assert_eq!(
            stage_work,
            CopyWork {
                objects: 3,
                bytes: 12,
                metadata_calls: 3
            }
        );
        let copied = fs::metadata(into.join(".nx/cache/1/output")).unwrap();
        assert_eq!(copied.modified().unwrap(), original.modified().unwrap());
        assert_eq!(
            copied.permissions().mode() & 0o777,
            original.permissions().mode() & 0o777
        );
        let published = Connection::open(&destination_db).unwrap();
        let source = Connection::open(&source_db).unwrap();
        for column in ["code", "size", "created_at", "accessed_at"] {
            let mut original = source
                .prepare(&format!(
                    "SELECT {column} FROM cache_outputs WHERE hash='1'"
                ))
                .unwrap();
            let mut copied = published
                .prepare(&format!(
                    "SELECT {column} FROM cache_outputs WHERE hash='1'"
                ))
                .unwrap();
            assert!(original.step().unwrap() && copied.step().unwrap());
            assert_eq!(original.text(0), copied.text(0), "{column}");
        }
        drop(published);
        drop(source);
        run(&from, "add3");
        let all = stage(&from, &fixture.state, &into, &fixture.state);
        assert!(all.stopped.is_none(), "{all:?}");
        run(&from, "add4");
        let legacy = commit(all, &into);
        assert_eq!(legacy.entries, 2, "{legacy:?}");
        assert_eq!(legacy.bytes, 24, "{legacy:?}");
        assert!(legacy.stopped.is_none(), "{legacy:?}");
        for hash in ["3", "4"] {
            assert_eq!(
                fs::read(into.join(format!(".nx/cache/{hash}/output"))).unwrap(),
                b"warm"
            );
            assert_eq!(
                fs::read(into.join(format!(".nx/cache/terminalOutputs/{hash}"))).unwrap(),
                b"terminal"
            );
        }
        cowshed_os::eprintln!(
            "real RAW carry: {stage_work:?}, rows={}, elapsed={:?}, Nx-initialized DB={} bytes",
            carried.entries,
            started.elapsed(),
            fs::metadata(&destination_db).unwrap().len()
        );
        run(&into, "verify");
        drop(fixture);
    }
}
