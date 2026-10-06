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

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
    let mut staged = Staged::default();
    let staging = into.join(STAGING);
    if let Err(error) = remove(&staging) {
        staged.stopped = Some(format!("clear {}: {error}", staging.display()));
        return staged;
    }
    let targets = nx_states(into_state);
    for source in nx_states(from_state) {
        let Some(destination) = targets
            .iter()
            .find(|state| state.checkout == source.checkout)
        else {
            continue;
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
                from_database,
                from_cache: from.join(source.cache),
                into_database,
                into_cache: into.join(destination.cache),
                staging: staging.join(staged.databases.len().to_string()),
                rows: Vec::new(),
            };
            let result = stage_database(&mut database);
            staged.databases.push(database);
            if let Err(error) = result {
                staged.stopped = Some(error);
                return staged;
            }
        }
    }
    staged
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
pub fn commit(staged: Staged, into: &Path) -> Carried {
    let mut carried = Carried {
        stopped: staged.stopped.clone(),
        ..Carried::default()
    };
    let complete = staged.stopped.is_none();
    for database in &staged.databases {
        match commit_database(database, complete) {
            Ok((entries, bytes)) => {
                carried.entries += entries;
                carried.bytes += bytes;
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
fn stage_database(database: &mut StagedDatabase) -> Result<(), String> {
    let context = |error: io::Error| {
        format!(
            "stage {} into {}: {error}",
            database.from_database.display(),
            database.into_database.display()
        )
    };
    // The connection closes before the copies: phase 1 holds the target's database only for
    // the one read.
    let missing = missing(&database.into_database, &database.from_database).map_err(context)?;
    for row in missing {
        match copy_entry(&database.from_cache, &database.staging, &row.hash) {
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
fn missing(into_database: &Path, from_database: &Path) -> io::Result<Vec<Row>> {
    let connection = attached(into_database, from_database)?;
    rows(&connection)
}

/// [`MISSING`]: none when the target's database has no `cache_outputs`, so indexes nothing.
fn rows(connection: &Connection) -> io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    if table_definition(connection, "previous", "cache_outputs")?.is_none() {
        return Ok(rows);
    }
    let mut statement = connection.prepare(MISSING)?;
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
fn commit_database(database: &StagedDatabase, complete: bool) -> io::Result<(u64, u64)> {
    let connection = attached(&database.into_database, &database.from_database)?;
    // Every row the target indexes now that the landing volume lacks, staged or not.
    let now = rows(&connection)?;
    let current: BTreeSet<&Row> = now.iter().collect();
    let unchanged: BTreeSet<&str> = database
        .rows
        .iter()
        .filter(|staged| current.contains(staged))
        .map(|staged| staged.hash.as_str())
        .collect();
    // Each entry is named here before its files move, so a failure removes it with the rest.
    let mut placed: BTreeSet<&str> = BTreeSet::new();
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
                if copy_entry(&database.from_cache, &database.into_cache, &row.hash)? {
                    placed.insert(&row.hash);
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
    Ok((placed.len() as u64, bytes))
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
/// have one.
fn index(connection: &Connection, placed: &BTreeSet<&str>) -> io::Result<()> {
    if placed.is_empty() {
        return Ok(());
    }
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

/// Copy entry `hash`'s output directory and terminal output from the cache `from` to the same
/// names under `into`. `false` when `from` holds no output directory for it: Nx would answer
/// that row with a hit that restores nothing, so it is not carried. A copy that fails removes
/// what it wrote.
fn copy_entry(from: &Path, into: &Path, hash: &str) -> io::Result<bool> {
    let source = from.join(hash);
    match fs::symlink_metadata(&source) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    remove_entry(into, hash)?;
    fs::create_dir_all(into)?;
    let output = from.join(TERMINAL_OUTPUTS).join(hash);
    let copied = copyfile(
        &source,
        &into.join(hash),
        COPYFILE_ALL | libc::COPYFILE_RECURSIVE | libc::COPYFILE_NOFOLLOW,
    )
    .and_then(|()| match fs::symlink_metadata(&output) {
        Ok(_) => fs::create_dir_all(into.join(TERMINAL_OUTPUTS)).and_then(|()| {
            copyfile(
                &output,
                &into.join(TERMINAL_OUTPUTS).join(hash),
                COPYFILE_ALL | libc::COPYFILE_NOFOLLOW,
            )
        }),
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
