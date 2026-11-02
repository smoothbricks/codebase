//! Removing a scratch tree with several threads unlinking at once.
//!
//! A fixture's checkout clones Bun's install namespace and its store copies it again, tens of
//! thousands of entries, and APFS unlinks one entry per call. Each fixture removes its own tree
//! this way when it is dropped, a keeper when its owner died, and the lane setup what a fallback
//! or a killed run left (08_testing.md, "Fixtures").

use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

/// How many threads unlink one tree. Measured with `remove_dir_all` against this remover over the
/// same 80 dead scratch roots (122,089 entries) on the host's APFS data volume: 9.7 s against
/// 7.7 s at load 22-44, 9.4 s against 7.5 s at load 87-95, 17.3 s against 9.9 s at load 98. Four
/// threads and sixteen did no better than eight.
const REMOVERS: usize = 8;

/// Remove `root` and everything below it, never following a link; a `root` already gone is
/// removed. A tool may have sealed a directory below it (Go's module cache is 0555): a removal
/// refused for permission restores the owner's access to the directories this user owns
/// ([`cowshed_os::fsio::grant_owner_directory_access`], as `remove_owned_tree` does) and removes
/// once more. A failure leaves what is not yet removed in place and names the entry that refused.
pub(crate) fn remove(root: &Path) -> io::Result<()> {
    match remove_once(root) {
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            cowshed_os::fsio::grant_owner_directory_access(root)?;
            remove_once(root)
        }
        removed => removed,
    }
}

/// One directory of the tree: the coordinator's alone, so no thread shares a mutable count.
struct Directory {
    path: PathBuf,
    parent: Option<usize>,
    /// Its subdirectories not yet removed.
    pending: usize,
}

/// What a remover thread is asked to do with a directory, by its index in the coordinator's list.
enum Job {
    /// Unlink every entry that is not a directory and name the subdirectories.
    Empty(usize, PathBuf),
    /// Remove the directory, empty by now.
    Remove(usize, PathBuf),
}

/// A remover thread's answer to a [`Job`].
enum Done {
    Emptied(usize, io::Result<Vec<PathBuf>>),
    Removed(usize, io::Result<()>),
}

fn remove_once(root: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(named("inspect", root, error)),
        Ok(metadata) if !metadata.is_dir() => {
            return gone_or(std::fs::remove_file(root))
                .map_err(|error| named("unlink", root, error));
        }
        Ok(_) => {}
    }
    let (answers, answered) = mpsc::channel::<(usize, Done)>();
    std::thread::scope(|scope| {
        let mut removers = Vec::with_capacity(REMOVERS);
        for _ in 0..REMOVERS {
            let remover = removers.len();
            let (jobs, queue) = mpsc::channel::<Job>();
            let answers = answers.clone();
            let spawned = std::thread::Builder::new()
                .name("scratch-remove".to_owned())
                .spawn_scoped(scope, move || {
                    for job in queue {
                        if answers.send((remover, run(job))).is_err() {
                            return;
                        }
                    }
                });
            if spawned.is_ok() {
                removers.push(jobs);
            }
        }
        // Only the removers answer: with every one gone, the coordinator's wait ends.
        drop(answers);
        if removers.is_empty() {
            cowshed_os::eprintln!(
                "scratch tree {}: no thread to remove it beside this one; removing it in turn",
                root.display()
            );
            return gone_or(std::fs::remove_dir_all(root))
                .map_err(|error| named("remove", root, error));
        }
        coordinate(root, &removers, &answered)
    })
}

/// Hand the tree's directories to idle removers, deepest last to be removed first: a directory is
/// removed once it is emptied and its last subdirectory is gone. The first failure stops new work;
/// the removers still busy finish theirs, and the failure is the answer.
fn coordinate(
    root: &Path,
    removers: &[mpsc::Sender<Job>],
    answered: &mpsc::Receiver<(usize, Done)>,
) -> io::Result<()> {
    let mut directories = vec![Directory {
        path: root.to_path_buf(),
        parent: None,
        pending: 0,
    }];
    let mut ready = vec![Job::Empty(0, root.to_path_buf())];
    let mut idle: Vec<usize> = (0..removers.len()).collect();
    let mut busy = 0_usize;
    let mut failed = None;
    loop {
        while failed.is_none() && !ready.is_empty() && !idle.is_empty() {
            let (Some(job), Some(remover)) = (ready.pop(), idle.pop()) else {
                break;
            };
            if removers[remover].send(job).is_err() {
                failed = Some(io::Error::other("a scratch remover thread ended early"));
                break;
            }
            busy += 1;
        }
        if busy == 0 {
            return failed.map_or(Ok(()), Err);
        }
        let Ok((remover, done)) = answered.recv() else {
            return Err(failed.unwrap_or_else(|| {
                io::Error::other("the scratch remover threads ended without an answer")
            }));
        };
        busy -= 1;
        idle.push(remover);
        let emptied = match done {
            Done::Emptied(directory, Ok(subdirectories)) => {
                directories[directory].pending = subdirectories.len();
                for path in subdirectories {
                    ready.push(Job::Empty(directories.len(), path.clone()));
                    directories.push(Directory {
                        path,
                        parent: Some(directory),
                        pending: 0,
                    });
                }
                Some(directory)
            }
            Done::Removed(directory, Ok(())) => directories[directory].parent.inspect(|&parent| {
                directories[parent].pending -= 1;
            }),
            Done::Emptied(_, Err(error)) | Done::Removed(_, Err(error)) => {
                failed.get_or_insert(error);
                None
            }
        };
        if let Some(directory) = emptied.filter(|&directory| directories[directory].pending == 0) {
            ready.push(Job::Remove(directory, directories[directory].path.clone()));
        }
    }
}

/// Do one [`Job`] on a remover thread. An entry already gone counts as removed.
fn run(job: Job) -> Done {
    match job {
        Job::Empty(directory, path) => Done::Emptied(directory, empty(&path)),
        Job::Remove(directory, path) => Done::Removed(
            directory,
            gone_or(std::fs::remove_dir(&path)).map_err(|error| named("remove", &path, error)),
        ),
    }
}

/// Unlink every entry of `directory` that is not a directory, links included and never followed,
/// and name its subdirectories.
fn empty(directory: &Path) -> io::Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(directory) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        entries => entries.map_err(|error| named("list", directory, error))?,
    };
    let mut subdirectories = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| named("list", directory, error))?;
        let path = entry.path();
        if entry
            .file_type()
            .map_err(|error| named("inspect", &path, error))?
            .is_dir()
        {
            subdirectories.push(path);
        } else {
            gone_or(std::fs::remove_file(&path)).map_err(|error| named("unlink", &path, error))?;
        }
    }
    Ok(subdirectories)
}

fn gone_or(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

fn named(what: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{what} {}: {error}", path.display()))
}
