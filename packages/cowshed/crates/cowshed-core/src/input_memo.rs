//! An answer read from a few files, kept while those files keep their identity.
//!
//! Some readings cost a process: `git ls-files` for the tracked build inputs, `mbx settings get`
//! for the host scheduler. Admission asks them again for every exec (15_capabilities.md,
//! "Detection inputs"), and their answer moves only when a file the reading depends on moves. An
//! [`InputMemo`] keeps the last answer beside the identity ([`FileState`]: inode, size, mtime,
//! ctime) each of those files had, and answers from it while every one of them still has that
//! identity. An absent input is watched through the nearest directory above it that exists.
//! While that directory keeps its identity the input is still absent; when it moves (a
//! sibling's create or remove, such as Git's `index.lock`, moves it too), the input and each
//! directory on its way are looked up again, and the answer stands while the input is still
//! absent, now watched through the nearest existing directory again. A file whose last
//! change is not yet [`SETTLED`] behind the clock when the reading starts could be rewritten
//! again within its timestamp's tick without moving its identity (a coarse Linux tick, an HFS+
//! second), so an answer that read it is used once and not kept; a creation made while a
//! reading runs moves its watching directory inside that window and is never kept. Bytes are
//! never compared: the reading is the authority, and a moved identity only means it runs again
//! (11_shell.md, "Kept readings").
//!
//! The decisions are pure ([`watched`], [`stands`], [`keeps`]): an ask's identities and the time
//! it started are their inputs, so a model of coarse filesystem clocks checks them (`tests`);
//! [`InputMemo`] only reads identities and the clock, runs the reading, and publishes. An ask
//! loads one published map; keeping an answer publishes a new one. Nothing locks.

use std::collections::HashMap;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwap;

use crate::runtime::shell_watch::FileState;

/// How far behind the clock a file's last change must be before an answer that read it is kept:
/// past the coarsest timestamp tick an identity is read at (a second on HFS+), with room for a
/// process clock that runs ahead of a filesystem's stamps.
pub(crate) const SETTLED: Duration = Duration::from_secs(3);

/// Answers by key, each kept with the identities of the files it was read from.
pub(crate) struct InputMemo<K, V> {
    kept: ArcSwap<HashMap<K, Arc<Kept<V>>>>,
}

struct Kept<V> {
    /// The inputs the answer was asked for.
    inputs: Box<[PathBuf]>,
    /// Each input and, for an absent one, the nearest existing directory watching it.
    watched: Box<[WatchedInput]>,
    value: V,
}

#[derive(Clone, Debug)]
enum WatchedInput {
    /// Input was present with this identity and must keep it.
    Present { path: PathBuf, identity: FileState },
    /// Input was absent, watched through the nearest existing directory above it (if any).
    Absent {
        path: PathBuf,
        watcher: Option<(PathBuf, FileState)>,
    },
}

enum Standing {
    /// Every present input kept its identity, and every absent input is still absent with no
    /// settled re-anchoring to publish.
    Valid,
    /// Every input still stands, and at least one moved watcher re-anchored to a settled
    /// directory to publish for the next ask.
    Reanchored(Box<[WatchedInput]>),
    /// At least one present input moved or at least one absent input is no longer absent.
    Invalid,
}

impl<K: Clone + Eq + Hash, V: Clone> InputMemo<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            kept: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// `read`'s answer for `key`, which depends on the files `inputs` and on nothing else that
    /// can change while this process runs: the kept one while every input stands, else a fresh
    /// reading. A failed reading is returned and keeps nothing.
    pub(crate) fn read<E>(
        &self,
        key: &K,
        inputs: &[PathBuf],
        read: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        let started = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(started) => started,
        };
        let value = read()?;
        self.keep(key, started, inputs.to_vec(), &value);
        Ok(value)
    }

    /// [`Self::read`] for a reading that awaits.
    pub(crate) async fn read_async<E, F>(
        &self,
        key: &K,
        inputs: &[PathBuf],
        read: impl FnOnce() -> F,
    ) -> Result<V, E>
    where
        F: Future<Output = Result<V, E>>,
    {
        let started = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(started) => started,
        };
        let value = read().await?;
        self.keep(key, started, inputs.to_vec(), &value);
        Ok(value)
    }

    /// [`Self::read`] for a reading that learns its inputs only by reading them: it answers its
    /// value and every path whose identity decides it. Their identities are taken after it,
    /// and its answer is kept only when each was settled before it started ([`keeps`]): a change
    /// made while it ran stamps no earlier than its start, less a stamping tick, so a kept
    /// answer's identities are the ones its reading saw, and a later change moves one.
    pub(crate) fn read_recorded<E>(
        &self,
        key: &K,
        read: impl FnOnce() -> Result<(V, Vec<PathBuf>), E>,
    ) -> Result<V, E> {
        if let Some(kept) = self.standing(key) {
            return Ok(kept);
        }
        let started = now_ns();
        let (value, inputs) = read()?;
        self.keep(key, started, inputs, &value);
        Ok(value)
    }

    /// [`Self::read_recorded`] for a reading that awaits.
    pub(crate) async fn read_recorded_async<E, F>(
        &self,
        key: &K,
        read: impl FnOnce() -> F,
    ) -> Result<V, E>
    where
        F: Future<Output = Result<(V, Vec<PathBuf>), E>>,
    {
        if let Some(kept) = self.standing(key) {
            return Ok(kept);
        }
        let started = now_ns();
        let (value, inputs) = read().await?;
        self.keep(key, started, inputs, &value);
        Ok(value)
    }

    /// The answer kept for `key` by a recorded reading, while every path it recorded stands.
    fn standing(&self, key: &K) -> Option<V> {
        let kept = self.kept.load().get(key).cloned()?;
        self.check(key, &kept)
    }

    /// The kept answer for `key` when it stands for `inputs` now; else the clock reading before
    /// the fresh reading starts, against which the post-read observation is checked for settling.
    fn ask(&self, key: &K, inputs: &[PathBuf]) -> Result<V, Option<i128>> {
        if let Some(kept) = self.kept.load().get(key).cloned()
            && *kept.inputs == *inputs
            && let Some(value) = self.check(key, &kept)
        {
            return Ok(value);
        }
        Err(now_ns())
    }

    /// Whether `kept` still stands now, re-anchoring moved watchers when their new directory is
    /// already settled.
    fn check(&self, key: &K, kept: &Arc<Kept<V>>) -> Option<V> {
        let checked_at = now_ns();
        match stands(&kept.watched, checked_at, FileState::of) {
            Standing::Valid => Some(kept.value.clone()),
            Standing::Reanchored(watched) => {
                self.reanchor(key, kept, watched);
                Some(kept.value.clone())
            }
            Standing::Invalid => None,
        }
    }

    /// Keep `value`, read from `inputs` by a reading that started at `started`, observing
    /// identities after the reading so a creation made during it moves its watcher inside the
    /// settling window and is never kept.
    fn keep(&self, key: &K, started: Option<i128>, inputs: Vec<PathBuf>, value: &V) {
        let (watched, identities) = observe(&inputs);
        self.publish(key, inputs.into(), &watched, &identities, started, value);
    }

    /// Publish `value` for `key` when every watched identity was settled at `started`.
    fn publish(
        &self,
        key: &K,
        inputs: Box<[PathBuf]>,
        watched: &[PathBuf],
        identities: &[FileState],
        started: Option<i128>,
        value: &V,
    ) {
        if !started.is_some_and(|started| keeps(identities, started)) {
            return;
        }
        let kept = Arc::new(Kept {
            watched: watched_inputs(&inputs, watched, identities),
            inputs,
            value: value.clone(),
        });
        self.kept.rcu(|answers| {
            let mut answers = HashMap::clone(answers);
            answers.insert(key.clone(), Arc::clone(&kept));
            answers
        });
    }

    /// Publish a re-anchored copy of `previous` if `key` still points at `previous`.
    fn reanchor(&self, key: &K, previous: &Arc<Kept<V>>, watched: Box<[WatchedInput]>) {
        let reanchored = Arc::new(Kept {
            inputs: previous.inputs.clone(),
            watched,
            value: previous.value.clone(),
        });
        self.kept.rcu(|answers| {
            if !answers
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, previous))
            {
                return Arc::clone(answers);
            }
            let mut answers = HashMap::clone(answers);
            answers.insert(key.clone(), Arc::clone(&reanchored));
            Arc::new(answers)
        });
    }
}

/// The paths that decide an answer read from `inputs`, and the identity of each now.
fn observe(inputs: &[PathBuf]) -> (Vec<PathBuf>, Vec<FileState>) {
    observe_with(inputs, FileState::of)
}

fn observe_with(
    inputs: &[PathBuf],
    mut state: impl FnMut(&std::path::Path) -> FileState,
) -> (Vec<PathBuf>, Vec<FileState>) {
    let mut identities: Vec<FileState> = inputs.iter().map(|path| state(path)).collect();
    let mut watcher_identities = Vec::new();
    let watched = watched(inputs, &identities, |directory| {
        let found = state(directory);
        if !matches!(found, FileState::Missing) {
            watcher_identities.push(found);
        }
        found
    });
    identities.extend(watcher_identities);
    (watched, identities)
}

/// The paths whose identities decide an answer read from `inputs`, which had `identities`: every
/// input, and for each absent one the nearest directory above it that exists by `state`, whose
/// entries change when the input or a directory on its way is created, even if it is removed
/// again before the next ask.
fn watched(
    inputs: &[PathBuf],
    identities: &[FileState],
    mut state: impl FnMut(&std::path::Path) -> FileState,
) -> Vec<PathBuf> {
    let absent = inputs
        .iter()
        .zip(identities)
        .filter(|(_, identity)| matches!(identity, FileState::Missing))
        .filter_map(|(path, _)| {
            path.ancestors()
                .skip(1)
                .find(|directory| !matches!(state(directory), FileState::Missing))
                .map(PathBuf::from)
        });
    inputs.iter().cloned().chain(absent).collect()
}

/// This process's clock, in nanoseconds since the epoch; `None` before it.
pub(crate) fn now_ns() -> Option<i128> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|now| i128::try_from(now.as_nanos()).ok())
}

/// Pair each input from [`observe_with`] with its observed state and, for an absent input, the
/// nearest existing watcher directory `watched` appended for it.
fn watched_inputs(
    inputs: &[PathBuf],
    watched: &[PathBuf],
    identities: &[FileState],
) -> Box<[WatchedInput]> {
    let mut watchers = watched[inputs.len()..]
        .iter()
        .zip(&identities[inputs.len()..])
        .peekable();
    inputs
        .iter()
        .zip(&identities[..inputs.len()])
        .map(|(path, identity)| match *identity {
            FileState::Missing => {
                let watcher = watchers
                    .next_if(|(dir, _)| path.ancestors().skip(1).any(|ancestor| ancestor == *dir))
                    .map(|(dir, state)| (dir.clone(), *state));
                WatchedInput::Absent {
                    path: path.clone(),
                    watcher,
                }
            }
            identity => WatchedInput::Present {
                path: path.clone(),
                identity,
            },
        })
        .collect()
}

/// Whether an answer kept against `kept` still stands at `checked_at_ns` under `state`:
/// - every present input must keep its recorded identity ([`FileState::same_as`]);
/// - an absent input whose watcher kept its identity is still absent;
/// - when a watcher moved (or none was recorded), the absent input is looked up by name: if it is
///   no longer absent the answer is invalid; if it is still absent, it stands and re-anchors to
///   its current nearest existing directory when that directory is [`settled`] at `checked_at_ns`.
fn stands(
    kept: &[WatchedInput],
    checked_at_ns: Option<i128>,
    mut state: impl FnMut(&std::path::Path) -> FileState,
) -> Standing {
    let mut updated: Option<Vec<WatchedInput>> = None;
    for (index, entry) in kept.iter().enumerate() {
        match entry {
            WatchedInput::Present { path, identity } => {
                if !identity.same_as(state(path)) {
                    return Standing::Invalid;
                }
            }
            WatchedInput::Absent { path, watcher } => {
                let moved_watcher = match watcher {
                    Some((dir, dir_identity)) => {
                        let now_dir = state(dir);
                        if dir_identity.same_as(now_dir) {
                            continue;
                        }
                        Some((dir.as_path(), now_dir))
                    }
                    None => None,
                };
                if !matches!(state(path), FileState::Missing) {
                    return Standing::Invalid;
                }
                let new_watcher = path.ancestors().skip(1).find_map(|directory| {
                    let found = match moved_watcher {
                        Some((checked_dir, checked_state)) if directory == checked_dir => {
                            checked_state
                        }
                        _ => state(directory),
                    };
                    (!matches!(found, FileState::Missing)).then(|| (directory.to_path_buf(), found))
                });
                if let Some((_, new_identity)) = &new_watcher
                    && checked_at_ns.is_some_and(|now| settled(*new_identity, now))
                {
                    let entries = updated.get_or_insert_with(|| kept.to_vec());
                    entries[index] = WatchedInput::Absent {
                        path: path.clone(),
                        watcher: new_watcher,
                    };
                }
            }
        }
    }
    match updated {
        Some(entries) => Standing::Reanchored(entries.into_boxed_slice()),
        None => Standing::Valid,
    }
}

/// Whether an answer whose watched paths have `identities`, read from `started_ns` by this
/// process's clock, is kept: every one is [`settled`] then.
pub(crate) fn keeps(identities: &[FileState], started_ns: i128) -> bool {
    identities
        .iter()
        .all(|identity| settled(*identity, started_ns))
}

/// Whether `identity` moves with the path's next change after `started_ns`: absent (a creation
/// moves it, and moves the directory [`watched`] adds), or last changed at least [`SETTLED`]
/// before it, so that change's stamp and any later one's cannot share a timestamp tick. An
/// unreadable path, or a change time ahead of the clock, is never settled.
fn settled(identity: FileState, started_ns: i128) -> bool {
    match identity {
        FileState::Missing => true,
        FileState::Unreadable(_) => false,
        FileState::Present { ctime_ns, .. } => {
            started_ns - ctime_ns >= i128::try_from(SETTLED.as_nanos()).unwrap_or(i128::MAX)
        }
    }
}

/// Wait until `path`'s last change is [`SETTLED`] behind the clock, so an answer that reads it is
/// kept: nothing a test may call moves a change time backwards, so the clock moves forward instead.
#[cfg(test)]
pub(crate) fn settle(path: &std::path::Path) {
    let FileState::Present { ctime_ns, .. } = FileState::of(path) else {
        panic!("{} exists", path.display());
    };
    while now_ns().is_none_or(|now| {
        !settled(
            FileState::Present {
                dev: 0,
                ino: 0,
                size: 0,
                mtime_ns: ctime_ns,
                ctime_ns,
            },
            now,
        )
    }) {
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::convert::Infallible;

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-input-memo-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn a_settled_input_is_read_once_and_a_change_reads_again() {
        let root = scratch("change");
        let input = root.join("index");
        std::fs::write(&input, b"one").unwrap();
        settle(&input);
        let memo = InputMemo::<&str, u32>::new();
        let reads = Cell::new(0);
        let read = || {
            reads.set(reads.get() + 1);
            Ok::<_, Infallible>(reads.get())
        };
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), read).unwrap(),
            1
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), read).unwrap(),
            1
        );
        std::fs::write(&input, b"two").unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), read).unwrap(),
            2
        );
        assert_eq!(reads.get(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_input_changed_within_the_settling_window_is_never_kept() {
        let root = scratch("fresh");
        let input = root.join("config.toml");
        std::fs::write(&input, b"fresh").unwrap();
        let memo = InputMemo::<&str, u32>::new();
        let reads = Cell::new(0);
        let read = || {
            reads.set(reads.get() + 1);
            Ok::<_, Infallible>(reads.get())
        };
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), read).unwrap(),
            1
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), read).unwrap(),
            2
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// An absent input is kept against its directory: creating and removing it between asks
    /// still leaves it absent so the answer stands, creating it invalidates, and a failed
    /// reading keeps nothing.
    #[test]
    fn an_absent_input_is_watched_through_its_directory() {
        let root = scratch("absent");
        let input = root.join("absent");
        settle(&root);
        let memo = InputMemo::<&str, u32>::new();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Err::<u32, &str>(
                "refused"
            )),
            Err("refused")
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(1)),
            Ok(1)
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(2)),
            Ok(1)
        );
        std::fs::write(&input, b"briefly").unwrap();
        std::fs::remove_file(&input).unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(3)),
            Ok(1)
        );
        std::fs::write(&input, b"now").unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(4)),
            Ok(4)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// An absent input whose directory is absent too is watched through the nearest directory
    /// that exists: creating and removing the directory and the input between asks still leaves
    /// the input absent so the answer stands.
    #[test]
    fn an_absent_input_under_an_absent_directory_is_watched_through_the_nearest_existing_one() {
        let root = scratch("absent-nested");
        let input = root.join("missing/absent");
        assert_eq!(
            observe(std::slice::from_ref(&input)).0,
            vec![input.clone(), root.clone()]
        );
        settle(&root);
        let memo = InputMemo::<&str, u32>::new();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(1)),
            Ok(1)
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(2)),
            Ok(1)
        );
        std::fs::create_dir(root.join("missing")).unwrap();
        std::fs::write(&input, b"briefly").unwrap();
        std::fs::remove_dir_all(root.join("missing")).unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(3)),
            Ok(1)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn observe_reads_each_input_and_watcher_once() {
        let present = |ino| FileState::Present {
            dev: 1,
            ino,
            size: 1,
            mtime_ns: 0,
            ctime_ns: 0,
        };
        let inputs = vec![
            PathBuf::from("/m/a"),
            PathBuf::from("/m/b"),
            PathBuf::from("/m/c"),
            PathBuf::from("/m/absent"),
        ];
        let lookups = Cell::new(0_usize);
        let (watched, identities) = observe_with(&inputs, |path| {
            lookups.set(lookups.get() + 1);
            match path.to_str() {
                Some("/m/a") => present(1),
                Some("/m/b") => present(2),
                Some("/m/c") => present(3),
                Some("/m/absent") => FileState::Missing,
                Some("/m") => present(100),
                other => panic!("unexpected path {other:?}"),
            }
        });
        assert_eq!(
            watched,
            vec![
                PathBuf::from("/m/a"),
                PathBuf::from("/m/b"),
                PathBuf::from("/m/c"),
                PathBuf::from("/m/absent"),
                PathBuf::from("/m"),
            ]
        );
        let expected = [
            present(1),
            present(2),
            present(3),
            FileState::Missing,
            present(100),
        ];
        assert_eq!(identities.len(), expected.len());
        assert!(
            identities
                .iter()
                .zip(expected)
                .all(|(got, want)| got.same_as(want))
        );
        // N = 3 present inputs + 1 absent input + 1 watcher directory = N + 2 lookups.
        assert_eq!(lookups.get(), inputs.len() + 1);
    }

    /// An unrelated sibling created and removed under an absent input's watcher directory (such
    /// as Git's `index.lock`) leaves the absent input absent, so the kept answer stands;
    /// creating the absent input itself invalidates it.
    #[test]
    fn an_unrelated_sibling_under_a_watcher_keeps_the_answer() {
        let root = scratch("sibling-churn");
        let absent = root.join("commondir");
        settle(&root);
        let memo = InputMemo::<&str, u32>::new();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&absent), || Ok::<_, &str>(1)),
            Ok(1)
        );
        let lock = root.join("index.lock");
        std::fs::write(&lock, b"").unwrap();
        std::fs::remove_file(&lock).unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&absent), || Ok::<_, &str>(2)),
            Ok(1)
        );
        std::fs::write(&absent, b".\n").unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&absent), || Ok::<_, &str>(3)),
            Ok(3)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Creating a directory on the way to an absent input leaves the input absent so the answer
    /// stands, and creating the leaf afterwards invalidates both before and after the new parent
    /// settles and becomes the re-anchored watcher.
    #[test]
    fn a_nested_absent_input_stands_when_its_parent_is_created_and_invalidates_on_leaf() {
        let root = scratch("nested-reanchor");
        settle(&root);
        let memo = InputMemo::<&str, u32>::new();

        let unsettled_dir = root.join("unsettled");
        let unsettled_leaf = unsettled_dir.join("leaf");
        let settled_dir = root.join("settled");
        let settled_leaf = settled_dir.join("leaf");
        assert_eq!(
            memo.read(&"unsettled", std::slice::from_ref(&unsettled_leaf), || {
                Ok::<_, &str>(1)
            }),
            Ok(1)
        );
        assert_eq!(
            memo.read(&"settled", std::slice::from_ref(&settled_leaf), || {
                Ok::<_, &str>(1)
            }),
            Ok(1)
        );

        // Unsettled intermediate directory: answer stands without publishing the unsettled anchor,
        // and creating the leaf immediately afterwards still invalidates.
        std::fs::create_dir(&unsettled_dir).unwrap();
        assert_eq!(
            memo.read(&"unsettled", std::slice::from_ref(&unsettled_leaf), || {
                Ok::<_, &str>(2)
            }),
            Ok(1)
        );
        assert!(matches!(
            memo.kept.load().get(&"unsettled").unwrap().watched.as_ref(),
            [WatchedInput::Absent { watcher: Some((dir, _)), .. }] if dir == &root
        ));
        std::fs::write(&unsettled_leaf, b"now").unwrap();
        assert_eq!(
            memo.read(&"unsettled", std::slice::from_ref(&unsettled_leaf), || {
                Ok::<_, &str>(3)
            }),
            Ok(3)
        );

        // Settled intermediate directory: ask re-anchors to `settled_dir`, and creating `leaf`
        // moves `settled_dir` (not `root`) and invalidates.
        std::fs::create_dir(&settled_dir).unwrap();
        settle(&settled_dir);
        assert_eq!(
            memo.read(&"settled", std::slice::from_ref(&settled_leaf), || {
                Ok::<_, &str>(2)
            }),
            Ok(1)
        );
        assert!(matches!(
            memo.kept.load().get(&"settled").unwrap().watched.as_ref(),
            [WatchedInput::Absent { watcher: Some((dir, _)), .. }] if dir == &settled_dir
        ));
        std::fs::write(&settled_leaf, b"now").unwrap();
        assert_eq!(
            memo.read(&"settled", std::slice::from_ref(&settled_leaf), || {
                Ok::<_, &str>(3)
            }),
            Ok(3)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Creating and removing an absent input while `read` runs moves its watching directory
    /// inside the settling window, so the post-read `keep` refuses to keep that answer.
    #[test]
    fn a_creation_and_removal_during_read_is_never_kept() {
        let root = scratch("during-read");
        let absent = root.join("absent");
        settle(&root);
        let memo = InputMemo::<&str, u32>::new();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&absent), || {
                std::fs::write(&absent, b"during").unwrap();
                std::fs::remove_file(&absent).unwrap();
                Ok::<_, &str>(1)
            }),
            Ok(1)
        );
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&absent), || Ok::<_, &str>(2)),
            Ok(2)
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    /// One step of a model checkout whose files stamp their changes at a coarse clock, a
    /// Linux tick or an HFS+ second, behind this process's clock by up to one tick.
    #[derive(Clone, Debug)]
    enum Step {
        /// Write `content` into `file`, creating it (and `/m/sub` for the nested leaf) when
        /// absent: in place (the inode stays) or by a rename over it.
        Write {
            file: usize,
            content: u8,
            renamed: bool,
        },
        /// Remove `file` when present.
        Remove { file: usize },
        /// Create `file` and remove it again between asks when absent.
        Flicker { file: usize, content: u8 },
        /// Create `/m/sub` when absent.
        CreateDir,
        /// Remove `/m/sub` (and its leaf if present) when present.
        RemoveDir,
        /// Create and remove an unrelated sibling inside `/m` (or `/m/sub` when `in_sub` and
        /// present), moving only the directory's stamp.
        SiblingChurn { in_sub: bool },
        /// Let `ns` pass.
        Advance { ns: i128 },
        /// Ask, taking `reading_ns` to read, while `during` changes halfway through: the reading
        /// sees the checkout after that change when `seen_after`, else before it. Both
        /// [`InputMemo::read`] and [`InputMemo::read_recorded`] observe post-read identities
        /// against `started`.
        Ask {
            during: Option<Change>,
            reading_ns: i128,
            recorded: bool,
            seen_after: bool,
        },
    }

    #[derive(Clone, Copy, Debug)]
    enum Change {
        Write {
            file: usize,
            content: u8,
            renamed: bool,
        },
        Remove {
            file: usize,
        },
        /// Create `file` and remove it again inside one reading.
        Flicker {
            file: usize,
            content: u8,
        },
        CreateDir,
        RemoveDir,
        SiblingChurn {
            in_sub: bool,
        },
    }

    /// A model directory `/m` of flat files `/m/0..` plus an optional nested directory `/m/sub`
    /// and leaf `/m/sub/leaf`. Each file is absent or has an inode, a one-byte content (one size,
    /// so only the clock tells two writes apart) and a change stamp; each directory's stamp moves
    /// with creations, removals and sibling churn directly inside it.
    struct Checkout {
        now: i128,
        tick: i128,
        lag: i128,
        next_ino: u64,
        directory: (u64, i128),
        sub: Option<(u64, i128)>,
        files: Vec<Option<(u64, u8, i128)>>,
        leaf: Option<(u64, u8, i128)>,
    }

    impl Checkout {
        fn stamp(&self) -> i128 {
            let behind = self.now - self.lag;
            behind - behind.rem_euclid(self.tick)
        }

        fn total_files(&self) -> usize {
            self.files.len() + 1
        }

        fn create_dir(&mut self) {
            if self.sub.is_none() {
                let stamp = self.stamp();
                self.next_ino += 1;
                self.sub = Some((self.next_ino, stamp));
                self.directory.1 = stamp;
            }
        }

        fn remove_dir(&mut self) {
            if self.sub.take().is_some() {
                self.leaf = None;
                self.directory.1 = self.stamp();
            }
        }

        fn sibling_churn(&mut self, in_sub: bool) {
            let stamp = self.stamp();
            if in_sub && let Some((_, sub_stamp)) = &mut self.sub {
                *sub_stamp = stamp;
            } else {
                self.directory.1 = stamp;
            }
        }

        fn write(&mut self, file: usize, content: u8, renamed: bool) {
            let index = file % self.total_files();
            if index == self.files.len() {
                self.create_dir();
                let stamp = self.stamp();
                let created = self.leaf.is_none();
                let ino = match self.leaf {
                    Some((ino, ..)) if !renamed => ino,
                    _ => {
                        self.next_ino += 1;
                        self.next_ino
                    }
                };
                self.leaf = Some((ino, content, stamp));
                if (created || renamed)
                    && let Some((_, sub_stamp)) = &mut self.sub
                {
                    *sub_stamp = stamp;
                }
                return;
            }
            let stamp = self.stamp();
            let created = self.files[index].is_none();
            let ino = match self.files[index] {
                Some((ino, ..)) if !renamed => ino,
                _ => {
                    self.next_ino += 1;
                    self.next_ino
                }
            };
            self.files[index] = Some((ino, content, stamp));
            if created || renamed {
                self.directory.1 = stamp;
            }
        }

        fn remove(&mut self, file: usize) {
            let index = file % self.total_files();
            if index == self.files.len() {
                if self.leaf.take().is_some() {
                    let stamp = self.stamp();
                    if let Some((_, sub_stamp)) = &mut self.sub {
                        *sub_stamp = stamp;
                    }
                }
                return;
            }
            if self.files[index].take().is_some() {
                self.directory.1 = self.stamp();
            }
        }

        fn flicker(&mut self, file: usize, content: u8) -> Vec<Option<u8>> {
            let index = file % self.total_files();
            if index == self.files.len() {
                if self.leaf.is_none() {
                    let had_sub = self.sub.is_some();
                    self.write(index, content, false);
                    let seen = self.contents();
                    self.remove(index);
                    if !had_sub {
                        self.remove_dir();
                    }
                    return seen;
                }
                return self.contents();
            }
            if self.files[index].is_none() {
                self.write(index, content, false);
                let seen = self.contents();
                self.remove(index);
                return seen;
            }
            self.contents()
        }

        fn apply_change(&mut self, change: Change) -> Vec<Option<u8>> {
            match change {
                Change::Write {
                    file,
                    content,
                    renamed,
                } => {
                    self.write(file, content, renamed);
                    self.contents()
                }
                Change::Remove { file } => {
                    self.remove(file);
                    self.contents()
                }
                Change::Flicker { file, content } => self.flicker(file, content),
                Change::CreateDir => {
                    self.create_dir();
                    self.contents()
                }
                Change::RemoveDir => {
                    self.remove_dir();
                    self.contents()
                }
                Change::SiblingChurn { in_sub } => {
                    self.sibling_churn(in_sub);
                    self.contents()
                }
            }
        }

        fn state(&self, path: &std::path::Path) -> FileState {
            let present = |ino, stamp| FileState::Present {
                dev: 1,
                ino,
                size: 1,
                mtime_ns: stamp,
                ctime_ns: stamp,
            };
            if path == std::path::Path::new("/m") {
                return present(self.directory.0, self.directory.1);
            }
            if path == std::path::Path::new("/m/sub") {
                return match self.sub {
                    Some((ino, stamp)) => present(ino, stamp),
                    None => FileState::Missing,
                };
            }
            if path == std::path::Path::new("/m/sub/leaf") {
                return match self.leaf {
                    Some((ino, _, stamp)) => present(ino, stamp),
                    None => FileState::Missing,
                };
            }
            let file: usize = path.file_name().unwrap().to_str().unwrap().parse().unwrap();
            match self.files[file] {
                Some((ino, _, stamp)) => present(ino, stamp),
                None => FileState::Missing,
            }
        }

        fn observe(&self, inputs: &[PathBuf]) -> (Vec<PathBuf>, Vec<FileState>) {
            observe_with(inputs, |path| self.state(path))
        }

        fn contents(&self) -> Vec<Option<u8>> {
            self.files
                .iter()
                .map(|file| file.map(|(_, content, _)| content))
                .chain(std::iter::once(self.leaf.map(|(_, content, _)| content)))
                .collect()
        }
    }

    fn change(files: usize) -> impl proptest::strategy::Strategy<Value = Change> {
        use proptest::prelude::*;
        prop_oneof![
            (0..files, 0_u8..3, any::<bool>()).prop_map(|(file, content, renamed)| Change::Write {
                file,
                content,
                renamed
            }),
            (0..files).prop_map(|file| Change::Remove { file }),
            (0..files, 0_u8..3).prop_map(|(file, content)| Change::Flicker { file, content }),
            Just(Change::CreateDir),
            Just(Change::RemoveDir),
            any::<bool>().prop_map(|in_sub| Change::SiblingChurn { in_sub }),
        ]
    }

    fn step(files: usize) -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        prop_oneof![
            change(files).prop_map(|change| match change {
                Change::Write {
                    file,
                    content,
                    renamed,
                } => Step::Write {
                    file,
                    content,
                    renamed
                },
                Change::Remove { file } => Step::Remove { file },
                Change::Flicker { file, content } => Step::Flicker { file, content },
                Change::CreateDir => Step::CreateDir,
                Change::RemoveDir => Step::RemoveDir,
                Change::SiblingChurn { in_sub } => Step::SiblingChurn { in_sub },
            }),
            prop_oneof![0_i128..2_000_000, 0_i128..5_000_000_000]
                .prop_map(|ns| Step::Advance { ns }),
            (
                proptest::option::of(change(files)),
                prop_oneof![0_i128..3_000_000_000, 0_i128..10_000_000_000],
                any::<bool>(),
                any::<bool>()
            )
                .prop_map(|(during, reading_ns, recorded, seen_after)| Step::Ask {
                    during,
                    reading_ns,
                    recorded,
                    seen_after
                }),
        ]
    }

    proptest::proptest! {
        /// A kept answer is served only while every input still holds what it held when that
        /// answer was read, whatever the stamping tick, the lag of stamps behind the clock, and
        /// the writes, creations, removals, nested directory changes and sibling churn that land
        /// in place, by rename, between asks or while one reads; for both `read` and
        /// `read_recorded`.
        #[test]
        fn a_kept_answer_is_never_stale(
            tick in proptest::prop_oneof![
                proptest::strategy::Just(1_i128),
                proptest::strategy::Just(4_000_000_i128),
                proptest::strategy::Just(1_000_000_000_i128),
            ],
            lag_fraction in 0.0_f64..1.0,
            ages in proptest::collection::vec(proptest::option::of(0_i128..10_000_000_000), 1..4),
            steps in proptest::collection::vec(step(4), 1..60),
        ) {
            let now = 1_800_000_000_000_000_000_i128;
            let files = ages.len();
            let mut checkout = Checkout {
                now,
                tick,
                lag: (tick as f64 * lag_fraction) as i128,
                next_ino: files as u64 + 1,
                directory: (1_000, now - 20_000_000_000),
                sub: None,
                files: ages
                    .iter()
                    .enumerate()
                    .map(|(file, age)| age.map(|age| (file as u64 + 1, 0, now - age)))
                    .collect(),
                leaf: None,
            };
            let mut inputs: Vec<PathBuf> =
                (0..files).map(|file| PathBuf::from(format!("/m/{file}"))).collect();
            inputs.push(PathBuf::from("/m/sub/leaf"));
            // The watched inputs and answer as the memo last kept (or re-anchored) them.
            type ModelKept = (Box<[WatchedInput]>, Vec<Option<u8>>);
            let mut kept: Option<ModelKept> = None;
            for step in steps {
                match step {
                    Step::Write { file, content, renamed } => {
                        checkout.write(file, content, renamed);
                    }
                    Step::Remove { file } => checkout.remove(file),
                    Step::Flicker { file, content } => {
                        let _ = checkout.flicker(file, content);
                    }
                    Step::CreateDir => checkout.create_dir(),
                    Step::RemoveDir => checkout.remove_dir(),
                    Step::SiblingChurn { in_sub } => checkout.sibling_churn(in_sub),
                    Step::Advance { ns } => checkout.now += ns,
                    Step::Ask { during, reading_ns, recorded, seen_after } => {
                        if let Some((watched_then, value)) = &mut kept {
                            let checked_at = checkout.now;
                            match stands(watched_then, Some(checked_at), |path| checkout.state(path)) {
                                Standing::Valid => {
                                    proptest::prop_assert_eq!(&*value, &checkout.contents());
                                    continue;
                                }
                                Standing::Reanchored(reanchored) => {
                                    *watched_then = reanchored;
                                    proptest::prop_assert_eq!(&*value, &checkout.contents());
                                    continue;
                                }
                                Standing::Invalid => {}
                            }
                        }
                        let started = checkout.now;
                        checkout.now += reading_ns / 2;
                        // The reading sees the checkout halfway through, before or after the
                        // change that lands then, a flicker's short-lived file included.
                        let unchanged = checkout.contents();
                        let changed = match during {
                            Some(change) => checkout.apply_change(change),
                            None => checkout.contents(),
                        };
                        let value = if seen_after { changed } else { unchanged };
                        checkout.now += reading_ns - reading_ns / 2;
                        // Both `read` (recorded == false) and `read_recorded` (recorded == true)
                        // observe post-read identities against `started`.
                        let (watched, identities) = match recorded {
                            true | false => checkout.observe(inputs.as_slice()),
                        };
                        if keeps(&identities, started) {
                            kept = Some((watched_inputs(&inputs, &watched, &identities), value));
                        }
                    }
                }
            }
        }
    }
}
