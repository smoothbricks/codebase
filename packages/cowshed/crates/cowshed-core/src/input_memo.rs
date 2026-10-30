//! An answer read from a few files, kept while those files keep their identity.
//!
//! Some readings cost a process: `git ls-files` for the tracked build inputs, `mbx settings get`
//! for the host scheduler. Admission asks them again for every exec (15_capabilities.md,
//! "Detection inputs"), and their answer moves only when a file the reading depends on moves. An
//! [`InputMemo`] keeps the last answer beside the identity ([`FileState`]: inode, size, mtime,
//! ctime) each of those files had, and answers from it while every one of them still has that
//! identity. An absent input is watched through the directory that would hold it, whose entries
//! change when it is created, even when it is removed again before the next ask. A file whose last
//! change is not yet [`SETTLED`] behind the clock when the reading starts could be rewritten
//! again within its timestamp's tick without moving its identity (a coarse Linux tick, an HFS+
//! second), so an answer that read it is used once and not kept. Bytes are never compared: the
//! reading is the authority, and a moved identity only means it runs again (11_shell.md,
//! "Kept readings").
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
    /// What decides it ([`watched`]) and the identity each had.
    watched: Box<[PathBuf]>,
    identities: Box<[FileState]>,
    value: V,
}

impl<K: Clone + Eq + Hash, V: Clone> InputMemo<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            kept: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// `read`'s answer for `key`, which depends on the files `inputs` and on nothing else that
    /// can change while this process runs: the kept one while every input keeps its identity,
    /// else a fresh reading. A failed reading is returned and keeps nothing.
    pub(crate) fn read<E>(
        &self,
        key: &K,
        inputs: &[PathBuf],
        read: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        let asked = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(asked) => asked,
        };
        let value = read()?;
        self.keep(key, inputs, asked, &value);
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
        let asked = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(asked) => asked,
        };
        let value = read().await?;
        self.keep(key, inputs, asked, &value);
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
        if let Some(kept) = self.kept.load().get(key) {
            let (watched, identities) = observe(&kept.inputs);
            if stands(&kept.watched, &kept.identities, &watched, &identities) {
                return Ok(kept.value.clone());
            }
        }
        let started = now_ns();
        let (value, inputs) = read()?;
        let (watched, identities) = observe(&inputs);
        self.publish(key, inputs.into(), watched, identities, started, &value);
        Ok(value)
    }

    /// The kept answer for `key` when it stands for `inputs` now; else what an answer read from
    /// them now is kept against, taken before the reading: a change made while it runs stamps no
    /// earlier than then, less a stamping tick.
    fn ask(&self, key: &K, inputs: &[PathBuf]) -> Result<V, Asked> {
        let (watched, identities) = observe(inputs);
        let started = now_ns();
        match self.kept.load().get(key) {
            Some(kept)
                if *kept.inputs == *inputs
                    && stands(&kept.watched, &kept.identities, &watched, &identities) =>
            {
                Ok(kept.value.clone())
            }
            _ => Err(Asked {
                watched,
                identities,
                started,
            }),
        }
    }

    fn keep(&self, key: &K, inputs: &[PathBuf], asked: Asked, value: &V) {
        self.publish(
            key,
            inputs.into(),
            asked.watched,
            asked.identities,
            asked.started,
            value,
        );
    }

    /// Publish `value` for `key` when every watched identity was settled at `started`.
    fn publish(
        &self,
        key: &K,
        inputs: Box<[PathBuf]>,
        watched: Vec<PathBuf>,
        identities: Vec<FileState>,
        started: Option<i128>,
        value: &V,
    ) {
        if !started.is_some_and(|started| keeps(&identities, started)) {
            return;
        }
        let kept = Arc::new(Kept {
            inputs,
            watched: watched.into(),
            identities: identities.into(),
            value: value.clone(),
        });
        self.kept.rcu(|answers| {
            let mut answers = HashMap::clone(answers);
            answers.insert(key.clone(), Arc::clone(&kept));
            answers
        });
    }
}

/// What an ask that found no standing answer keeps a fresh one against.
struct Asked {
    watched: Vec<PathBuf>,
    identities: Vec<FileState>,
    started: Option<i128>,
}

/// The paths that decide an answer read from `inputs`, and the identity of each now.
fn observe(inputs: &[PathBuf]) -> (Vec<PathBuf>, Vec<FileState>) {
    let states: Vec<FileState> = inputs.iter().map(|path| FileState::of(path)).collect();
    let watched = watched(inputs, &states);
    let identities = watched.iter().map(|path| FileState::of(path)).collect();
    (watched, identities)
}

/// The paths whose identities decide an answer read from `inputs`, which had `identities`: every
/// input, and the directory of each absent one, whose entries change when it is created even if
/// it is removed again before the next ask.
fn watched(inputs: &[PathBuf], identities: &[FileState]) -> Vec<PathBuf> {
    let absent = inputs
        .iter()
        .zip(identities)
        .filter(|(_, identity)| matches!(identity, FileState::Missing))
        .filter_map(|(path, _)| path.parent().map(PathBuf::from));
    inputs.iter().cloned().chain(absent).collect()
}

/// This process's clock, in nanoseconds since the epoch; `None` before it.
pub(crate) fn now_ns() -> Option<i128> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|now| i128::try_from(now.as_nanos()).ok())
}

/// Whether an answer kept against `kept_watched` with `kept_identities` answers an ask that
/// watches `watched` with `identities` now: the same paths, each the same file state
/// ([`FileState::same_as`]).
fn stands(
    kept_watched: &[PathBuf],
    kept_identities: &[FileState],
    watched: &[PathBuf],
    identities: &[FileState],
) -> bool {
    kept_watched == watched
        && kept_identities.len() == identities.len()
        && kept_identities
            .iter()
            .zip(identities)
            .all(|(kept, now)| kept.same_as(*now))
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

    /// An absent input is kept against its directory: a file created and removed again moves
    /// that directory, a creation moves the input itself, and a failed reading keeps nothing.
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
            Ok(3)
        );
        std::fs::write(&input, b"now").unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(4)),
            Ok(4)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// One step of a model checkout whose files stamp their changes at a coarse clock, a
    /// Linux tick or an HFS+ second, behind this process's clock by up to one tick.
    #[derive(Clone, Debug)]
    enum Step {
        /// Write `content` into `file`, creating it when absent: in place (the inode stays) or by
        /// a rename over it.
        Write {
            file: usize,
            content: u8,
            renamed: bool,
        },
        /// Remove `file` when present.
        Remove { file: usize },
        /// Let `ns` pass.
        Advance { ns: i128 },
        /// Ask, taking `reading_ns` to read, while `during` changes halfway through. A recorded
        /// ask takes its identities after the reading ([`InputMemo::read_recorded`]), any other
        /// before it ([`InputMemo::read`]).
        Ask {
            during: Option<Change>,
            reading_ns: i128,
            recorded: bool,
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
    }

    /// A model directory of files, each absent or with an inode, a one-byte content (one size, so
    /// only the clock tells two writes apart) and a change stamp; the directory's own inode and
    /// stamp move with every creation and removal.
    struct Checkout {
        now: i128,
        tick: i128,
        lag: i128,
        next_ino: u64,
        directory: (u64, i128),
        files: Vec<Option<(u64, u8, i128)>>,
    }

    impl Checkout {
        fn stamp(&self) -> i128 {
            let behind = self.now - self.lag;
            behind - behind.rem_euclid(self.tick)
        }

        fn write(&mut self, file: usize, content: u8, renamed: bool) {
            let stamp = self.stamp();
            let created = self.files[file].is_none();
            let ino = match self.files[file] {
                Some((ino, ..)) if !renamed => ino,
                _ => {
                    self.next_ino += 1;
                    self.next_ino
                }
            };
            self.files[file] = Some((ino, content, stamp));
            if created || renamed {
                self.directory.1 = stamp;
            }
        }

        fn remove(&mut self, file: usize) {
            if self.files[file].take().is_some() {
                self.directory.1 = self.stamp();
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
            let file: usize = path.file_name().unwrap().to_str().unwrap().parse().unwrap();
            match self.files[file] {
                Some((ino, _, stamp)) => present(ino, stamp),
                None => FileState::Missing,
            }
        }

        fn observe(&self, inputs: &[PathBuf]) -> (Vec<PathBuf>, Vec<FileState>) {
            let states: Vec<FileState> = inputs.iter().map(|path| self.state(path)).collect();
            let watched = watched(inputs, &states);
            let identities = watched.iter().map(|path| self.state(path)).collect();
            (watched, identities)
        }

        fn contents(&self) -> Vec<Option<u8>> {
            self.files
                .iter()
                .map(|file| file.map(|(_, content, _)| content))
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
                Change::Remove { file } | Change::Flicker { file, .. } => Step::Remove { file },
            }),
            prop_oneof![0_i128..2_000_000, 0_i128..5_000_000_000]
                .prop_map(|ns| Step::Advance { ns }),
            (
                proptest::option::of(change(files)),
                0_i128..3_000_000_000,
                any::<bool>()
            )
                .prop_map(|(during, reading_ns, recorded)| Step::Ask {
                    during,
                    reading_ns,
                    recorded
                }),
        ]
    }

    proptest::proptest! {
        /// A kept answer is served only while every input still holds what it held when that
        /// answer was read, whatever the stamping tick, the lag of stamps behind the clock, and
        /// the writes, creations and removals that land in place, by rename, between asks or
        /// while one reads; whether the reading's identities are taken before it or after it.
        #[test]
        fn a_kept_answer_is_never_stale(
            tick in proptest::prop_oneof![
                proptest::strategy::Just(1_i128),
                proptest::strategy::Just(4_000_000_i128),
                proptest::strategy::Just(1_000_000_000_i128),
            ],
            lag_fraction in 0.0_f64..1.0,
            ages in proptest::collection::vec(proptest::option::of(0_i128..10_000_000_000), 1..4),
            steps in proptest::collection::vec(step(3), 1..60),
        ) {
            let now = 1_800_000_000_000_000_000_i128;
            let files = ages.len();
            let mut checkout = Checkout {
                now,
                tick,
                lag: (tick as f64 * lag_fraction) as i128,
                next_ino: files as u64 + 1,
                directory: (1_000, now - 20_000_000_000),
                files: ages
                    .iter()
                    .enumerate()
                    .map(|(file, age)| age.map(|age| (file as u64 + 1, 0, now - age)))
                    .collect(),
            };
            let inputs: Vec<PathBuf> =
                (0..files).map(|file| PathBuf::from(format!("/m/{file}"))).collect();
            let mut kept: Option<(Vec<PathBuf>, Vec<FileState>, Vec<Option<u8>>)> = None;
            for step in steps {
                match step {
                    Step::Write { file, content, renamed } => {
                        checkout.write(file % files, content, renamed);
                    }
                    Step::Remove { file } => checkout.remove(file % files),
                    Step::Advance { ns } => checkout.now += ns,
                    Step::Ask { during, reading_ns, recorded } => {
                        if let Some((watched_then, identities_then, value)) = &kept {
                            let (watched, identities) = checkout.observe(inputs.as_slice());
                            if stands(watched_then, identities_then, &watched, &identities) {
                                proptest::prop_assert_eq!(value, &checkout.contents());
                                continue;
                            }
                        }
                        let before = checkout.observe(inputs.as_slice());
                        let started = checkout.now;
                        checkout.now += reading_ns / 2;
                        // The reading sees the checkout halfway through, a flicker's short-lived
                        // file included.
                        let value = match during {
                            Some(Change::Write { file, content, renamed }) => {
                                checkout.write(file % files, content, renamed);
                                checkout.contents()
                            }
                            Some(Change::Remove { file }) => {
                                checkout.remove(file % files);
                                checkout.contents()
                            }
                            Some(Change::Flicker { file, content })
                                if checkout.files[file % files].is_none() =>
                            {
                                checkout.write(file % files, content, false);
                                let seen = checkout.contents();
                                checkout.remove(file % files);
                                seen
                            }
                            Some(Change::Flicker { .. }) | None => checkout.contents(),
                        };
                        checkout.now += reading_ns - reading_ns / 2;
                        let (watched, identities) = if recorded {
                            checkout.observe(inputs.as_slice())
                        } else {
                            before
                        };
                        if keeps(&identities, started) {
                            kept = Some((watched, identities, value));
                        }
                    }
                }
            }
        }
    }
}
