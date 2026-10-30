//! An answer read from a few files, kept while those files keep their identity.
//!
//! Some readings cost a process: `git ls-files` for the tracked build inputs, `mbx settings get`
//! for the host scheduler. Admission asks them again for every exec (15_capabilities.md,
//! "Detection inputs"), and their answer moves only when a file the reading depends on moves. An
//! [`InputMemo`] keeps the last answer beside the identity ([`FileState`]: inode, size, mtime,
//! ctime) each of those files had before that reading started, and answers from it while every
//! one of them still has that identity. Identities are taken before the reading, so a change made
//! while it runs moves one and the next ask reads again. A file whose last change is not yet
//! [`SETTLED`] behind the clock could be rewritten again within its timestamp's tick without
//! moving its identity (a coarse Linux tick, an HFS+ second), so an answer that read it is used
//! once and not kept. Bytes are never compared: the reading is the authority, and a moved identity
//! only means it runs again (11_shell.md, "Freshness is direnv's own").
//!
//! The decisions are pure ([`stands`], [`keeps`]): an ask's identities and the time it read them
//! are their inputs, so a model of coarse filesystem clocks checks them (`tests`); [`InputMemo`]
//! only reads identities and the clock, runs the reading, and publishes. An ask loads one
//! published map; keeping an answer publishes a new one. Nothing locks.

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
    inputs: Box<[PathBuf]>,
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
        let (identities, asked) = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(asked) => asked,
        };
        let value = read()?;
        self.keep(key, inputs.into(), identities, asked, &value);
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
        let (identities, asked) = match self.ask(key, inputs) {
            Ok(kept) => return Ok(kept),
            Err(asked) => asked,
        };
        let value = read().await?;
        self.keep(key, inputs.into(), identities, asked, &value);
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
            let identities: Vec<FileState> =
                kept.inputs.iter().map(|path| FileState::of(path)).collect();
            if stands(&kept.inputs, &kept.identities, &kept.inputs, &identities) {
                return Ok(kept.value.clone());
            }
        }
        let asked = now_ns();
        let (value, inputs) = read()?;
        let identities = inputs.iter().map(|path| FileState::of(path)).collect();
        self.keep(key, inputs.into(), identities, asked, &value);
        Ok(value)
    }

    /// The kept answer for `key` when it stands for `inputs` now; else their identities and the
    /// time they were read, taken before the reading: a change made while it runs stamps no
    /// earlier than that, less a stamping tick.
    fn ask(&self, key: &K, inputs: &[PathBuf]) -> Result<V, (Box<[FileState]>, Option<i128>)> {
        let identities: Box<[FileState]> = inputs.iter().map(|path| FileState::of(path)).collect();
        let asked = now_ns();
        match self.kept.load().get(key) {
            Some(kept) if stands(&kept.inputs, &kept.identities, inputs, &identities) => {
                Ok(kept.value.clone())
            }
            _ => Err((identities, asked)),
        }
    }

    /// Publish `value` for `key` when every input's identity was settled at `asked`.
    fn keep(
        &self,
        key: &K,
        inputs: Box<[PathBuf]>,
        identities: Box<[FileState]>,
        asked: Option<i128>,
        value: &V,
    ) {
        if !asked.is_some_and(|asked| keeps(&identities, asked)) {
            return;
        }
        let kept = Arc::new(Kept {
            inputs,
            identities,
            value: value.clone(),
        });
        self.kept.rcu(|answers| {
            let mut answers = HashMap::clone(answers);
            answers.insert(key.clone(), Arc::clone(&kept));
            answers
        });
    }
}

/// This process's clock, in nanoseconds since the epoch; `None` before it.
pub(crate) fn now_ns() -> Option<i128> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|now| i128::try_from(now.as_nanos()).ok())
}

/// Whether an answer kept for `kept_inputs`, whose identities were `kept_identities` before it
/// was read, answers an ask of `inputs` whose identities are now `identities`: the same files,
/// each the same file state ([`FileState::same_as`]).
fn stands(
    kept_inputs: &[PathBuf],
    kept_identities: &[FileState],
    inputs: &[PathBuf],
    identities: &[FileState],
) -> bool {
    kept_inputs == inputs
        && kept_identities.len() == identities.len()
        && kept_identities
            .iter()
            .zip(identities)
            .all(|(kept, now)| kept.same_as(*now))
}

/// Whether an answer read after its inputs had `identities`, read at `asked_ns` by this process's
/// clock, is kept: every input is [`settled`] then.
pub(crate) fn keeps(identities: &[FileState], asked_ns: i128) -> bool {
    identities
        .iter()
        .all(|identity| settled(*identity, asked_ns))
}

/// Whether `identity` moves with the file's next change after `asked_ns`: absent (a creation
/// moves it), or last changed at least [`SETTLED`] before it, so that change's stamp and any
/// later one's cannot share a timestamp tick. An unreadable file, or a change time ahead of the
/// clock, is never settled.
fn settled(identity: FileState, asked_ns: i128) -> bool {
    match identity {
        FileState::Missing => true,
        FileState::Unreadable(_) => false,
        FileState::Present { ctime_ns, .. } => {
            asked_ns - ctime_ns >= i128::try_from(SETTLED.as_nanos()).unwrap_or(i128::MAX)
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

    #[test]
    fn a_created_input_reads_again_and_a_failure_keeps_nothing() {
        let root = scratch("created");
        let input = root.join("absent");
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
        std::fs::write(&input, b"now").unwrap();
        assert_eq!(
            memo.read(&"k", std::slice::from_ref(&input), || Ok::<_, &str>(3)),
            Ok(3)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// One step of a model checkout whose files stamp their changes at a coarse clock, a
    /// Linux tick or an HFS+ second, behind this process's clock by up to one tick.
    #[derive(Clone, Debug)]
    enum Step {
        /// Write `content` into `file`: in place (the inode stays) or by a rename over it.
        Write {
            file: usize,
            content: u8,
            renamed: bool,
        },
        /// Let `ns` pass.
        Advance { ns: i128 },
        /// Ask, taking `reading_ns` to read, while `during` is written halfway through.
        Ask {
            during: Option<(usize, u8, bool)>,
            reading_ns: i128,
        },
    }

    struct Checkout {
        now: i128,
        tick: i128,
        lag: i128,
        next_ino: u64,
        /// Each file's inode, one-byte content (one size, so only the clock can tell two writes
        /// apart) and change stamp.
        files: Vec<(u64, u8, i128)>,
    }

    impl Checkout {
        fn stamp(&self) -> i128 {
            let behind = self.now - self.lag;
            behind - behind.rem_euclid(self.tick)
        }

        fn write(&mut self, file: usize, content: u8, renamed: bool) {
            let stamp = self.stamp();
            let file = &mut self.files[file];
            if renamed {
                file.0 = self.next_ino;
                self.next_ino += 1;
            }
            file.1 = content;
            file.2 = stamp;
        }

        fn identities(&self) -> Vec<FileState> {
            self.files
                .iter()
                .map(|&(ino, _, stamp)| FileState::Present {
                    dev: 1,
                    ino,
                    size: 1,
                    mtime_ns: stamp,
                    ctime_ns: stamp,
                })
                .collect()
        }

        fn contents(&self) -> Vec<u8> {
            self.files.iter().map(|&(_, content, _)| content).collect()
        }
    }

    fn step(files: usize) -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        let write = (0..files, 0_u8..3, any::<bool>());
        prop_oneof![
            write
                .clone()
                .prop_map(|(file, content, renamed)| Step::Write {
                    file,
                    content,
                    renamed
                }),
            prop_oneof![0_i128..2_000_000, 0_i128..5_000_000_000]
                .prop_map(|ns| Step::Advance { ns }),
            (proptest::option::of(write), 0_i128..3_000_000_000)
                .prop_map(|(during, reading_ns)| Step::Ask { during, reading_ns }),
        ]
    }

    proptest::proptest! {
        /// A kept answer is served only while every input still holds what it held when that
        /// answer was read, whatever the stamping tick, the lag of stamps behind the clock, and
        /// the writes that land in place, by rename, between asks or while one reads.
        #[test]
        fn a_kept_answer_is_never_stale(
            tick in proptest::prop_oneof![
                proptest::strategy::Just(1_i128),
                proptest::strategy::Just(4_000_000_i128),
                proptest::strategy::Just(1_000_000_000_i128),
            ],
            lag_fraction in 0.0_f64..1.0,
            ages in proptest::collection::vec(0_i128..10_000_000_000, 1..4),
            steps in proptest::collection::vec(step(3), 1..60),
        ) {
            let now = 1_800_000_000_000_000_000_i128;
            let mut checkout = Checkout {
                now,
                tick,
                lag: (tick as f64 * lag_fraction) as i128,
                next_ino: ages.len() as u64 + 1,
                files: ages
                    .iter()
                    .enumerate()
                    .map(|(file, age)| (file as u64 + 1, 0, now - age))
                    .collect(),
            };
            let inputs: Vec<PathBuf> =
                (0..ages.len()).map(|file| PathBuf::from(format!("/m/{file}"))).collect();
            let mut kept: Option<(Vec<FileState>, Vec<u8>)> = None;
            for step in steps {
                match step {
                    Step::Write { file, content, renamed } => {
                        checkout.write(file % ages.len(), content, renamed);
                    }
                    Step::Advance { ns } => checkout.now += ns,
                    Step::Ask { during, reading_ns } => {
                        let identities = checkout.identities();
                        let asked = checkout.now;
                        if let Some((identities_then, value)) = &kept
                            && stands(&inputs, identities_then, &inputs, &identities)
                        {
                            proptest::prop_assert_eq!(value, &checkout.contents());
                            continue;
                        }
                        checkout.now += reading_ns / 2;
                        if let Some((file, content, renamed)) = during {
                            checkout.write(file % ages.len(), content, renamed);
                        }
                        checkout.now += reading_ns - reading_ns / 2;
                        let value = checkout.contents();
                        if keeps(&identities, asked) {
                            kept = Some((identities, value));
                        }
                    }
                }
            }
        }
    }
}
