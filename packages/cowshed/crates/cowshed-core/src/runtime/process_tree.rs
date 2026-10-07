//! The retained process tree of one job, folded from the observer's fork, exec and exit events
//! (07_api.md, "Process-tree observations").
//!
//! The fold is pure: it reads no kernel state and keeps every life it was told of, the exited
//! ones included, in the order their births were observed. Each event names its process by
//! [`ProcessIdentity`] -- a pid and the birth the kernel gave that pid -- so the late exit of a
//! pid's first life never closes the second life that reused it.
//!
//! An event the fold cannot place, because the birth it depends on was never observed, is a
//! coverage gap: the tree says so instead of guessing. An event that contradicts what was already
//! observed -- a second birth of one identity, anything after an exit -- is an observer defect and
//! is refused.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::dto::{CommandArg, ExitStatus, JobId, UtcTimestamp};
use crate::api::process::{
    JobProcessSample, JobProcessTree, ProcessCoverage, ProcessCoverageGap, ProcessExit,
};

/// A value the kernel never gives two lives of the same pid while the observer holds the job:
/// the unique id of macOS's `proc_uniqidentifierinfo`, the pidfd inode or birth time on Linux.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BirthToken(pub u64);

/// One life of one process.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub birth: BirthToken,
}

/// The image a process runs after an exec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessImage {
    pub program: String,
    pub argv: Vec<CommandArg>,
}

/// One thing the observer saw, in the order the kernel reported it for that process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessObservation {
    /// A process the job owns whose parent is outside the job: its first process, or the command
    /// a shell host starts for it.
    Root {
        process: ProcessIdentity,
        ppid: u32,
        at: UtcTimestamp,
        image: ProcessImage,
    },
    /// A process a member forked. It runs its parent's image until it execs.
    Forked {
        process: ProcessIdentity,
        parent: ProcessIdentity,
        at: UtcTimestamp,
    },
    Exec {
        process: ProcessIdentity,
        image: ProcessImage,
    },
    Exited {
        process: ProcessIdentity,
        status: ExitStatus,
        at: UtcTimestamp,
    },
    /// The event source itself reported that it missed something.
    Lost(ProcessCoverageGap),
}

/// An observation that contradicts what the fold already holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProcessFoldError {
    #[error("process {pid} was observed born twice with the same birth identity")]
    BornTwice { pid: u32 },
    #[error("process {pid} was observed again after its exit")]
    AfterExit { pid: u32 },
}

/// A retained life and the life it was forked from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessNode<'a> {
    pub identity: ProcessIdentity,
    /// `None` for a root.
    pub parent: Option<ProcessIdentity>,
    pub ppid: u32,
    pub image: &'a ProcessImage,
    pub born_at: &'a UtcTimestamp,
    pub exit: Option<&'a ProcessExit>,
}

#[derive(Debug)]
struct Life {
    identity: ProcessIdentity,
    parent: Option<usize>,
    ppid: u32,
    /// Shared with the parent until this life execs: a fork copies no argv.
    image: Arc<ProcessImage>,
    born_at: UtcTimestamp,
    exit: Option<ProcessExit>,
}

impl Life {
    fn sample(&self) -> JobProcessSample {
        JobProcessSample {
            pid: self.identity.pid,
            ppid: self.ppid,
            program: self.image.program.clone(),
            argv: self.image.argv.clone(),
            born_at: self.born_at.clone(),
            exit: self.exit.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ProcessTreeFold {
    /// Every life, in birth-observation order.
    lives: Vec<Life>,
    by_identity: HashMap<ProcessIdentity, usize>,
    /// Each pid's life with no observed exit.
    running: HashMap<u32, usize>,
    coverage: ProcessCoverage,
}

impl Default for ProcessTreeFold {
    fn default() -> Self {
        Self {
            lives: Vec::new(),
            by_identity: HashMap::new(),
            running: HashMap::new(),
            coverage: ProcessCoverage::Complete,
        }
    }
}

impl ProcessTreeFold {
    pub fn apply(&mut self, observation: ProcessObservation) -> Result<(), ProcessFoldError> {
        match observation {
            ProcessObservation::Root {
                process,
                ppid,
                at,
                image,
            } => self.born(process, None, ppid, at, Arc::new(image)),
            ProcessObservation::Forked {
                process,
                parent,
                at,
            } => {
                let Some(&index) = self.by_identity.get(&parent) else {
                    self.gap(ProcessCoverageGap::UnobservedBirth { pid: parent.pid });
                    return Ok(());
                };
                let parent_life = &self.lives[index];
                if parent_life.exit.is_some() {
                    return Err(ProcessFoldError::AfterExit { pid: parent.pid });
                }
                let image = Arc::clone(&parent_life.image);
                self.born(process, Some(index), parent.pid, at, image)
            }
            ProcessObservation::Exec { process, image } => {
                let Some(life) = self.running_life(process)? else {
                    return Ok(());
                };
                life.image = Arc::new(image);
                Ok(())
            }
            ProcessObservation::Exited {
                process,
                status,
                at,
            } => {
                let Some(life) = self.running_life(process)? else {
                    return Ok(());
                };
                life.exit = Some(ProcessExit {
                    status,
                    exited_at: at,
                });
                // A late exit of a pid's earlier life must not end the life now holding it.
                let index = self.by_identity[&process];
                if self.running.get(&process.pid) == Some(&index) {
                    self.running.remove(&process.pid);
                }
                Ok(())
            }
            ProcessObservation::Lost(reason) => {
                self.gap(reason);
                Ok(())
            }
        }
    }

    /// The retained lives, in birth-observation order.
    pub fn nodes(&self) -> impl Iterator<Item = ProcessNode<'_>> {
        self.lives.iter().map(|life| ProcessNode {
            identity: life.identity,
            parent: life.parent.map(|index| self.lives[index].identity),
            ppid: life.ppid,
            image: &life.image,
            born_at: &life.born_at,
            exit: life.exit.as_ref(),
        })
    }

    pub fn coverage(&self) -> &ProcessCoverage {
        &self.coverage
    }

    /// The life of `pid` with no observed exit: a kernel event that names only a pid (kqueue's
    /// `NOTE_EXEC`/`NOTE_EXIT`) belongs to it.
    pub fn live(&self, pid: u32) -> Option<ProcessIdentity> {
        self.running
            .get(&pid)
            .map(|&index| self.lives[index].identity)
    }

    pub fn tree(&self, job_id: JobId, sampled_at: UtcTimestamp) -> JobProcessTree {
        JobProcessTree {
            job_id,
            sampled_at,
            processes: self.lives.iter().map(Life::sample).collect(),
            coverage: self.coverage.clone(),
        }
    }

    fn born(
        &mut self,
        identity: ProcessIdentity,
        parent: Option<usize>,
        ppid: u32,
        at: UtcTimestamp,
        image: Arc<ProcessImage>,
    ) -> Result<(), ProcessFoldError> {
        if self.by_identity.contains_key(&identity) {
            return Err(ProcessFoldError::BornTwice { pid: identity.pid });
        }
        let index = self.lives.len();
        if self.running.insert(identity.pid, index).is_some() {
            self.gap(ProcessCoverageGap::UnobservedExit { pid: identity.pid });
        }
        self.by_identity.insert(identity, index);
        self.lives.push(Life {
            identity,
            parent,
            ppid,
            image,
            born_at: at,
            exit: None,
        });
        Ok(())
    }

    /// A life with no observed exit; `None`, recorded as a gap, for a life whose birth was never
    /// observed.
    fn running_life(
        &mut self,
        process: ProcessIdentity,
    ) -> Result<Option<&mut Life>, ProcessFoldError> {
        let Some(&index) = self.by_identity.get(&process) else {
            self.gap(ProcessCoverageGap::UnobservedBirth { pid: process.pid });
            return Ok(None);
        };
        let life = &mut self.lives[index];
        if life.exit.is_some() {
            return Err(ProcessFoldError::AfterExit { pid: process.pid });
        }
        Ok(Some(life))
    }

    fn gap(&mut self, reason: ProcessCoverageGap) {
        if self.coverage == ProcessCoverage::Complete {
            self.coverage = ProcessCoverage::Gap { reason };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    use super::*;

    fn at(second: u32) -> UtcTimestamp {
        UtcTimestamp::new(format!("2026-10-07T00:00:{second:02}Z")).unwrap()
    }

    fn id(pid: u32, birth: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(birth),
        }
    }

    fn image(program: &str, argv: &[&[u8]]) -> ProcessImage {
        ProcessImage {
            program: program.to_owned(),
            argv: argv
                .iter()
                .map(|arg| CommandArg::new(OsString::from_vec(arg.to_vec())))
                .collect(),
        }
    }

    fn root(fold: &mut ProcessTreeFold, process: ProcessIdentity) {
        fold.apply(ProcessObservation::Root {
            process,
            ppid: 1,
            at: at(0),
            image: image("/bin/sh", &[b"sh", b"-c", b"build"]),
        })
        .unwrap();
    }

    fn fork(
        fold: &mut ProcessTreeFold,
        process: ProcessIdentity,
        parent: ProcessIdentity,
        second: u32,
    ) {
        fold.apply(ProcessObservation::Forked {
            process,
            parent,
            at: at(second),
        })
        .unwrap();
    }

    fn exit(fold: &mut ProcessTreeFold, process: ProcessIdentity, code: i32, second: u32) {
        fold.apply(ProcessObservation::Exited {
            process,
            status: ExitStatus::Exited { code },
            at: at(second),
        })
        .unwrap();
    }

    fn job() -> JobId {
        JobId::new(7).unwrap()
    }

    #[test]
    fn retains_exited_lives_with_parentage_and_byte_exact_argv() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        let compiler = id(101, 2);
        root(&mut fold, shell);
        fork(&mut fold, compiler, shell, 1);
        let argv: &[&[u8]] = &[b"cc", b"-o", b"out", b"\xff\xfe-not-utf8"];
        fold.apply(ProcessObservation::Exec {
            process: compiler,
            image: image("/usr/bin/cc", argv),
        })
        .unwrap();
        exit(&mut fold, compiler, 3, 2);

        let nodes: Vec<_> = fold.nodes().collect();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].identity, shell);
        assert_eq!(nodes[0].parent, None);
        assert_eq!(nodes[0].exit, None);
        assert_eq!(nodes[1].parent, Some(shell));

        let tree = fold.tree(job(), at(3));
        assert_eq!(tree.coverage, ProcessCoverage::Complete);
        let child = &tree.processes[1];
        assert_eq!((child.pid, child.ppid), (101, 100));
        assert_eq!(child.program, "/usr/bin/cc");
        let bytes: Vec<Vec<u8>> = child
            .argv
            .iter()
            .map(|arg| arg.as_os_str().to_owned().into_vec())
            .collect();
        assert_eq!(
            bytes,
            argv.iter().map(|arg| arg.to_vec()).collect::<Vec<_>>()
        );
        assert_eq!(child.born_at, at(1));
        assert_eq!(
            child.exit,
            Some(ProcessExit {
                status: ExitStatus::Exited { code: 3 },
                exited_at: at(2),
            })
        );
    }

    #[test]
    fn a_forked_child_runs_its_parents_image_until_it_execs() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        let subshell = id(102, 3);
        root(&mut fold, shell);
        fork(&mut fold, subshell, shell, 1);
        let tree = fold.tree(job(), at(2));
        assert_eq!(tree.processes[1].program, "/bin/sh");
        assert_eq!(tree.processes[1].argv, tree.processes[0].argv);
        fold.apply(ProcessObservation::Exec {
            process: subshell,
            image: image("/usr/bin/make", &[b"make"]),
        })
        .unwrap();
        fork(&mut fold, id(103, 4), shell, 2);
        let tree = fold.tree(job(), at(3));
        assert_eq!(tree.processes[1].program, "/usr/bin/make");
        // The child's exec leaves the image it shared with its parent, which the parent's next
        // child shares in turn.
        assert_eq!(tree.processes[0].program, "/bin/sh");
        assert_eq!(tree.processes[2].program, "/bin/sh");
        assert_eq!(tree.processes[2].argv, tree.processes[0].argv);
    }

    /// Joining by numeric pid fails this: it folds both lives of pid 200 into one record and
    /// applies the stale event to the second life.
    #[test]
    fn a_reused_pid_is_a_second_life_and_never_takes_the_first_ones_events() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        let first = id(200, 10);
        let second = id(200, 11);
        root(&mut fold, shell);
        fork(&mut fold, first, shell, 1);
        exit(&mut fold, first, 0, 2);
        fork(&mut fold, second, shell, 3);
        fold.apply(ProcessObservation::Exec {
            process: second,
            image: image("/usr/bin/ld", &[b"ld"]),
        })
        .unwrap();

        assert_eq!(
            fold.apply(ProcessObservation::Exec {
                process: first,
                image: image("/bin/stale", &[b"stale"]),
            }),
            Err(ProcessFoldError::AfterExit { pid: 200 })
        );
        exit(&mut fold, second, 7, 4);

        let tree = fold.tree(job(), at(5));
        assert_eq!(tree.coverage, ProcessCoverage::Complete);
        let lives: Vec<_> = tree.processes.iter().filter(|p| p.pid == 200).collect();
        assert_eq!(lives.len(), 2);
        assert_eq!(lives[0].program, "/bin/sh");
        assert_eq!(
            (lives[0].born_at.clone(), lives[1].born_at.clone()),
            (at(1), at(3))
        );
        assert_eq!(
            lives[0].exit.as_ref().unwrap().status,
            ExitStatus::Exited { code: 0 }
        );
        assert_eq!(lives[0].exit.as_ref().unwrap().exited_at, at(2));
        assert_eq!(lives[1].program, "/usr/bin/ld");
        assert_eq!(
            lives[1].exit.as_ref().unwrap().status,
            ExitStatus::Exited { code: 7 }
        );
        assert_eq!(lives[1].exit.as_ref().unwrap().exited_at, at(4));
    }

    #[test]
    fn a_source_reported_loss_is_a_gap_that_later_events_cannot_close() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        root(&mut fold, shell);
        fold.apply(ProcessObservation::Lost(ProcessCoverageGap::EventsLost))
            .unwrap();
        fork(&mut fold, id(101, 2), shell, 1);
        exit(&mut fold, id(101, 2), 0, 2);
        assert_eq!(
            fold.tree(job(), at(3)).coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::EventsLost
            }
        );
    }

    #[test]
    fn an_event_for_an_unobserved_birth_is_a_gap_not_a_guess() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        root(&mut fold, shell);
        exit(&mut fold, id(150, 9), 0, 1);
        fork(&mut fold, id(151, 10), id(150, 9), 1);
        let tree = fold.tree(job(), at(2));
        assert_eq!(tree.processes.len(), 1);
        assert_eq!(
            tree.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnobservedBirth { pid: 150 }
            }
        );
    }

    #[test]
    fn a_pid_born_again_without_its_exit_records_the_missed_exit() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        root(&mut fold, shell);
        fork(&mut fold, id(300, 20), shell, 1);
        fork(&mut fold, id(300, 21), shell, 2);
        let tree = fold.tree(job(), at(3));
        assert_eq!(tree.processes.len(), 3);
        assert_eq!(tree.processes[1].exit, None);
        assert_eq!(
            tree.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnobservedExit { pid: 300 }
            }
        );
    }

    /// Removing the pid from the live map on any exit fails this: the late exit of the first
    /// life would leave the second life, still running, unreachable by its pid.
    #[test]
    fn a_late_exit_of_an_earlier_life_leaves_the_current_life_live() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        let first = id(300, 20);
        let second = id(300, 21);
        root(&mut fold, shell);
        fork(&mut fold, first, shell, 1);
        fork(&mut fold, second, shell, 2);
        exit(&mut fold, first, 0, 3);
        assert_eq!(fold.live(300), Some(second));
        exit(&mut fold, second, 4, 4);
        assert_eq!(fold.live(300), None);

        let tree = fold.tree(job(), at(5));
        assert_eq!(tree.processes[1].exit.as_ref().unwrap().exited_at, at(3));
        assert_eq!(
            tree.processes[2].exit.as_ref().unwrap().status,
            ExitStatus::Exited { code: 4 }
        );
    }

    #[test]
    fn contradicting_observations_are_refused() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        root(&mut fold, shell);
        assert_eq!(
            fold.apply(ProcessObservation::Root {
                process: shell,
                ppid: 1,
                at: at(1),
                image: image("/bin/sh", &[b"sh"]),
            }),
            Err(ProcessFoldError::BornTwice { pid: 100 })
        );
        exit(&mut fold, shell, 0, 2);
        assert_eq!(
            fold.apply(ProcessObservation::Exited {
                process: shell,
                status: ExitStatus::Exited { code: 1 },
                at: at(3),
            }),
            Err(ProcessFoldError::AfterExit { pid: 100 })
        );
        assert_eq!(
            fold.apply(ProcessObservation::Forked {
                process: id(101, 2),
                parent: shell,
                at: at(3),
            }),
            Err(ProcessFoldError::AfterExit { pid: 100 })
        );
        assert_eq!(fold.coverage(), &ProcessCoverage::Complete);
    }

    #[test]
    fn the_wire_projection_pairs_exit_with_its_time_and_omits_what_is_absent() {
        let mut fold = ProcessTreeFold::default();
        let shell = id(100, 1);
        root(&mut fold, shell);
        fork(&mut fold, id(101, 2), shell, 1);
        fold.apply(ProcessObservation::Exited {
            process: id(101, 2),
            status: ExitStatus::Signaled {
                signal: 9,
                core_dumped: false,
            },
            at: at(2),
        })
        .unwrap();
        fold.apply(ProcessObservation::Lost(
            ProcessCoverageGap::UnobservedBirth { pid: 5 },
        ))
        .unwrap();
        let tree = fold.tree(job(), at(3));
        let json = serde_json::to_value(&tree).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "jobId": 7,
                "sampledAt": "2026-10-07T00:00:03Z",
                "processes": [
                    {
                        "pid": 100,
                        "ppid": 1,
                        "program": "/bin/sh",
                        "argv": serde_json::to_value(&tree.processes[0].argv).unwrap(),
                        "bornAt": "2026-10-07T00:00:00Z",
                    },
                    {
                        "pid": 101,
                        "ppid": 100,
                        "program": "/bin/sh",
                        "argv": serde_json::to_value(&tree.processes[1].argv).unwrap(),
                        "bornAt": "2026-10-07T00:00:01Z",
                        "exit": {
                            "status": { "kind": "signaled", "signal": 9, "coreDumped": false },
                            "exitedAt": "2026-10-07T00:00:02Z",
                        },
                    },
                ],
                "coverage": { "kind": "gap", "reason": { "kind": "unobservedBirth", "pid": 5 } },
            })
        );
        assert_eq!(
            serde_json::from_value::<JobProcessTree>(json.clone()).unwrap(),
            tree
        );

        let mut exit_without_time = json;
        exit_without_time["processes"][1]["exit"]
            .as_object_mut()
            .unwrap()
            .remove("exitedAt");
        assert!(serde_json::from_value::<JobProcessTree>(exit_without_time).is_err());
    }
}
