//! Whom a holder census asks, and whose lifecycle leases pass a process over (02_workspaces.md,
//! "Holders cowshed does not run").
//!
//! A census serves one store, the one its caller's roots name ([`CensusScope`]). The host store's
//! census asks every process the kernel lists: what it exists to find is a process cowshed does not
//! run, such as a host shell's `direnv exec <ws> nx …`. A test fixture's store is reached only by
//! the process that minted the fixture's namespace, the processes in its process group and the
//! descendants of either, so its census asks those alone: it never queries an unrelated host
//! process, whose per-descriptor query waits as long as any kernel call that process sits in. A
//! fixture's stand-in for a host shell's run, which the fixture backgrounds from a shell that
//! exits, stays in the group though launchd adopts it; a daemon that made a session of its own
//! leaves both. Either census reads its own store's lifecycle leases and no other store's.
//!
//! [`asked`] decides from the kernel's listing alone, with no I/O; `build_volume::nx` lists the
//! processes, reads the leases and asks the processes it names.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use cowshed_os::namespace::HostNamespace;

/// One store's holder census: the store whose lifecycle leases it reads, and whose names it serves,
/// which decide the processes it may ask ([`Self::reach`]). It shares the store path its roots
/// hold, so taking one from [`crate::storage::bootstrap::CanonicalRoots::census`] copies nothing;
/// a storage host builds its own once and lends it to every census it takes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusScope {
    store: Arc<Path>,
    namespace: HostNamespace,
}

/// The processes a census may ask.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reach {
    /// Every process the kernel lists: the host store's census.
    Host,
    /// The process that minted a test namespace, by its pid
    /// ([`cowshed_os::namespace::TestNamespace::owner_pid`]), the processes in its process group,
    /// and the descendants of either: a fixture store's census.
    Fixture(libc::pid_t),
}

impl CensusScope {
    /// The census of the store at `store`, whose names `namespace` gives.
    pub fn new(namespace: HostNamespace, store: Arc<Path>) -> Self {
        Self { store, namespace }
    }

    /// The store whose `<owner>/<repo>/sessions` leases the census reads.
    pub fn store(&self) -> &Path {
        &self.store
    }

    pub fn namespace(&self) -> HostNamespace {
        self.namespace
    }

    /// Production's census reaches every process, a test namespace's its minting process's group
    /// and tree.
    pub fn reach(&self) -> Reach {
        match self.namespace {
            HostNamespace::Production => Reach::Host,
            // Five base-36 digits hold at most 36^5 - 1, well inside a pid_t.
            HostNamespace::Test(test) => Reach::Fixture(test.owner_pid().cast_signed()),
        }
    }
}

impl Reach {
    /// Whether [`asked`] reads each listed process's lineage.
    pub(crate) fn needs_lineage(self) -> bool {
        matches!(self, Self::Fixture(_))
    }
}

/// One process the kernel lists, with its lineage when the reach needs it and the kernel
/// describes the process to this user.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Listed {
    pub pid: libc::pid_t,
    pub lineage: Option<Lineage>,
}

/// A process's parent and process group, as the kernel lists them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Lineage {
    pub parent: libc::pid_t,
    pub group: libc::pid_t,
}

/// The processes one census asks, in listing order, and how many in its reach it passed over
/// because a lifecycle lease of its store names them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Asked {
    pub pids: Vec<libc::pid_t>,
    pub passed_over: usize,
}

/// Of the processes `listed`, those `reach` covers, but for each one a running lifecycle operation
/// of the census's store is (`lifecycle`), which is passed over and counted. A fixture's reach is
/// its owner, every process in the owner's process group, and every descendant of either by the
/// listed parents; a parent the listing does not name, or a chain longer than the listing (a pid
/// reused mid-listing), ends outside it, so a process launchd adopted is in a fixture's reach only
/// while it stays in the owner's group.
pub(crate) fn asked(reach: Reach, listed: &[Listed], lifecycle: &BTreeSet<libc::pid_t>) -> Asked {
    let reached = match reach {
        Reach::Host => None,
        Reach::Fixture(owner) => Some(fixture(owner, listed)),
    };
    let mut asked = Asked::default();
    for process in listed {
        if reached
            .as_ref()
            .is_some_and(|reached| reached.get(&process.pid) != Some(&true))
        {
            continue;
        }
        if lifecycle.contains(&process.pid) {
            asked.passed_over += 1;
        } else {
            asked.pids.push(process.pid);
        }
    }
    asked
}

/// Whether each listed process is in the fixture reach of `owner`: `owner` itself, a process in
/// its process group, or a descendant of either, by the listed lineage. One walk up per process,
/// each answer remembered for every pid the walk passed.
fn fixture(owner: libc::pid_t, listed: &[Listed]) -> HashMap<libc::pid_t, bool> {
    let lineage: HashMap<libc::pid_t, Lineage> = listed
        .iter()
        .filter_map(|process| process.lineage.map(|lineage| (process.pid, lineage)))
        .collect();
    let group = lineage.get(&owner).map(|lineage| lineage.group);
    let mut known = HashMap::with_capacity(listed.len() + 1);
    known.insert(owner, true);
    let mut walked = Vec::new();
    for process in listed {
        let mut current = process.pid;
        let member = loop {
            if let Some(&member) = known.get(&current) {
                break member;
            }
            if walked.len() > listed.len() {
                break false;
            }
            walked.push(current);
            match lineage.get(&current) {
                Some(found) if Some(found.group) == group => break true,
                Some(found) if found.parent != current => current = found.parent,
                _ => break false,
            }
        };
        for pid in walked.drain(..) {
            known.insert(pid, member);
        }
    }
    known
}

/// A test's store that holds no lease: the census of [`this_test`] or [`everywhere`] reads none.
#[cfg(test)]
const NO_STORE: &str = "/var/empty";

/// This test process's census: the process and its descendants, as a fixture store's census
/// reaches them, over a store that holds no lease.
#[cfg(test)]
pub(crate) fn this_test() -> CensusScope {
    let namespace = cowshed_os::namespace::TestNamespace::of(std::process::id(), 0)
        .expect("this process's namespace");
    CensusScope::new(
        HostNamespace::Test(namespace),
        Arc::from(Path::new(NO_STORE)),
    )
}

/// A census that reaches every process, as the host store's does, over a store that holds no
/// lease: for a test whose claim is that no process anywhere, a detached one included, holds
/// something.
#[cfg(test)]
pub(crate) fn everywhere() -> CensusScope {
    CensusScope::new(HostNamespace::Production, Arc::from(Path::new(NO_STORE)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::namespace::TestNamespace;

    /// Rows of (pid, parent, process group); a parent of 0 stands for a process the kernel did
    /// not describe.
    fn listed(rows: &[(libc::pid_t, libc::pid_t, libc::pid_t)]) -> Vec<Listed> {
        rows.iter()
            .map(|&(pid, parent, group)| Listed {
                pid,
                lineage: (parent != 0).then_some(Lineage { parent, group }),
            })
            .collect()
    }

    /// Production's census reaches every process and a test namespace's its minting process's
    /// group and tree, each over the store its roots name.
    #[test]
    fn a_namespace_decides_the_reach_and_the_roots_the_store() {
        let store = Path::new("/fixture/store");
        let host = CensusScope::new(
            HostNamespace::Production,
            Arc::from(Path::new("/private/cowshed/store")),
        );
        assert_eq!(host.reach(), Reach::Host);
        assert!(!host.reach().needs_lineage());
        let fixture = CensusScope::new(
            HostNamespace::Test(TestNamespace::of(94_769, 3).unwrap()),
            Arc::from(store),
        );
        assert_eq!(
            (fixture.reach(), fixture.store()),
            (Reach::Fixture(94_769), store)
        );
        assert!(fixture.reach().needs_lineage());
    }

    /// Which pids a census asks, and how many it passes over, for each reach.
    #[test]
    fn asked_follows_the_reach_and_passes_over_the_scopes_lifecycle_processes() {
        type Row = (
            &'static str,
            Reach,
            &'static [(libc::pid_t, libc::pid_t, libc::pid_t)],
            &'static [libc::pid_t],
            &'static [libc::pid_t],
            usize,
        );
        let rows: &[Row] = &[
            (
                "the host asks every listed process, in listing order",
                Reach::Host,
                &[(5, 0, 0), (3, 0, 0), (4, 0, 0)],
                &[],
                &[5, 3, 4],
                0,
            ),
            (
                "the host passes over a lifecycle process and counts it",
                Reach::Host,
                &[(1, 0, 0), (10, 0, 0), (11, 0, 0)],
                &[11],
                &[1, 10],
                1,
            ),
            (
                "a fixture is its owner and every descendant, never a sibling's",
                Reach::Fixture(10),
                &[
                    (1, 1, 1),
                    (10, 1, 10),
                    (11, 10, 11),
                    (12, 11, 12),
                    (20, 1, 20),
                    (21, 20, 21),
                ],
                &[],
                &[10, 11, 12],
                0,
            ),
            (
                "a lifecycle process in reach is passed over; one outside it is no concern",
                Reach::Fixture(10),
                &[(10, 1, 10), (11, 10, 11), (20, 1, 20)],
                &[11, 20],
                &[10],
                1,
            ),
            (
                "a run launchd adopted is in reach while it stays in the owner's group",
                Reach::Fixture(10),
                &[(10, 1, 10), (30, 1, 10), (31, 30, 31)],
                &[],
                &[10, 30, 31],
                0,
            ),
            (
                "a daemon launchd adopted that made a group of its own is outside",
                Reach::Fixture(10),
                &[(10, 1, 10), (30, 1, 30)],
                &[],
                &[10],
                0,
            ),
            (
                "an owner in a group it does not lead reaches every process of that group",
                Reach::Fixture(10),
                &[(5, 1, 5), (10, 5, 5), (6, 1, 5), (7, 1, 7)],
                &[],
                &[5, 10, 6],
                0,
            ),
            (
                "a process the kernel did not describe is outside",
                Reach::Fixture(10),
                &[(10, 1, 10), (11, 0, 0)],
                &[],
                &[10],
                0,
            ),
            (
                "a parent that exited before the listing ends the chain outside",
                Reach::Fixture(10),
                &[(10, 1, 10), (12, 11, 12)],
                &[],
                &[10],
                0,
            ),
            (
                "a cycle of reused pids ends outside",
                Reach::Fixture(10),
                &[(10, 1, 10), (40, 41, 40), (41, 40, 41)],
                &[],
                &[10],
                0,
            ),
            (
                "an unlisted owner's children are still in reach",
                Reach::Fixture(10),
                &[(11, 10, 11), (13, 12, 13)],
                &[],
                &[11],
                0,
            ),
        ];
        for &(case, reach, rows, lifecycle, pids, passed_over) in rows {
            let lifecycle = lifecycle.iter().copied().collect();
            assert_eq!(
                asked(reach, &listed(rows), &lifecycle),
                Asked {
                    pids: pids.to_vec(),
                    passed_over,
                },
                "{case}"
            );
        }
    }
}
