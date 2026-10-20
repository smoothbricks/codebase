use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::hint::black_box;

thread_local! {
    static ALLOCATIONS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}
struct Counting;
fn charge(bytes: usize) {
    ALLOCATIONS.with(|count| {
        let (calls, total) = count.get();
        count.set((calls + 1, total + bytes));
    });
}
// SAFETY: ownership and layouts are forwarded unchanged to the system allocator. The
// const-initialized thread-local census allocates nothing and isolates concurrent tests.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        charge(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations<T>(f: impl FnOnce() -> T) -> (T, (usize, usize)) {
    ALLOCATIONS.with(|count| count.set((0, 0)));
    let result = black_box(f());
    let count = ALLOCATIONS.with(Cell::get);
    (result, count)
}

/// Frozen pre-cutover control: the actor's tuple Vec, the writer's Group Vec, the inherited
/// clone, JSON buffer and temporary-path construction all remain inside the measured call.
fn legacy_record<I>(path: &Path, groups: I) -> io::Result<()>
where
    I: Iterator<Item = JobGroupClaim>,
{
    let groups: Vec<_> = groups.map(|claim| (claim.job_id, claim.leader)).collect();
    let carried: Vec<UnresolvedGroup> = Vec::new();
    let ledger = Ledger {
        supervisor: std::process::id(),
        groups: groups
            .iter()
            .map(|&(job_id, leader)| Group {
                job_id,
                pgid: leader.pgid,
                leader_start: Some(leader.start),
                leader_birth: None,
                members: None,
            })
            .collect(),
        ended: false,
        unresolved: carried.clone(),
    };
    replace(path, &ledger)
}

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("group-publication-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove publication fixture");
    }
}

#[test]
fn borrowed_publication_is_byte_identical_to_the_owned_ledger() {
    let inherited = [
        UnresolvedGroup {
            lost_supervisor: u32::MAX,
            group: Group {
                job_id: u64::MAX,
                pgid: i32::MAX,
                leader_start: Some(u64::MAX),
                leader_birth: None,
                members: None,
            },
        },
        UnresolvedGroup {
            lost_supervisor: 1,
            group: Group {
                job_id: 2,
                pgid: 3,
                leader_start: None,
                leader_birth: Some(4),
                members: Some(RecordedMembers {
                    boot_session: "an-earlier-boot".into(),
                    lives: vec![ProcessIdentity {
                        pid: 8,
                        birth: super::super::process_tree::BirthToken(9),
                    }],
                }),
            },
        },
        UnresolvedGroup {
            lost_supervisor: 5,
            group: Group {
                job_id: 6,
                pgid: 7,
                leader_start: None,
                leader_birth: None,
                members: None,
            },
        },
    ];
    // Lives are published only beside a session, so every count crosses claims with and without.
    let boot = OnceLock::from(String::from("this-boot"));
    let lives: Arc<[ProcessIdentity]> = (1..=3)
        .map(|pid| ProcessIdentity {
            pid,
            birth: super::super::process_tree::BirthToken(u64::MAX - u64::from(pid)),
        })
        .collect();
    for count in [0, 1, 16, 17, 1_024] {
        let groups: Vec<_> = (0..count)
            .map(|id| JobGroupClaim {
                job_id: u64::MAX - id,
                leader: GroupLeader::from_parts(i32::MAX, id, u64::MAX - id).unwrap(),
                members: match id % 3 {
                    0 => None,
                    1 => Some(Arc::from([])),
                    _ => Some(Arc::clone(&lives)),
                },
            })
            .collect();
        for carried in [&inherited[..0], &inherited[..]] {
            for ended in [false, true] {
                let owned = Ledger {
                    supervisor: u32::MAX,
                    groups: groups
                        .iter()
                        .map(|claim| Group {
                            job_id: claim.job_id,
                            pgid: claim.leader.pgid,
                            leader_start: Some(claim.leader.start),
                            leader_birth: None,
                            members: claim
                                .members
                                .as_deref()
                                .filter(|lives| !lives.is_empty())
                                .map(|lives| RecordedMembers {
                                    boot_session: "this-boot".into(),
                                    lives: lives.to_vec(),
                                }),
                        })
                        .collect(),
                    ended,
                    unresolved: carried.to_vec(),
                };
                let borrowed = Publication {
                    supervisor: u32::MAX,
                    groups: Groups {
                        claims: groups.iter().cloned(),
                        boot_session: &boot,
                    },
                    ended,
                    unresolved: carried,
                };
                let old = serde_json::to_vec(&owned).unwrap();
                let new = serde_json::to_vec(&borrowed).unwrap();
                assert_eq!(
                    old,
                    new,
                    "count={count}, inherited={}, ended={ended}",
                    carried.len()
                );
                let decoded: Ledger = serde_json::from_slice(&new).unwrap();
                assert_eq!(decoded.groups, owned.groups);
                assert_eq!(decoded.unresolved, owned.unresolved);
            }
        }
    }
}

#[test]
fn ordinary_publication_allocates_only_its_json_output_buffer() {
    let (_, positive) = allocations(|| black_box(vec![0_u8; 97]));
    assert!(
        positive.0 > 0 && positive.1 >= 97,
        "allocator positive control"
    );
    let scratch = Scratch::new();
    let path = scratch.0.join("groups");
    let recorder = Recorder::new(path.clone());
    let leader = GroupLeader::from_parts(42, 1, 1).unwrap();
    for (retained, active) in [(16, 0), (16, 1), (17, 1), (1_024, 1), (1_024, 1_024)] {
        let jobs: BTreeMap<_, _> = (1..=retained)
            .map(|id| (id, id > retained - active))
            .collect();
        let groups = || {
            jobs.iter()
                .filter(|(_, active)| **active)
                .map(|(&id, _)| JobGroupClaim::group(id, leader))
        };
        let (before, old_allocations) = allocations(|| legacy_record(&path, groups()));
        before.unwrap();
        let old = std::fs::read(&path).unwrap();
        let (after, new_allocations) = allocations(|| recorder.record(groups(), &[]));
        let recorded = after.unwrap();
        assert!(recorded.carried.is_empty() && recorded.uninspected.is_empty());
        let new = std::fs::read(&path).unwrap();
        assert_eq!(old, new, "retained={retained}, active={active}");
        let (encoded, buffer_allocations) = allocations(|| {
            serde_json::to_vec(&Publication {
                supervisor: std::process::id(),
                groups: Groups {
                    claims: groups(),
                    boot_session: &OnceLock::new(),
                },
                ended: false,
                unresolved: &[],
            })
            .unwrap()
        });
        assert_eq!(new, encoded);
        assert_eq!(
            new_allocations, buffer_allocations,
            "publication allocated beyond its JSON buffer"
        );
        assert!(new_allocations.0 < old_allocations.0);
        assert!(new_allocations.1 < old_allocations.1);
        println!(
            "publication retained={retained} active={active} before={old_allocations:?} after={new_allocations:?} json_len={} json_capacity={}",
            encoded.len(),
            encoded.capacity()
        );
    }
}
