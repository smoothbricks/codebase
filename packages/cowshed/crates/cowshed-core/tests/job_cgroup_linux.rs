//! A job owns its cgroup v2 from before its first instruction (07_api.md, "Complete job
//! accounting and observation reconciliation"), proven with native processes inside a cgroup
//! delegated to an unprivileged controller.
//!
//! The harness asks `sudo -n systemd-run --scope -p Delegate=yes` for a fresh delegated scope and
//! re-executes this test binary there as root, only to hand the scope to the invoking user the way
//! cgroup-v2.rst "Delegation" describes (its directory, `cgroup.procs`, `cgroup.threads` and
//! `cgroup.subtree_control`) and drop to that user. The unprivileged controller then takes
//! authority with [`CgroupAuthority::delegated`] and runs workloads: this binary again, whose
//! first action is to report its own cgroup, which burns CPU it measures itself (`getrusage`) and
//! may start children. Every comparison reads the job's `cpu.stat` directly.
//!
//! A host without passwordless sudo, systemd, or a delegable cgroup v2 fails here with the
//! refused step; nothing is skipped.

#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::os::unix::fs::chown;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use cowshed_core::api::dto::JobId;
use cowshed_core::fork_lock::{Run as _, Spawn as _};
use cowshed_core::metadata::WorkspaceIncarnation;
use cowshed_core::runtime::job_cgroup::{
    CGROUP_MOUNT, CONTROLLER_LEAF, CgroupAuthority, CgroupError, IncarnationCgroups, JobCgroup,
    JobCgroupIdentity,
};
use serde::{Deserialize, Serialize};

const ROLE: &str = "COWSHED_JOB_CGROUP_ROLE";
const ROLE_TEST: &str = "job_cgroup_role";
const REPORT: &str = "COWSHED_JOB_CGROUP_REPORT ";
const PAUSED: &str = "COWSHED_JOB_CGROUP_PAUSED";
const HELD: &str = "COWSHED_JOB_CGROUP_HELD";
/// A writable directory on block storage ([`block_storage_directory`]).
const STORAGE: &str = "COWSHED_JOB_CGROUP_STORAGE";

/// CPU a placed process can spend outside its job: from its fork to its placement write, in the
/// spawning library's child setup. Measured deficits are printed beside every comparison.
const PLACEMENT_WINDOW_US: u64 = 5_000;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "role", rename_all = "camelCase")]
enum Role {
    /// Root inside the fresh scope: delegate it to `uid`/`gid` and become the controller.
    Delegate {
        uid: u32,
        gid: u32,
    },
    Controller,
    Workload(Workload),
}

/// What a workload process does, in order.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Workload {
    burn_before_ms: u64,
    /// Report [`PAUSED`] and wait for one byte on stdin.
    pause: bool,
    burn_after_ms: u64,
    /// Anonymous memory to touch page by page and release again.
    allocate_mib: u64,
    /// A file beside this binary to fill with this many MiB through the page cache, held until
    /// the workload ends and removed then.
    page_cache: Option<(PathBuf, u64)>,
    /// A file beside this binary to write this many MiB to past the page cache (`O_DIRECT`),
    /// flushed, then read this many MiB of it back the same way, and remove it.
    direct_io: Option<(PathBuf, u64, u64)>,
    /// A file the page cache already holds, read whole through it.
    cached_read: Option<PathBuf>,
    /// A file to allocate this many MiB to without writing it, held until the workload ends.
    allocate_file: Option<(PathBuf, u64)>,
    /// Started one after another once the burns are done, each waited for.
    children: Vec<Workload>,
    /// Once every child was reaped, report [`HELD`] and wait for one byte on stdin.
    hold: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadReport {
    pid: u32,
    /// The process's cgroup as its first action read it.
    first_cgroup: String,
    /// Its cgroup as it read it last, before exiting.
    last_cgroup: String,
    /// Its own CPU, user + system, from exec of this image's parent fork to its report.
    self_cpu_us: u64,
    /// The CPU of every descendant it reaped.
    children_cpu_us: u64,
    children: Vec<WorkloadReport>,
}

impl WorkloadReport {
    fn total_cpu_us(&self) -> u64 {
        self.self_cpu_us + self.children_cpu_us
    }
}

/// The harness: everything else runs in the delegated scope.
#[test]
fn a_job_owns_its_cgroup_from_before_its_first_instruction() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    // SAFETY: getuid and getgid cannot fail.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let delegate = serde_json::to_string(&Role::Delegate { uid, gid }).expect("role");
    let mut assignment = OsString::from(format!("{ROLE}="));
    assignment.push(delegate);
    // Chosen here, in the invoking user's own environment, which sudo does not pass on.
    let mut storage = OsString::from(format!("{STORAGE}="));
    storage.push(block_storage_directory());
    let output = Command::new(find_program("sudo"))
        .arg("-n")
        .arg(find_program("env"))
        .arg(assignment)
        .arg(storage)
        .arg(find_program("systemd-run"))
        .args([
            "--scope",
            "--quiet",
            "--collect",
            "--property=Delegate=yes",
            "--",
        ])
        .arg(test_binary())
        .args(role_arguments())
        .output_locked()
        .expect("run sudo");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{stdout}");
    eprintln!("{stderr}");
    assert!(
        output.status.success(),
        "the delegated controller failed: {}",
        output.status
    );
    assert!(
        stdout.contains("COWSHED_JOB_CGROUP_CONTROLLER_DONE"),
        "the controller never finished its scenarios"
    );
}

#[test]
fn job_cgroup_role() {
    let Some(role) = std::env::var_os(ROLE) else {
        return;
    };
    match serde_json::from_slice(role.as_encoded_bytes()).expect("role") {
        Role::Delegate { uid, gid } => delegate(uid, gid),
        Role::Controller => {
            control();
            println!("COWSHED_JOB_CGROUP_CONTROLLER_DONE");
        }
        Role::Workload(workload) => work(&workload),
    }
}

// ---------------------------------------------------------------------------------------------
// Roles

/// Hand this scope to `uid`/`gid` and continue as the controller under that identity.
fn delegate(uid: u32, gid: u32) {
    let scope = own_cgroup_path();
    for file in [
        "",
        "cgroup.procs",
        "cgroup.threads",
        "cgroup.subtree_control",
    ] {
        let path = if file.is_empty() {
            scope.clone()
        } else {
            scope.join(file)
        };
        chown(&path, Some(uid), Some(gid))
            .unwrap_or_else(|error| panic!("chown {}: {error}", path.display()));
    }
    // SAFETY: plain identity calls on values this process was given; each answer is checked.
    unsafe {
        assert_eq!(libc::setgroups(1, &gid), 0, "setgroups: {}", last_error());
        assert_eq!(libc::setgid(gid), 0, "setgid: {}", last_error());
        assert_eq!(libc::setuid(uid), 0, "setuid: {}", last_error());
    }
    let error = Command::new(test_binary())
        .args(role_arguments())
        .env(
            ROLE,
            serde_json::to_string(&Role::Controller).expect("role"),
        )
        .exec();
    panic!("exec the controller: {error}");
}

/// Report this process's cgroup first, then do what `workload` says.
fn work(workload: &Workload) {
    let first_cgroup = own_cgroup();
    burn(workload.burn_before_ms);
    if workload.pause {
        println!("{PAUSED}");
        std::io::stdout().flush().expect("flush");
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).expect("resume");
    }
    burn(workload.burn_after_ms);
    touch_and_release(workload.allocate_mib);
    if let Some((path, mebibytes)) = &workload.page_cache {
        fill_page_cache(path, *mebibytes);
    }
    if let Some((path, write, read)) = &workload.direct_io {
        uncached_io(path, *write, *read);
        std::fs::remove_file(path).expect("remove the direct I/O file");
    }
    if let Some(path) = &workload.cached_read {
        read_through_cache(path);
    }
    if let Some((path, mebibytes)) = &workload.allocate_file {
        allocate(path, *mebibytes);
    }
    let children = workload
        .children
        .iter()
        .map(|child| {
            let output = workload_command(child)
                .output_locked()
                .expect("run a child workload");
            assert!(output.status.success(), "child workload: {}", output.status);
            report_in(&String::from_utf8_lossy(&output.stdout))
        })
        .collect();
    if workload.hold {
        println!("{HELD}");
        std::io::stdout().flush().expect("flush");
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).expect("release");
    }
    if let Some((path, _)) = &workload.page_cache {
        std::fs::remove_file(path).expect("remove the page cache file");
    }
    if let Some((path, _)) = &workload.allocate_file {
        std::fs::remove_file(path).expect("remove the allocated file");
    }
    let report = WorkloadReport {
        pid: std::process::id(),
        first_cgroup,
        last_cgroup: own_cgroup(),
        self_cpu_us: cpu_us(libc::RUSAGE_SELF),
        children_cpu_us: cpu_us(libc::RUSAGE_CHILDREN),
        children,
    };
    println!(
        "{REPORT}{}",
        serde_json::to_string(&report).expect("report")
    );
}

/// The scenarios, run by the unprivileged controller that holds the delegated scope.
fn control() {
    let scope = own_cgroup_path();
    let authority = CgroupAuthority::delegated().expect("authority over the delegated scope");
    assert_eq!(authority.root(), scope);
    assert_eq!(
        own_cgroup_path(),
        scope.join(CONTROLLER_LEAF),
        "the controller holds no place among the cgroups it distributes controllers to"
    );
    // A restarted controller in the same process finds itself already in its leaf.
    let again = CgroupAuthority::delegated().expect("authority again");
    assert_eq!(again.root(), scope);

    let incarnation = incarnation(1);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");

    descendants_inherit_the_job(&jobs);
    concurrent_jobs_stay_apart(&jobs);
    a_spawners_earlier_work_is_not_charged(&jobs);
    late_migration_misses_initial_cpu(&jobs);
    burst_cpu_outlives_its_processes(&jobs);
    charged_memory_is_not_resident_memory(&jobs);
    storage_io_outlives_its_processes(&jobs);
    retirement_waits_for_the_last_process(&jobs);
    restart_lookup_keeps_the_identity_under_its_incarnation(&authority, &jobs);

    // Leave the scope as admission found it.
    for identity in jobs.outstanding().expect("outstanding") {
        jobs.lookup(&identity)
            .expect("lookup")
            .terminal()
            .expect("every workload ended")
            .retire()
            .expect("retire");
    }
}

// ---------------------------------------------------------------------------------------------
// Scenarios

fn descendants_inherit_the_job(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(1)).expect("admit");
    let child = Workload {
        burn_before_ms: 100,
        ..Workload::default()
    };
    let report = Spawned::placed(
        &job,
        &Workload {
            burn_before_ms: 50,
            children: vec![child.clone(), child],
            ..Workload::default()
        },
    )
    .finish();
    let path = relative(job.path());
    assert_eq!(
        report.first_cgroup, path,
        "the first instruction ran in the job"
    );
    for child in &report.children {
        assert_eq!(child.first_cgroup, path, "a descendant is born in the job");
    }
    assert_accounted(&job, &report, "inheritance");
}

fn concurrent_jobs_stay_apart(jobs: &IncarnationCgroups) {
    let small = jobs.admit(job_id(2)).expect("admit");
    let large = jobs.admit(job_id(3)).expect("admit");
    assert!(
        matches!(
            jobs.admit(job_id(2)),
            Err(CgroupError::AlreadyExists { .. })
        ),
        "a second admission never joins the first one's cgroup"
    );
    let running_small = Spawned::placed(
        &small,
        &Workload {
            burn_before_ms: 300,
            ..Workload::default()
        },
    );
    let running_large = Spawned::placed(
        &large,
        &Workload {
            burn_before_ms: 600,
            ..Workload::default()
        },
    );
    let small_report = running_small.finish();
    let large_report = running_large.finish();
    assert_eq!(small_report.first_cgroup, relative(small.path()));
    assert_eq!(large_report.first_cgroup, relative(large.path()));
    assert_accounted(&small, &small_report, "concurrent small");
    assert_accounted(&large, &large_report, "concurrent large");
}

/// A reused host's idle work before a job is its own: here the spawner itself burns before it
/// starts the job's first process, and none of that reaches the job.
fn a_spawners_earlier_work_is_not_charged(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(4)).expect("admit");
    let before = cpu_us(libc::RUSAGE_SELF);
    burn(300);
    let spawner_burned = cpu_us(libc::RUSAGE_SELF) - before;
    let report = Spawned::placed(
        &job,
        &Workload {
            burn_before_ms: 50,
            ..Workload::default()
        },
    )
    .finish();
    let usage = usage_us(&job);
    println!(
        "spawner burned {spawner_burned} us; the job's cgroup read {usage} us for its own {} us",
        report.total_cpu_us()
    );
    assert_accounted(&job, &report, "spawner");
}

/// The control: a process that ran before it was moved into the job's cgroup took its first
/// CPU with it, and the job's counters miss it. Admission must place before execution.
fn late_migration_misses_initial_cpu(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(5)).expect("admit");
    let mut late = Spawned::unplaced(&Workload {
        burn_before_ms: 300,
        pause: true,
        burn_after_ms: 100,
        ..Workload::default()
    });
    late.await_pause();
    let procs = job.path().join("cgroup.procs");
    std::fs::write(&procs, late.child.id().to_string())
        .unwrap_or_else(|error| panic!("migrate into {}: {error}", procs.display()));
    late.resume();
    let report = late.finish();
    assert_eq!(
        report.first_cgroup,
        relative(&own_cgroup_path()),
        "the control started outside the job"
    );
    assert_eq!(report.last_cgroup, relative(job.path()));
    let usage = usage_us(&job);
    let total = report.total_cpu_us();
    println!("late migration: cgroup {usage} us of the workload's own {total} us");
    assert!(
        usage + PLACEMENT_WINDOW_US < total,
        "a late-migrated workload must fail the accounting boundary: cgroup {usage} us, own \
         {total} us"
    );
    assert!(
        total - usage >= 250_000,
        "the control's 300 ms burn before migration is what the cgroup misses: cgroup {usage} us, \
         own {total} us"
    );
}

/// Children that burn CPU and are reaped between two looks at the job leave no live process to
/// sum, yet the job's `cpu.stat` keeps every microsecond of theirs; a concurrent unrelated job's
/// CPU never enters it.
fn burst_cpu_outlives_its_processes(jobs: &IncarnationCgroups) {
    let burst = jobs.admit(job_id(8)).expect("admit");
    let unrelated = jobs.admit(job_id(9)).expect("admit");
    let before = burst.cpu().expect("cpu before");
    let child = Workload {
        burn_before_ms: 40,
        ..Workload::default()
    };
    let neighbour = Spawned::placed(
        &unrelated,
        &Workload {
            burn_before_ms: 400,
            ..Workload::default()
        },
    );
    let mut running = Spawned::placed(
        &burst,
        &Workload {
            children: vec![child; 8],
            hold: true,
            ..Workload::default()
        },
    );
    running.await_marker(HELD);
    // The poll after the burst: only the parent is left, and it burned next to nothing itself.
    let live = live_members_cpu_us(&burst);
    let during = burst.cpu().expect("cpu after the burst");
    println!(
        "burst: cgroup {} us before, {} us after; live members hold {live} us",
        before.usage_us.get(),
        during.usage_us.get()
    );
    assert!(
        live + 250_000 < during.usage_us.get(),
        "a live-members-only sum must miss the reaped children's 320 ms: live {live} us, cgroup \
         {} us",
        during.usage_us.get()
    );
    running.release();
    let report = running.finish();
    let neighbour_report = neighbour.finish();
    assert!(
        report.children_cpu_us >= 8 * 40_000,
        "the children burned what they were told: {} us",
        report.children_cpu_us
    );
    assert_accounted(&burst, &report, "burst");
    assert_accounted(&unrelated, &neighbour_report, "burst neighbour");
}

/// A job that fills the page cache is charged for it while no process of it holds that memory
/// resident; anonymous memory it released leaves its mark on the peak, which reading never
/// resets.
fn charged_memory_is_not_resident_memory(jobs: &IncarnationCgroups) {
    const MIB: u64 = 1 << 20;
    let job = jobs.admit(job_id(10)).expect("admit");
    let file = on_block_storage("page-cache");
    let mut running = Spawned::placed(
        &job,
        &Workload {
            allocate_mib: 128,
            page_cache: Some((file, 64)),
            hold: true,
            ..Workload::default()
        },
    );
    running.await_marker(HELD);
    let direct_before = direct_charged_memory(&job);
    let charged = job.charged_memory().expect("charged memory");
    let direct_after = direct_charged_memory(&job);
    let peak_again = job
        .charged_memory()
        .expect("charged memory again")
        .peak_bytes;
    let resident = live_members_resident_bytes(&job);
    let (current, peak) = (charged.current_bytes.get(), charged.peak_bytes.get());
    println!(
        "charged memory: current {current} B, peak {peak} B (direct before {direct_before:?}, \
         after {direct_after:?}); live members resident {resident} B"
    );
    assert!(
        (direct_before.1..=direct_after.1).contains(&peak),
        "the reader's peak lies between two direct reads around it"
    );
    let (low, high) = (
        direct_before.0.min(direct_after.0),
        direct_before.0.max(direct_after.0),
    );
    assert!(
        (low.saturating_sub(4 * MIB)..=high + 4 * MIB).contains(&current),
        "the reader's current charge {current} B lies near two direct reads around it"
    );
    assert!(
        current >= resident + 48 * MIB,
        "the 64 MiB of page cache is charged though nothing holds it resident: charged {current} \
         B, resident {resident} B"
    );
    assert!(
        peak >= 128 * MIB && peak >= current + 32 * MIB,
        "the released 128 MiB stays in the peak: peak {peak} B, current {current} B"
    );
    assert!(
        peak_again.get() >= peak,
        "reading the peak never resets it: {peak} B, then {} B",
        peak_again.get()
    );
    running.release();
    running.finish();
    let terminal = job.terminal().expect("its last process was reaped");
    let ended = terminal.charged_memory().expect("final charged memory");
    assert!(
        ended.peak_bytes.get() >= peak,
        "the peak outlives the job's processes"
    );
}

/// Children that move bytes to and from storage past the page cache, then exit before anyone
/// looks, leave their I/O in the job's `io.stat`: a census of live members finds none of it, a
/// volume's allocation is no transfer, a read the cache served is none, and a concurrent job's
/// writes stay its own.
fn storage_io_outlives_its_processes(jobs: &IncarnationCgroups) {
    const MIB: u64 = 1 << 20;
    let job = jobs.admit(job_id(11)).expect("admit");
    let neighbour_job = jobs.admit(job_id(12)).expect("admit");
    let scratch = on_block_storage("probe");
    println!(
        "storage I/O scratch {} on filesystem type {:#x}",
        scratch.display(),
        filesystem_type(scratch.parent().expect("a directory")).expect("statfs")
    );
    // Cached by the controller, outside every job: the job's read of it is served from memory.
    let cached = on_block_storage("cached");
    fill_page_cache(&cached, 32);
    std::fs::File::open(&cached)
        .and_then(|file| file.sync_all())
        .expect("flush the cached file");
    read_through_cache(&cached);
    let allocated = on_block_storage("allocated");
    let children = (0..3)
        .map(|index| Workload {
            direct_io: Some((on_block_storage(&format!("direct-{index}")), 8, 4)),
            ..Workload::default()
        })
        .collect();
    let neighbour = Spawned::placed(
        &neighbour_job,
        &Workload {
            direct_io: Some((on_block_storage("neighbour"), 16, 0)),
            ..Workload::default()
        },
    );
    let mut running = Spawned::placed(
        &job,
        &Workload {
            cached_read: Some(cached.clone()),
            allocate_file: Some((allocated.clone(), 64)),
            children,
            hold: true,
            ..Workload::default()
        },
    );
    running.await_marker(HELD);
    neighbour.finish();
    let io = job.storage_io().expect("the job's io.stat");
    let raw = std::fs::read_to_string(job.path().join("io.stat")).expect("read io.stat");
    let (raw_read, raw_written) = raw_io_stat_sum(&raw);
    let live = live_members_storage_io_bytes(&job);
    let allocated_bytes = {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::metadata(&allocated)
            .expect("allocated file")
            .blocks()
            * 512
    };
    let (read, written) = (io.read_bytes.get(), io.write_bytes.get());
    println!(
        "storage I/O: read {read} B, written {written} B; io.stat {raw:?} sums to read \
         {raw_read} B, written {raw_written} B; live members {live} B; allocated {allocated_bytes} B"
    );
    assert!(
        read <= raw_read && written <= raw_written,
        "aggregation only ever leaves stacked devices out"
    );
    assert!(
        written >= 24 * MIB && read >= 12 * MIB,
        "three children wrote 8 MiB and read 4 MiB each past the cache: read {read} B, written \
         {written} B"
    );
    assert!(
        written < 24 * MIB + 8 * MIB,
        "neither the neighbour's 16 MiB nor the 64 MiB allocation is this job's writing: \
         {written} B"
    );
    assert!(
        read < 12 * MIB + 8 * MIB,
        "the 32 MiB the cache served is no storage read: {read} B"
    );
    assert!(
        allocated_bytes >= 64 * MIB && allocated_bytes > written + 16 * MIB,
        "a volume-allocation proxy would report {allocated_bytes} B for {written} B written"
    );
    assert!(
        live + 16 * MIB < read + written,
        "a live-members census misses the reaped children's I/O: live {live} B"
    );
    running.release();
    running.finish();
    let neighbour_io = neighbour_job.storage_io().expect("the neighbour's io.stat");
    assert!(
        neighbour_io.write_bytes.get() >= 16 * MIB,
        "the neighbour's own writes are its own: {:?}",
        neighbour_io
    );
    std::fs::remove_file(&cached).expect("remove the cached file");
}

fn retirement_waits_for_the_last_process(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(6)).expect("admit");
    let identity = job.identity().clone();
    let mut running = Spawned::placed(
        &job,
        &Workload {
            pause: true,
            ..Workload::default()
        },
    );
    running.await_pause();
    let refused = job.terminal().expect_err("a live job is not terminal");
    assert!(
        matches!(refused.error, CgroupError::Populated { .. }),
        "{:?}",
        refused.error
    );
    let job = refused.cgroup;
    running.resume();
    running.finish();
    let terminal = job.terminal().expect("its last process was reaped");
    let path = terminal.path().to_path_buf();
    let final_cpu = terminal.cpu().expect("final cpu");
    let totals = terminal.retire().expect("retire");
    assert_eq!(
        totals.cpu, final_cpu,
        "retirement collects the final counters"
    );
    assert!(!path.exists(), "{} was retired", path.display());
    assert!(matches!(
        jobs.lookup(&identity),
        Err(CgroupError::Missing { .. })
    ));
}

fn restart_lookup_keeps_the_identity_under_its_incarnation(
    authority: &CgroupAuthority,
    jobs: &IncarnationCgroups,
) {
    let identity = jobs.admit(job_id(7)).expect("admit").identity().clone();
    // A restarted controller reaches the same incarnation afresh.
    let restarted = authority
        .incarnation(jobs.incarnation())
        .expect("incarnation again");
    let found = restarted.lookup(&identity).expect("lookup by identity");
    assert_eq!(found.identity(), &identity);
    let outstanding: Vec<u64> = restarted
        .outstanding()
        .expect("outstanding")
        .iter()
        .map(|identity| identity.job_id().get())
        .collect();
    assert_eq!(
        outstanding,
        [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12],
        "job 6 was retired"
    );

    let other = authority.incarnation(&incarnation(2)).expect("incarnation");
    assert!(matches!(
        other.lookup(&identity),
        Err(CgroupError::ForeignIncarnation { .. })
    ));
    assert!(other.outstanding().expect("outstanding").is_empty());
    let forged = JobCgroupIdentity::recorded(
        identity.incarnation().clone(),
        identity.job_id(),
        identity.cgroup_id() + 1,
    );
    assert!(matches!(
        restarted.lookup(&forged),
        Err(CgroupError::Replaced { .. })
    ));
}

// ---------------------------------------------------------------------------------------------
// Workload processes

struct Spawned {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Spawned {
    fn placed(job: &JobCgroup, workload: &Workload) -> Self {
        let mut command = workload_command(workload);
        job.placement().expect("placement").on_exec(&mut command);
        Self::spawn(command)
    }

    fn unplaced(workload: &Workload) -> Self {
        Self::spawn(workload_command(workload))
    }

    fn spawn(mut command: Command) -> Self {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn_locked()
            .expect("spawn a workload");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn await_pause(&mut self) {
        self.await_marker(PAUSED);
    }

    fn await_marker(&mut self, marker: &str) {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self.stdout.read_line(&mut line).expect("workload output");
            assert_ne!(read, 0, "the workload ended before it reported {marker}");
            if line.trim_end() == marker {
                return;
            }
        }
    }

    fn resume(&mut self) {
        self.stdin.write_all(b"r").expect("resume the workload");
    }

    fn release(&mut self) {
        self.stdin.write_all(b"r").expect("release the workload");
    }

    fn finish(mut self) -> WorkloadReport {
        let mut rest = String::new();
        self.stdout
            .read_to_string(&mut rest)
            .expect("workload output");
        let status = self.child.wait().expect("wait for the workload");
        assert!(status.success(), "workload: {status}\n{rest}");
        report_in(&rest)
    }
}

fn workload_command(workload: &Workload) -> Command {
    let mut command = Command::new(test_binary());
    command.args(role_arguments()).env(
        ROLE,
        serde_json::to_string(&Role::Workload(workload.clone())).expect("role"),
    );
    command
}

fn report_in(output: &str) -> WorkloadReport {
    let line = output
        .lines()
        .find_map(|line| line.strip_prefix(REPORT))
        .unwrap_or_else(|| panic!("no workload report in {output:?}"));
    serde_json::from_str(line).expect("workload report")
}

// ---------------------------------------------------------------------------------------------
// Kernel reads

/// The job's cgroup accounts at least the workload's own CPU, less the fork-to-placement window,
/// and at most that CPU: nothing outside the workload's tree is charged to it. The reader agrees
/// with an independent read of the same final counters, and its user and system split the usage.
fn assert_accounted(job: &JobCgroup, report: &WorkloadReport, scenario: &str) {
    let cpu = job.cpu().expect("the job's cpu.stat");
    let usage = usage_us(job);
    assert_eq!(
        cpu.usage_us.get(),
        usage,
        "{scenario}: the reader and an independent read of final counters differ"
    );
    let split = cpu.user_us.get() + cpu.system_us.get();
    assert!(
        split.abs_diff(usage) <= 2,
        "{scenario}: user {} us + system {} us does not split usage {usage} us",
        cpu.user_us.get(),
        cpu.system_us.get()
    );
    let total = report.total_cpu_us();
    println!(
        "{scenario}: cgroup {usage} us (user {} us, system {} us), workload's own {total} us, \
         deficit {} us",
        cpu.user_us.get(),
        cpu.system_us.get(),
        i128::from(total) - i128::from(usage)
    );
    assert!(
        usage + PLACEMENT_WINDOW_US >= total,
        "{scenario}: the job's cgroup misses its workload's CPU: cgroup {usage} us, own {total} us"
    );
    assert!(
        usage <= total + 1_000,
        "{scenario}: the job's cgroup holds CPU its workload never spent: cgroup {usage} us, own \
         {total} us"
    );
}

/// What a census of the job's live members holds resident, from `/proc/<pid>/statm`.
fn live_members_resident_bytes(job: &JobCgroup) -> u64 {
    // SAFETY: sysconf takes a constant.
    let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).expect("page size");
    let procs = job.path().join("cgroup.procs");
    std::fs::read_to_string(&procs)
        .unwrap_or_else(|error| panic!("read {}: {error}", procs.display()))
        .lines()
        .filter_map(|pid| {
            let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
            let pages: u64 = statm.split_whitespace().nth(1)?.parse().expect("resident");
            Some(pages * page)
        })
        .sum()
}

/// `memory.current` and `memory.peak`, read directly.
fn direct_charged_memory(job: &JobCgroup) -> (u64, u64) {
    let read = |name: &str| -> u64 {
        let path = job.path().join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
            .trim()
            .parse()
            .expect("a count")
    };
    (read("memory.current"), read("memory.peak"))
}

/// What a census of the job's live members finds: each one's own user + system CPU from
/// `/proc/<pid>/stat`, summed. Reaped processes are in no census.
fn live_members_cpu_us(job: &JobCgroup) -> u64 {
    // SAFETY: sysconf takes a constant.
    let ticks = u64::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).expect("clock ticks");
    let procs = job.path().join("cgroup.procs");
    std::fs::read_to_string(&procs)
        .unwrap_or_else(|error| panic!("read {}: {error}", procs.display()))
        .lines()
        .filter_map(|pid| {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            let after = &stat[stat.rfind(')')? + 2..];
            let fields: Vec<&str> = after.split_whitespace().collect();
            // utime and stime are fields 14 and 15 of stat(5); `after` starts at field 3.
            let utime: u64 = fields[11].parse().expect("utime");
            let stime: u64 = fields[12].parse().expect("stime");
            Some((utime + stime) * 1_000_000 / ticks)
        })
        .sum()
}

/// `usage_usec` of the job's `cpu.stat`, read directly.
fn usage_us(job: &JobCgroup) -> u64 {
    let path = job.path().join("cpu.stat");
    let stat = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    stat.lines()
        .find_map(|line| line.strip_prefix("usage_usec "))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("no usage_usec in {stat:?}"))
}

/// This process's own (or its reaped descendants') user + system CPU, in microseconds.
fn cpu_us(who: libc::c_int) -> u64 {
    // SAFETY: an all-zero rusage is a valid output buffer.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: a valid `who` and a buffer of the right type.
    assert_eq!(
        unsafe { libc::getrusage(who, &mut usage) },
        0,
        "getrusage: {}",
        last_error()
    );
    let micros = |time: libc::timeval| {
        u64::try_from(time.tv_sec).expect("seconds") * 1_000_000
            + u64::try_from(time.tv_usec).expect("microseconds")
    };
    micros(usage.ru_utime) + micros(usage.ru_stime)
}

/// Touch `mebibytes` of fresh anonymous memory page by page, then release it.
fn touch_and_release(mebibytes: u64) {
    if mebibytes == 0 {
        return;
    }
    let length = usize::try_from(mebibytes << 20).expect("length");
    // SAFETY: a fresh private anonymous mapping.
    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            length,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(region, libc::MAP_FAILED, "mmap: {}", last_error());
    for offset in (0..length).step_by(4096) {
        // SAFETY: within the writable mapping made above.
        unsafe { region.cast::<u8>().add(offset).write_volatile(1) };
    }
    // SAFETY: the whole mapping made above, unmapped once.
    assert_eq!(unsafe { libc::munmap(region, length) }, 0, "munmap");
}

/// Write `mebibytes` to `path` through the page cache, from one reused 1 MiB buffer.
fn fill_page_cache(path: &Path, mebibytes: u64) {
    let mut file = std::fs::File::create(path)
        .unwrap_or_else(|error| panic!("create {}: {error}", path.display()));
    let chunk = vec![0x5a_u8; 1 << 20];
    for _ in 0..mebibytes {
        file.write_all(&chunk)
            .expect("write through the page cache");
    }
}

/// Filesystems whose file pages the memory controller charges to the cgroup that faults them in
/// and whose writeback and direct I/O reach a block device as that cgroup's own bios, so
/// `memory.current` and `io.stat` see them. ZFS is not one: its ARC is no page cache, and its
/// own threads issue its I/O.
const BLOCK_FILESYSTEMS: [(libc::c_long, &str); 3] = [
    (0xEF53, "ext4"),
    (0x5846_5342, "xfs"),
    (0x9123_683E, "btrfs"),
];

/// A scratch path in the block-storage directory the harness chose.
fn on_block_storage(name: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(STORAGE).expect("the harness's storage directory"))
        .join(format!("job-cgroup-{}-{name}", std::process::id()))
}

/// The first writable directory among the build's and the user's usual scratch places that lies
/// on a [`BLOCK_FILESYSTEMS`] filesystem. None is a failure that lists what each candidate is and
/// every mount, never a skipped scenario.
fn block_storage_directory() -> PathBuf {
    let mut candidates = vec![
        test_binary()
            .parent()
            .expect("the test binary's directory")
            .to_path_buf(),
    ];
    candidates.extend(
        ["TMPDIR", "RUNNER_TEMP", "HOME"]
            .into_iter()
            .filter_map(std::env::var_os)
            .map(PathBuf::from),
    );
    candidates.extend(["/var/tmp", "/tmp"].map(PathBuf::from));
    let mut measured = Vec::new();
    for candidate in candidates {
        let f_type = filesystem_type(&candidate);
        let writable = writable_directory(&candidate);
        let block = f_type.as_ref().ok().and_then(|f_type| {
            BLOCK_FILESYSTEMS
                .iter()
                .find(|(magic, _)| magic == f_type)
                .map(|(_, name)| *name)
        });
        if let (Some(name), true) = (block, writable) {
            println!("block storage: {} on {name}", candidate.display());
            return candidate;
        }
        measured.push(format!(
            "{}: f_type {f_type:x?}, writable {writable}",
            candidate.display()
        ));
    }
    let mounts = std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap_or_else(|error| format!("unreadable: {error}"))
        .lines()
        .filter_map(|line| {
            let (before, after) = line.split_once(" - ")?;
            let mount = before.split_whitespace().nth(4)?;
            let mut after = after.split_whitespace();
            Some(format!(
                "{mount} {} {}",
                after.next()?,
                after.next().unwrap_or("")
            ))
        })
        .collect::<Vec<_>>()
        .join("\n");
    panic!(
        "no writable scratch directory on ext4, xfs or btrfs:\n{}\nmounts:\n{mounts}",
        measured.join("\n")
    );
}

fn writable_directory(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt as _;

    let Ok(name) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: a NUL-terminated path and plain flags.
    unsafe { libc::access(name.as_ptr(), libc::W_OK | libc::X_OK) == 0 }
}

/// Write `write` MiB to a new file at `path`, flushed to storage, then read `read` MiB of it
/// back, every transfer past the page cache (`O_DIRECT`) from one page-aligned buffer.
fn uncached_io(path: &Path, write: u64, read: u64) {
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::ffi::OsStrExt as _;

    const CHUNK: usize = 1 << 20;
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("a path");
    let open = |flags: libc::c_int| -> OwnedFd {
        // SAFETY: a NUL-terminated path and plain flags.
        let fd = unsafe {
            libc::open(
                name.as_ptr(),
                flags | libc::O_DIRECT | libc::O_CLOEXEC,
                0o600,
            )
        };
        assert!(fd >= 0, "open {}: {}", path.display(), last_error());
        // SAFETY: a new descriptor this workload owns.
        unsafe { OwnedFd::from_raw_fd(fd) }
    };
    // SAFETY: a fresh private anonymous mapping, page-aligned by construction.
    let buffer = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            CHUNK,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(buffer, libc::MAP_FAILED, "mmap: {}", last_error());
    // SAFETY: the whole writable mapping made above.
    unsafe { std::ptr::write_bytes(buffer.cast::<u8>(), 0x5a, CHUNK) };
    let file = open(libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC);
    for _ in 0..write {
        // SAFETY: CHUNK readable bytes of the mapping.
        let written = unsafe { libc::write(file.as_raw_fd(), buffer, CHUNK) };
        assert_eq!(
            usize::try_from(written).ok(),
            Some(CHUNK),
            "write: {}",
            last_error()
        );
    }
    // SAFETY: a live descriptor.
    assert_eq!(
        unsafe { libc::fsync(file.as_raw_fd()) },
        0,
        "fsync: {}",
        last_error()
    );
    drop(file);
    let file = open(libc::O_RDONLY);
    for _ in 0..read {
        // SAFETY: CHUNK writable bytes of the mapping.
        let got = unsafe { libc::read(file.as_raw_fd(), buffer, CHUNK) };
        assert_eq!(
            usize::try_from(got).ok(),
            Some(CHUNK),
            "read: {}",
            last_error()
        );
    }
    // SAFETY: the whole mapping made above, unmapped once.
    assert_eq!(unsafe { libc::munmap(buffer, CHUNK) }, 0, "munmap");
}

/// Read `path` whole through the page cache, into one reused buffer.
fn read_through_cache(path: &Path) {
    let mut file = std::fs::File::open(path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    let mut buffer = vec![0_u8; 1 << 20];
    while file.read(&mut buffer).expect("read through the cache") > 0 {}
}

/// Give `path` `mebibytes` of storage without writing a byte of it.
fn allocate(path: &Path, mebibytes: u64) {
    use std::os::fd::AsRawFd as _;

    let file = std::fs::File::create(path)
        .unwrap_or_else(|error| panic!("create {}: {error}", path.display()));
    let length = libc::off_t::try_from(mebibytes << 20).expect("length");
    // SAFETY: a live descriptor and plain integers.
    let allocated = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    assert_eq!(allocated, 0, "posix_fallocate: errno {allocated}");
}

/// `statfs(2)`'s `f_type` of the filesystem holding `path`.
fn filesystem_type(path: &Path) -> std::io::Result<libc::c_long> {
    use std::os::unix::ffi::OsStrExt as _;

    let name = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: an all-zero statfs is a valid output buffer.
    let mut statfs: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: a NUL-terminated path and a buffer of the right type.
    if unsafe { libc::statfs(name.as_ptr(), &mut statfs) } != 0 {
        return Err(last_error());
    }
    Ok(statfs.f_type)
}

/// Every device line's `rbytes` and `wbytes` of `io.stat`, summed without regard to stacking.
fn raw_io_stat_sum(stat: &str) -> (u64, u64) {
    let counter = |line: &str, name: &str| -> u64 {
        line.split_whitespace()
            .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no {name} in {line:?}"))
    };
    stat.lines()
        .filter(|line| !line.trim().is_empty())
        .fold((0, 0), |(read, written), line| {
            (
                read + counter(line, "rbytes"),
                written + counter(line, "wbytes"),
            )
        })
}

/// What a census of the job's live members finds in their own `/proc/<pid>/io` storage counters.
fn live_members_storage_io_bytes(job: &JobCgroup) -> u64 {
    let procs = job.path().join("cgroup.procs");
    std::fs::read_to_string(&procs)
        .unwrap_or_else(|error| panic!("read {}: {error}", procs.display()))
        .lines()
        .filter_map(|pid| {
            let io = std::fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
            Some(
                io.lines()
                    .filter_map(|line| {
                        let (key, value) = line.split_once(": ")?;
                        matches!(key, "read_bytes" | "write_bytes")
                            .then(|| value.parse::<u64>().expect("a count"))
                    })
                    .sum::<u64>(),
            )
        })
        .sum()
}

/// Spend `millis` of this process's own CPU, as `getrusage` measures it.
fn burn(millis: u64) {
    let until = cpu_us(libc::RUSAGE_SELF) + millis * 1_000;
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    while cpu_us(libc::RUSAGE_SELF) < until {
        for _ in 0..10_000 {
            state = state.rotate_left(5) ^ state.wrapping_mul(0x5851_f42d_4c95_7f2d);
        }
        std::hint::black_box(state);
    }
}

/// This process's cgroup v2 path, as `/proc/self/cgroup` names it.
fn own_cgroup() -> String {
    let content = std::fs::read_to_string("/proc/self/cgroup").expect("read /proc/self/cgroup");
    content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or_else(|| panic!("no cgroup v2 line in {content:?}"))
        .to_owned()
}

fn own_cgroup_path() -> PathBuf {
    Path::new(CGROUP_MOUNT).join(own_cgroup().trim_start_matches('/'))
}

/// A cgroup directory as `/proc/<pid>/cgroup` names it.
fn relative(path: &Path) -> String {
    format!(
        "/{}",
        path.strip_prefix(CGROUP_MOUNT)
            .expect("a cgroup beneath the mount")
            .display()
    )
}

// ---------------------------------------------------------------------------------------------
// Fixture plumbing

fn incarnation(ordinal: u8) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(format!(
        "{:08x}{:022x}{ordinal:02x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
            & ((1 << 88) - 1)
    ))
    .expect("incarnation")
}

fn job_id(value: u64) -> JobId {
    JobId::new(value).expect("job id")
}

fn test_binary() -> PathBuf {
    std::env::current_exe().expect("test binary")
}

fn role_arguments() -> [&'static str; 4] {
    ["--exact", ROLE_TEST, "--nocapture", "--quiet"]
}

fn find_program(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} on PATH"))
}

fn last_error() -> std::io::Error {
    std::io::Error::last_os_error()
}
