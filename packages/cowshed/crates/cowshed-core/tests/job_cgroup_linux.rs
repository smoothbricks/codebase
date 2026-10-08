//! A job owns its cgroup v2 from before its first instruction (07_api.md, "Complete job
//! accounting and observation reconciliation"), proven with native processes inside a cgroup
//! delegated to an unprivileged controller.
//!
//! The harness asks `sudo -n systemd-run --scope -p Delegate=yes` for a fresh delegated scope and
//! re-executes this test binary there as a root parent that delegates the scope to the invoking
//! user and owns scratch cleanup. The unprivileged controller then takes authority with
//! [`CgroupAuthority::delegated`] and runs workloads: this binary again, whose first action
//! reports its cgroup. It burns CPU measured by `getrusage` and may start children. The controller
//! collects each selected child's complete lifetime with `wait4`, including report/exit overhead,
//! and compares it with an independent final `cpu.stat` read.
//!
//! Workloads keep their files on test-owned scratch storage prepared by the root Delegate phase
//! and removed on success or unwind ([`Scratch`]): a sparse image formatted ext4 and loop-mounted.
//! `memory.stat` distinguishes regular-file cache from shmem, and `io.stat` counts only bios a
//! block device received. ZFS ARC is neither proof. Without loop devices, tmpfs measures anonymous
//! and shmem charges only; regular-file page-cache and block-I/O attribution remain blocked.
//! A host without passwordless sudo, systemd, a delegable cgroup v2, or (given loop devices)
//! `mkfs.ext4` fails naming the refused step; nothing is skipped.

#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::os::unix::fs::chown;
use std::os::unix::process::ExitStatusExt as _;
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
/// The directory workloads keep their files in ([`Scratch`]).
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
    /// A test-owned scratch file filled through the page cache and held until workload exit.
    page_cache: Option<(PathBuf, u64)>,
    /// A test-owned scratch file written/read past the page cache (`O_DIRECT`) and flushed.
    direct_io: Option<(PathBuf, u64, u64)>,
    /// A file the page cache already holds, read whole through it.
    cached_read: Option<PathBuf>,
    /// A file to allocate this many MiB to without writing it, held until the workload ends.
    allocate_file: Option<(PathBuf, u64)>,
    /// Started one after another once the burns are done, each waited for.
    children: Vec<Workload>,
    /// Once every child was reaped, report [`HELD`] and wait for one byte on stdin.
    hold: bool,
    /// CPU deliberately spent after the self-report, for the complete-lifetime oracle control.
    burn_after_report_ms: u64,
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

/// The selected child's complete lifetime, collected on reap, separate from its pre-exit report.
struct WorkloadExit {
    report: WorkloadReport,
    cpu_us: u64,
}

#[test]
fn workload_exit_cpu_includes_work_after_its_report() {
    let exit = Spawned::unplaced(&Workload {
        burn_after_report_ms: 20,
        ..Workload::default()
    })
    .finish();
    let reported = exit.report.total_cpu_us();
    println!(
        "CPU oracle control: pre-report {reported} us; wait4 lifetime {} us",
        exit.cpu_us
    );
    assert!(
        exit.cpu_us >= reported + 20_000,
        "a pre-report oracle omits the deliberate 20 ms of post-report CPU"
    );
}

/// The harness: everything else runs in the delegated scope.
#[test]
fn a_job_owns_its_cgroup_from_before_its_first_instruction() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    // SAFETY: getuid and getgid cannot fail.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    assert_ne!(
        uid, 0,
        "run the harness as the ordinary runner, not as its root setup role"
    );
    let delegate = serde_json::to_string(&Role::Delegate { uid, gid }).expect("role");
    let mut assignment = OsString::from(format!("{ROLE}="));
    assignment.push(delegate);
    // sudo does not preserve PATH: root needs the same declared test tools as the runner.
    let mut search_path = OsString::from("PATH=");
    search_path.push(std::env::var_os("PATH").expect("PATH"));
    let output = sudo("env")
        .arg(assignment)
        .arg(search_path)
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

/// Delegate the scope to `uid`/`gid`; retain a root parent solely to own scratch cleanup.
fn delegate(uid: u32, gid: u32) {
    report_fixture_authority("setup");
    // SAFETY: geteuid cannot fail; sudo grants guest fixture setup, not host capabilities.
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "the setup parent requires scoped guest root"
    );
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
    // The waiting root parent must not populate the distributing scope. Both it and the
    // controller stay in this non-job leaf; only admitted workloads enter job cgroups.
    let leaf = scope.join(CONTROLLER_LEAF);
    std::fs::create_dir(&leaf).expect("create the controller leaf");
    for file in ["", "cgroup.procs", "cgroup.threads"] {
        chown(leaf.join(file), Some(uid), Some(gid)).expect("delegate the controller leaf");
    }
    std::fs::write(leaf.join("cgroup.procs"), "0").expect("park the root parent in the leaf");
    // Keep this parent root until the controller ends: it owns the mount even when a scenario
    // panics or the controller cannot be spawned. Only the controller drops privilege.
    scratch_cleanup_survives_failure(uid, gid);
    let mut scratch = Scratch::make(uid, gid);
    let mut command = Command::new(find_program("setpriv"));
    command
        .arg("--reuid")
        .arg(uid.to_string())
        .arg("--regid")
        .arg(gid.to_string())
        .args(["--clear-groups", "--"])
        .arg(test_binary())
        .args(role_arguments())
        .env(
            ROLE,
            serde_json::to_string(&Role::Controller).expect("role"),
        )
        .env(STORAGE, &scratch.directory);
    let output = command
        .output_locked()
        .expect("run the unprivileged controller");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    scratch
        .cleanup()
        .expect("release scratch after the controller exits");
    assert!(output.status.success(), "controller: {}", output.status);
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
        // Flush unlink metadata before this child can be reaped and its parent can report HELD.
        std::fs::File::open(path.parent().expect("direct I/O file directory"))
            .and_then(|directory| directory.sync_all())
            .expect("flush the direct I/O deletion metadata");
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
        // Finish directory metadata before the parent reports/exits, making final io.stat
        // comparisons quiescent without a timing wait.
        std::fs::File::open(path.parent().expect("allocated file directory"))
            .and_then(|directory| directory.sync_all())
            .expect("flush the allocation/deletion metadata");
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
    if workload.burn_after_report_ms != 0 {
        burn(workload.burn_after_report_ms);
    }
}

/// The scenarios, run by the unprivileged controller that holds the delegated scope.
fn control() {
    report_fixture_authority("controller");
    // This controller receives only the runner-owned mountpoint, never device authority.
    let denied = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/loop-control");
    assert!(
        matches!(denied, Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied),
        "the unprivileged controller must not open the root-only loop control"
    );
    println!("fixture authority: ordinary controller loop-control open denied");
    let own = own_cgroup_path();
    assert_eq!(own.file_name(), Some(std::ffi::OsStr::new(CONTROLLER_LEAF)));
    let scope = own.parent().expect("the delegated scope");
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
    let exit = Spawned::placed(
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
        exit.report.first_cgroup, path,
        "the first instruction ran in the job"
    );
    for child in &exit.report.children {
        assert_eq!(child.first_cgroup, path, "a descendant is born in the job");
    }
    assert_accounted(&job, &exit, "inheritance");
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
    let small_exit = running_small.finish();
    let large_exit = running_large.finish();
    assert_eq!(small_exit.report.first_cgroup, relative(small.path()));
    assert_eq!(large_exit.report.first_cgroup, relative(large.path()));
    assert_accounted(&small, &small_exit, "concurrent small");
    assert_accounted(&large, &large_exit, "concurrent large");
}

/// A reused host's idle work before a job is its own: here the spawner itself burns before it
/// starts the job's first process, and none of that reaches the job.
fn a_spawners_earlier_work_is_not_charged(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(4)).expect("admit");
    let before = cpu_us(libc::RUSAGE_SELF);
    burn(300);
    let spawner_burned = cpu_us(libc::RUSAGE_SELF) - before;
    let exit = Spawned::placed(
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
        exit.cpu_us
    );
    assert_accounted(&job, &exit, "spawner");
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
    let exit = late.finish();
    assert_eq!(
        exit.report.first_cgroup,
        relative(&own_cgroup_path()),
        "the control started outside the job"
    );
    assert_eq!(exit.report.last_cgroup, relative(job.path()));
    let usage = usage_us(&job);
    let total = exit.cpu_us;
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
    let live = live_members_cpu_us(&burst, &running).expect("read the held parent's actual CPU");
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
    let exit = running.finish();
    let neighbour_exit = neighbour.finish();
    assert!(
        exit.report.children_cpu_us >= 8 * 40_000,
        "the children burned what they were told: {} us",
        exit.report.children_cpu_us
    );
    assert_accounted(&burst, &exit, "burst");
    assert_accounted(&unrelated, &neighbour_exit, "burst neighbour");
}

/// A job that fills the page cache is charged for it while no process of it holds that memory
/// resident; anonymous memory it released leaves its mark on the peak, which reading never
/// resets.
fn charged_memory_is_not_resident_memory(jobs: &IncarnationCgroups) {
    const MIB: u64 = 1 << 20;
    let job = jobs.admit(job_id(10)).expect("admit");
    let file = in_scratch("page-cache");
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
    let resident =
        live_members_resident_bytes(&job, &running).expect("read the held parent's actual RSS");
    let (current, peak) = (charged.current_bytes.get(), charged.peak_bytes.get());
    let stat = std::fs::read_to_string(job.path().join("memory.stat")).expect("memory.stat");
    let counter = |name: &str| -> u64 {
        stat.lines()
            .find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == name).then(|| value.parse().expect("memory.stat count"))
            })
            .unwrap_or_else(|| panic!("memory.stat has no {name}: {stat}"))
    };
    let (file, shmem) = (counter("file"), counter("shmem"));
    let f_type = filesystem_type(&scratch_directory()).expect("scratch statfs");
    let regular_file = file.checked_sub(shmem).expect("shmem is part of file");
    println!(
        "charged memory: current {current} B, peak {peak} B (direct before {direct_before:?}, \
         after {direct_after:?}); live members resident {resident} B; memory.stat file {file} B, \
         shmem {shmem} B, regular-file cache {regular_file} B; filesystem {f_type:#x}"
    );
    if f_type == libc::EXT4_SUPER_MAGIC {
        assert!(
            regular_file >= 64 * MIB,
            "the 64 MiB regular file is charged as non-shmem cache"
        );
    } else {
        assert_eq!(
            f_type,
            libc::TMPFS_MAGIC,
            "only ext4 or measured tmpfs scratch"
        );
        assert!(shmem >= 64 * MIB, "the tmpfs file is charged as shmem");
        println!(
            "blocked: tmpfs proves anonymous/shmem charging, not regular-file page-cache attribution"
        );
    }
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
        "the 64 MiB of file-backed memory is charged though nothing holds it resident: charged {current} \
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
    let directory = scratch_directory();
    let f_type = filesystem_type(&directory).expect("statfs");
    assert_eq!(
        f_type,
        libc::EXT4_SUPER_MAGIC,
        "blocked: storage I/O reaches io.stat only as bios a block device received, and the \
         scratch {} is filesystem type {f_type:#x}, which no loop device backs on this host",
        directory.display()
    );
    let job = jobs.admit(job_id(11)).expect("admit");
    let neighbour_job = jobs.admit(job_id(12)).expect("admit");
    // Cached by the controller, outside every job: the job's read of it is served from memory.
    let cached = in_scratch("cached");
    fill_page_cache(&cached, 32);
    std::fs::File::open(&cached)
        .and_then(|file| file.sync_all())
        .expect("flush the cached file");
    read_through_cache(&cached);
    let allocated = in_scratch("allocated");
    let children = (0..3)
        .map(|index| Workload {
            direct_io: Some((in_scratch(&format!("direct-{index}")), 8, 4)),
            ..Workload::default()
        })
        .collect();
    let neighbour = Spawned::placed(
        &neighbour_job,
        &Workload {
            direct_io: Some((in_scratch("neighbour"), 16, 0)),
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
    let held_members = held_parent_census(&job, &running).expect("the sole retained parent");
    let held_live = live_members_storage_io_bytes(&held_members).expect("both held I/O counters");
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
         {raw_read} B, written {raw_written} B; held live members (including waited-for children) \
         {held_live} B; allocated {allocated_bytes} B"
    );
    assert_eq!(
        (read, written),
        (raw_read, raw_written),
        "the test-owned loop ext4 totals equal an independent io.stat read"
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
    running.release();
    running.finish();
    // /proc/<pid>/io includes waited-for children. A held parent can retain those bytes;
    // the live-only proxy is incomplete only once the entire owned tree has ended.
    let terminal = job
        .terminal()
        .expect("the entire I/O workload tree was reaped");
    let terminal_members = std::fs::read_to_string(terminal.path().join("cgroup.procs"))
        .expect("read the proven empty terminal census");
    assert!(
        terminal_members.trim().is_empty(),
        "terminal group has no members"
    );
    let live = live_members_storage_io_bytes(&terminal_members).expect("terminal census I/O");
    let final_io = terminal.storage_io().expect("terminal io.stat");
    let final_raw = std::fs::read_to_string(terminal.path().join("io.stat"))
        .expect("independent terminal io.stat");
    let (final_read, final_written) = (final_io.read_bytes.get(), final_io.write_bytes.get());
    let (raw_read, raw_written) = raw_io_stat_sum(&final_raw);
    println!(
        "terminal storage I/O: read {final_read} B, written {final_written} B; \
         io.stat {final_raw:?} sums to read {raw_read} B, written {raw_written} B; \
         live members {live} B; earlier held census {held_live} B"
    );
    assert_eq!(
        (final_read, final_written),
        (raw_read, raw_written),
        "terminal totals equal the independent final io.stat read"
    );
    assert!(
        final_read >= read && final_written >= written,
        "the cgroup retains the reaped tree's earlier transfers"
    );
    assert!(
        final_written < 24 * MIB + 8 * MIB && final_read < 12 * MIB + 8 * MIB,
        "terminal totals still exclude the neighbour, allocation and cached-read proxies"
    );
    assert_eq!(
        live, 0,
        "no owned live process remains for the final census"
    );
    assert!(
        live + 16 * MIB < final_read + final_written,
        "the ended tree's retained I/O is absent from a live-only census"
    );
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

    fn finish(mut self) -> WorkloadExit {
        let mut rest = String::new();
        self.stdout
            .read_to_string(&mut rest)
            .expect("workload output");
        let (status, cpu_us) = reap_workload(&self.child).expect("reap the workload with rusage");
        assert!(status.success(), "workload: {status}\n{rest}");
        WorkloadExit {
            report: report_in(&rest),
            cpu_us,
        }
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

/// Reap only this child, collecting the same completed lifetime the final cgroup read covers.
/// Linux wait4 includes the child's own CPU and that of descendants it already reaped.
fn reap_workload(child: &Child) -> std::io::Result<(std::process::ExitStatus, u64)> {
    let pid = libc::pid_t::try_from(child.id()).expect("an owned child's pid fits pid_t");
    let mut status = 0;
    // SAFETY: an all-zero rusage is valid output storage.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    loop {
        // SAFETY: the selected child is owned by this caller and both output buffers are valid.
        let waited = unsafe { libc::wait4(pid, &mut status, 0, &mut usage) };
        if waited == pid {
            return Ok((
                std::process::ExitStatus::from_raw(status),
                rusage_cpu_us(&usage),
            ));
        }
        let error = last_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Kernel reads

/// Compare the job's final CPU to this selected child's independently reaped complete lifetime,
/// less only the fork-to-placement window. The pre-exit report is diagnostic, never that oracle.
/// The reader agrees with a direct final cpu.stat read, and its user/system counters split usage.
fn assert_accounted(job: &JobCgroup, exit: &WorkloadExit, scenario: &str) {
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
    let total = exit.cpu_us;
    println!(
        "{scenario}: cgroup {usage} us (user {} us, system {} us), wait4 lifetime {total} us, \
         pre-report {} us, deficit {} us",
        cpu.user_us.get(),
        cpu.system_us.get(),
        exit.report.total_cpu_us(),
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

/// HELD is emitted after every child has been reaped. The directly owned, unreaped parent
/// must still be the sole member: an empty or foreign census is not a zero-valued observation.
fn held_parent_census(job: &JobCgroup, running: &Spawned) -> Result<String, String> {
    let procs = std::fs::read_to_string(job.path().join("cgroup.procs"))
        .map_err(|error| format!("read held cgroup.procs: {error}"))?;
    let mut members = procs.lines();
    let pid: u32 = members
        .next()
        .ok_or_else(|| "HELD parent is absent from cgroup.procs".to_owned())?
        .parse()
        .map_err(|error| format!("held member PID: {error}"))?;
    if pid != running.child.id() || members.next().is_some() {
        return Err(format!(
            "held census {procs:?} is not sole owned parent {}",
            running.child.id()
        ));
    }
    Ok(procs)
}

/// What a census of the job's live members holds resident, from `/proc/<pid>/statm`.
fn live_members_resident_bytes(job: &JobCgroup, running: &Spawned) -> Result<u64, String> {
    // SAFETY: sysconf takes a constant.
    let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
        .map_err(|error| format!("page size: {error}"))?;
    let procs = held_parent_census(job, running)?;
    procs.lines().try_fold(0, |total, pid| {
        let path = format!("/proc/{pid}/statm");
        let statm =
            std::fs::read_to_string(&path).map_err(|error| format!("read {path}: {error}"))?;
        let pages: u64 = statm
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| format!("{path} has no resident field"))?
            .parse()
            .map_err(|error| format!("resident in {path}: {error}"))?;
        Ok(total + pages * page)
    })
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

/// Each live member's own user + system CPU from `/proc/<pid>/stat`. Every unavailable
/// record fails the observation; departure is not silently treated as zero.
fn live_members_cpu_us(job: &JobCgroup, running: &Spawned) -> Result<u64, String> {
    // SAFETY: sysconf takes a constant.
    let ticks = u64::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) })
        .map_err(|error| format!("clock ticks: {error}"))?;
    let procs = held_parent_census(job, running)?;
    procs.lines().try_fold(0, |total, pid| {
        let path = format!("/proc/{pid}/stat");
        let stat =
            std::fs::read_to_string(&path).map_err(|error| format!("read {path}: {error}"))?;
        let (_, after) = stat
            .rsplit_once(") ")
            .ok_or_else(|| format!("{path} has no command delimiter"))?;
        // utime/stime are fields 14/15; after starts at field 3.
        let mut fields = after.split_whitespace().skip(11);
        let mut counter = |name| -> Result<u64, String> {
            fields
                .next()
                .ok_or_else(|| format!("{path} has no {name}"))?
                .parse()
                .map_err(|error| format!("{name} in {path}: {error}"))
        };
        let utime = counter("utime")?;
        let stime = counter("stime")?;
        Ok(total + (utime + stime) * 1_000_000 / ticks)
    })
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
    rusage_cpu_us(&usage)
}

fn rusage_cpu_us(usage: &libc::rusage) -> u64 {
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

/// The scratch directory the harness made ([`Scratch`]).
fn scratch_directory() -> PathBuf {
    PathBuf::from(std::env::var_os(STORAGE).expect("the harness's scratch directory"))
}

/// A path for `name` in the [`scratch_directory`].
fn in_scratch(name: &str) -> PathBuf {
    scratch_directory().join(format!("job-cgroup-{}-{name}", std::process::id()))
}

/// The size of the sparse image backing [`Scratch`]: room for every scenario's files at once.
const SCRATCH_IMAGE_BYTES: u64 = 512 << 20;

/// One run's scratch storage, owned by the root Delegate parent and removed on normal exit or
/// unwind. Without usable loop devices, tmpfs proves shmem charging, not regular-file cache or I/O.
struct Scratch {
    /// Owned by this run alone: holds the image and its mountpoint, or is the tmpfs directory.
    root: PathBuf,
    /// Where workloads keep their files.
    directory: PathBuf,
    /// Cleanup is attempted once, explicitly on success and by Drop during unwind.
    cleanup_started: bool,
    /// The atomically configured loop association, with AUTOCLEAR; closing this last owned
    /// descriptor clears it after unmount, including a failure before mount.
    loop_association: Option<LoopAssociation>,
}

struct LoopAssociation {
    device: PathBuf,
    file: std::fs::File,
    backing: (u64, u64),
}

impl Scratch {
    fn make(uid: u32, gid: u32) -> Self {
        let name = format!("cowshed-job-cgroup-{}", std::process::id());
        let scratch = Self::prepare(std::env::temp_dir().join(name), uid, gid);
        if scratch.loop_association.is_none() {
            return scratch;
        }
        scratch.mount_using(uid, gid, run)
    }

    fn prepare(root: PathBuf, uid: u32, gid: u32) -> Self {
        std::fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("create {}: {error}", root.display()));
        let image = root.join("ext4.img");
        let mut scratch = Self {
            directory: root.join("mnt"),
            root,
            cleanup_started: false,
            loop_association: None,
        };
        let device = match scratch_loop_device() {
            Ok(device) => device,
            Err(reason) => {
                report_loop_unavailable(&reason);
                let name = scratch.root.file_name().expect("scratch name");
                return Self::tmpfs(Path::new("/dev/shm").join(name), uid, gid);
            }
        };
        std::fs::File::create(&image)
            .and_then(|file| file.set_len(SCRATCH_IMAGE_BYTES))
            .unwrap_or_else(|error| panic!("make the sparse {}: {error}", image.display()));
        run(Command::new(find_program("mkfs.ext4"))
            .arg("-q")
            .arg(&image))
        .unwrap_or_else(|refusal| panic!("{refusal}"));
        scratch.loop_association = Some(
            associate_scratch_loop(device, &image).unwrap_or_else(|refusal| panic!("{refusal}")),
        );
        scratch
    }

    fn mount_using(
        mut self,
        uid: u32,
        gid: u32,
        mount: impl FnOnce(&mut Command) -> Result<(), String>,
    ) -> Self {
        let scratch = &mut self;
        let association = scratch.loop_association.as_ref().expect("prepared loop");
        std::fs::create_dir(&scratch.directory)
            .unwrap_or_else(|error| panic!("create {}: {error}", scratch.directory.display()));
        // The trusted runner delegates mount(2) for ext4, not the fsopen/fsmount API.
        // Drop observes kernel mount identity even when mount reports an error after acquiring it.
        mount(
            Command::new(find_program("mount"))
                .env("LIBMOUNT_FORCE_MOUNT2", "always")
                .args(["-t", "ext4"])
                .arg(&association.device)
                .arg(&scratch.directory),
        )
        .unwrap_or_else(|refusal| panic!("{refusal}"));
        assert!(
            scratch
                .owned_mount()
                .expect("kernel mount identity")
                .is_some()
        );
        chown(&scratch.directory, Some(uid), Some(gid)).expect("chown scratch to the runner");
        let f_type = filesystem_type(&scratch.directory).expect("statfs the mounted image");
        assert_eq!(
            f_type,
            libc::EXT4_SUPER_MAGIC,
            "{} is ext4 once mounted: f_type {f_type:#x}",
            scratch.directory.display()
        );
        println!(
            "scratch: {} is ext4 on a loop device",
            scratch.directory.display()
        );
        self
    }

    fn tmpfs(root: PathBuf, uid: u32, gid: u32) -> Self {
        std::fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("create {}: {error}", root.display()));
        let scratch = Self {
            directory: root.clone(),
            root,
            cleanup_started: false,
            loop_association: None,
        };
        chown(&scratch.directory, Some(uid), Some(gid)).expect("chown tmpfs scratch to the runner");
        let f_type = filesystem_type(&scratch.directory).expect("statfs the tmpfs directory");
        assert_eq!(
            f_type,
            libc::TMPFS_MAGIC,
            "{} is on tmpfs: f_type {f_type:#x}",
            scratch.directory.display()
        );
        scratch
    }
}

/// Consume the runner's declared finite device pool. GET_FREE can select a globally free loop
/// that this namespace does not expose: refuse that selection, never create nodes or retry it.
fn scratch_loop_device() -> Result<PathBuf, String> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    let control = Path::new("/dev/loop-control");
    let control = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(control)
        .map_err(|error| format!("open {}: {error}", control.display()))?;
    let metadata = control
        .metadata()
        .map_err(|error| format!("stat loop-control: {error}"))?;
    if !metadata.file_type().is_char_device()
        || (metadata.uid(), metadata.gid(), metadata.mode() & 0o777) != (0, 0, 0o600)
    {
        return Err("loop-control must be a declared root:root 0600 character device".to_owned());
    }
    let expected = kernel_device_number(Path::new("/sys/class/misc/loop-control/dev"))?;
    let actual = (libc::major(metadata.rdev()), libc::minor(metadata.rdev()));
    if actual != expected {
        return Err(format!(
            "loop-control names {actual:?}, not its kernel device {expected:?}"
        ));
    }
    // SAFETY: the declared loop-control descriptor is live; LOOP_CTL_GET_FREE takes no
    // pointer argument and returns a globally free loop device's index or -1.
    let index = unsafe { libc::ioctl(control.as_raw_fd(), 0x4c82) };
    let index = u32::try_from(index).map_err(|_| format!("LOOP_CTL_GET_FREE: {}", last_error()))?;
    let device = PathBuf::from(format!("/dev/loop{index}"));
    let metadata = std::fs::metadata(&device).map_err(|error| {
        format!(
            "selected global loop{index} is not exposed in the runner's finite pool at {}: {error}",
            device.display()
        )
    })?;
    if !metadata.file_type().is_block_device() {
        return Err(format!(
            "{} is not a declared block device",
            device.display()
        ));
    }
    if (metadata.uid(), metadata.gid(), metadata.mode() & 0o777) != (0, 0, 0o600) {
        return Err(format!("{} must remain root:root 0600", device.display()));
    }
    let expected = kernel_device_number(&Path::new("/sys/block").join(format!("loop{index}/dev")))?;
    let actual = (libc::major(metadata.rdev()), libc::minor(metadata.rdev()));
    if actual != expected {
        return Err(format!(
            "{} names {actual:?}, not the kernel-selected device {expected:?}",
            device.display()
        ));
    }
    println!(
        "scratch loop: index {index}, declared device {}",
        device.display()
    );
    Ok(device)
}

/// Linux's loop_info64 and loop_config UAPI (linux/loop.h). Integer/byte-array-only layouts
/// admit an all-zero configuration; LOOP_CONFIGURE applies association and AUTOCLEAR atomically.
#[repr(C)]
struct LoopInfo64 {
    device: u64,
    inode: u64,
    rdevice: u64,
    offset: u64,
    sizelimit: u64,
    number: u32,
    encrypt_type: u32,
    encrypt_key_size: u32,
    flags: u32,
    file_name: [u8; 64],
    crypt_name: [u8; 64],
    encrypt_key: [u8; 32],
    init: [u64; 2],
}

#[repr(C)]
struct LoopConfig {
    fd: u32,
    block_size: u32,
    info: LoopInfo64,
    reserved: [u64; 8],
}

const _: () = assert!(std::mem::size_of::<LoopInfo64>() == 232);
const _: () = assert!(std::mem::size_of::<LoopConfig>() == 304);

fn associate_scratch_loop(device: PathBuf, image: &Path) -> Result<LoopAssociation, String> {
    use std::os::fd::AsRawFd as _;

    let open = |path: &Path| {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| format!("open {}: {error}", path.display()))
    };
    let loop_file = open(&device)?;
    let image = open(image)?;
    use std::os::unix::fs::MetadataExt as _;
    let metadata = image
        .metadata()
        .map_err(|error| format!("stat backing image: {error}"))?;
    let backing = (metadata.dev(), metadata.ino());
    // SAFETY: every bit pattern of these UAPI integer/byte-array fields is valid.
    let mut config: LoopConfig = unsafe { std::mem::zeroed() };
    config.fd = u32::try_from(image.as_raw_fd()).expect("an open file has a nonnegative fd");
    config.info.flags = 4; // LO_FLAGS_AUTOCLEAR
    // SAFETY: live loop and backing descriptors and the exact loop_config UAPI layout.
    if unsafe { libc::ioctl(loop_file.as_raw_fd(), 0x4c0a, std::ptr::from_ref(&config)) } != 0 {
        return Err(format!(
            "LOOP_CONFIGURE {}: {}",
            device.display(),
            last_error()
        ));
    }
    Ok(LoopAssociation {
        device,
        file: loop_file,
        backing,
    })
}

fn loop_backing(file: &std::fs::File) -> std::io::Result<(u64, u64)> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: the integer/byte-array-only output layout admits every bit pattern.
    let mut info: LoopInfo64 = unsafe { std::mem::zeroed() };
    // SAFETY: a live loop descriptor and the exact loop_info64 output buffer.
    if unsafe { libc::ioctl(file.as_raw_fd(), 0x4c05, std::ptr::from_mut(&mut info)) } != 0 {
        return Err(last_error());
    }
    Ok((info.device, info.inode))
}

/// Use the kernel's registered device number, not a guessed host node name or major/minor.
fn kernel_device_number(sys_dev: &Path) -> Result<(u32, u32), String> {
    let text = std::fs::read_to_string(sys_dev)
        .map_err(|error| format!("read {}: {error}", sys_dev.display()))?;
    let (major, minor) = text
        .trim()
        .split_once(':')
        .ok_or_else(|| format!("{} has no MAJ:MIN: {text:?}", sys_dev.display()))?;
    let major = major
        .parse()
        .map_err(|error| format!("device major: {error}"))?;
    let minor = minor
        .parse()
        .map_err(|error| format!("device minor: {error}"))?;
    Ok((major, minor))
}

fn report_loop_unavailable(reason: &str) {
    println!("scratch: loop devices unavailable: {reason}; tmpfs measures shmem only");
    let status = std::fs::read_to_string("/proc/self/status").expect("root status");
    for line in status.lines().filter(|line| {
        line.starts_with("CapEff:")
            || line.starts_with("NoNewPrivs:")
            || line.starts_with("Seccomp:")
            || line.starts_with("Seccomp_filters:")
    }) {
        println!("scratch availability: {line}");
    }
    println!(
        "scratch availability: loop module present {}, kernel {}",
        Path::new("/sys/module/loop").exists(),
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .expect("kernel release")
            .trim()
    );
    let devices = std::fs::read_to_string("/proc/devices").expect("registered devices");
    println!(
        "scratch availability: loop device registered {}",
        devices
            .lines()
            .any(|line| line.split_whitespace().eq(["7", "loop"]))
    );
    for entry in std::fs::read_dir("/sys/block").expect("kernel block devices") {
        let entry = entry.expect("block device entry");
        if entry.file_name().as_encoded_bytes().starts_with(b"loop") {
            println!("scratch availability: {}", entry.path().display());
        }
    }
}

/// Record the actual namespace/credentials used by each fixture role. Delegating cgroupfs
/// does not grant mount authority in that namespace, and UID zero alone proves no capability.
fn report_fixture_authority(role: &str) {
    // SAFETY: these credential reads cannot fail.
    let (uid, euid, gid) = unsafe { (libc::getuid(), libc::geteuid(), libc::getgid()) };
    let namespace = std::fs::read_link("/proc/self/ns/mnt").expect("read fixture mount namespace");
    let kernel =
        std::fs::read_to_string("/proc/sys/kernel/osrelease").expect("read fixture kernel");
    println!(
        "fixture authority: role {role}, uid {uid}, euid {euid}, gid {gid}, \
         mount namespace {}, kernel {}",
        namespace.display(),
        kernel.trim()
    );
    let status = std::fs::read_to_string("/proc/self/status").expect("read fixture capabilities");
    for line in status.lines().filter(|line| {
        line.starts_with("CapEff:")
            || line.starts_with("NoNewPrivs:")
            || line.starts_with("Seccomp:")
    }) {
        println!("fixture authority: {role} {line}");
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MountIdentity {
    id: u64,
    device: (u32, u32),
}

/// Ask the kernel for the mount containing this exact path, not mount(8)'s exit status.
fn mount_identity(path: &Path) -> std::io::Result<MountIdentity> {
    use std::os::unix::ffi::OsStrExt as _;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: an all-zero statx is valid output storage.
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: a NUL-terminated path and correctly sized statx output buffer.
    if unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_MNT_ID,
            &mut stat,
        )
    } != 0
    {
        return Err(last_error());
    }
    if stat.stx_mask & libc::STATX_MNT_ID == 0 {
        return Err(std::io::Error::other("kernel did not supply STATX_MNT_ID"));
    }
    Ok(MountIdentity {
        id: stat.stx_mnt_id,
        device: (stat.stx_dev_major, stat.stx_dev_minor),
    })
}

impl Scratch {
    fn owned_mount(&self) -> std::io::Result<Option<MountIdentity>> {
        use std::os::unix::fs::MetadataExt as _;
        let target = match mount_identity(&self.directory) {
            Ok(identity) => identity,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if target.id == mount_identity(&self.root)?.id {
            return Ok(None);
        }
        let association = self.loop_association.as_ref().ok_or_else(|| {
            std::io::Error::other(format!("unowned mount at {}", self.directory.display()))
        })?;
        let device = association.file.metadata()?.rdev();
        let expected = (libc::major(device), libc::minor(device));
        if target.device != expected || loop_backing(&association.file)? != association.backing {
            return Err(std::io::Error::other(format!(
                "mount {target:?} at {} does not belong to our loop backing {:?}",
                self.directory.display(),
                association.backing,
            )));
        }
        Ok(Some(target))
    }

    fn report_cleanup(failures: &mut Vec<String>, message: std::fmt::Arguments<'_>) {
        if let Err(error) = writeln!(std::io::stdout(), "{message}") {
            failures.push(format!("write scratch cleanup diagnostic: {error}"));
        }
    }

    /// Release every safe owned step; preserve an image whose detach is not proven.
    fn cleanup(&mut self) -> Result<(), String> {
        if self.cleanup_started {
            return Ok(());
        }
        self.cleanup_started = true;
        let mut failures = Vec::new();
        match self.owned_mount() {
            Ok(Some(identity)) => {
                Self::report_cleanup(
                    &mut failures,
                    format_args!("scratch cleanup: retiring kernel mount {identity:?}"),
                );
                // Command's PATH lookup is fallible; no executable lookup may panic in Drop.
                if let Err(refusal) = run(Command::new("umount").arg(&self.directory)) {
                    failures.push(refusal);
                }
            }
            Ok(None) => {}
            Err(error) => failures.push(format!("observe owned mount: {error}")),
        }
        let unmounted = match self.owned_mount() {
            Ok(None) => true,
            Ok(Some(identity)) => {
                failures.push(format!("kernel mount {identity:?} remains attached"));
                false
            }
            Err(error) => {
                failures.push(format!("verify unmount: {error}"));
                false
            }
        };
        let mut detached = true;
        // AUTOCLEAR was set atomically. Closing our descriptor is attempted even if umount failed.
        if let Some(LoopAssociation {
            device,
            file,
            backing,
        }) = self.loop_association.take()
        {
            drop(file);
            let after = std::fs::File::open(&device).and_then(|file| loop_backing(&file));
            match after {
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {
                    Self::report_cleanup(
                        &mut failures,
                        format_args!("scratch cleanup: loop association cleared"),
                    );
                }
                Ok(after) if backing != after => {
                    Self::report_cleanup(
                        &mut failures,
                        format_args!(
                            "scratch cleanup: our association cleared and the device was reused"
                        ),
                    );
                }
                Ok(_) => {
                    failures.push("our loop association remains attached".to_owned());
                    detached = false;
                }
                Err(error) => {
                    failures.push(format!("verify loop association cleanup: {error}"));
                    detached = false;
                }
            }
        }
        // Never descend into a still-mounted image or unlink an unproven backing association.
        let removed = if unmounted && detached {
            match std::fs::remove_dir_all(&self.root) {
                Ok(()) => true,
                Err(error) => {
                    failures.push(format!("remove {}: {error}", self.root.display()));
                    false
                }
            }
        } else {
            false
        };
        if removed {
            Self::report_cleanup(
                &mut failures,
                format_args!(
                    "scratch cleanup: {} unmounted and removed",
                    self.root.display()
                ),
            );
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "scratch cleanup {}: {}",
                self.root.display(),
                failures.join("; "),
            ))
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if !self.cleanup_started
            && let Err(refusal) = self.cleanup()
        {
            match writeln!(std::io::stderr(), "{refusal}") {
                Ok(()) => {}
                Err(stderr_error) => match writeln!(
                    std::io::stdout(),
                    "{refusal}; stderr diagnostic unavailable: {stderr_error}"
                ) {
                    Ok(()) => {}
                    Err(_stdout_error) => {
                        // Both existing delivery channels are unavailable. Drop cannot return
                        // their errors; all safe resource releases have already been attempted.
                    }
                },
            }
        }
    }
}

/// A refused command unwinds through the same guard as failed setup or a failed scenario.
/// When loop access is refused, report unmounted tmpfs cleanup only, not association proof.
fn scratch_cleanup_survives_failure(uid: u32, gid: u32) {
    for requested_mount in [false, true] {
        let scratch = if requested_mount {
            Scratch::make(uid, gid)
        } else {
            let name = format!("cowshed-job-cgroup-{}", std::process::id());
            Scratch::prepare(std::env::temp_dir().join(name), uid, gid)
        };
        let root = scratch.root.clone();
        let actual_mounted = scratch
            .owned_mount()
            .expect("kernel mount identity")
            .is_some();
        let associated = scratch.loop_association.is_some();
        let outcome = std::panic::catch_unwind(move || {
            let _scratch = scratch;
            run(&mut Command::new(find_program("false"))).expect("intentional cleanup refusal");
        });
        assert!(outcome.is_err(), "the refusal unwinds");
        assert!(
            !root.exists(),
            "cleanup removed {} after refusal",
            root.display()
        );
        println!(
            "scratch cleanup: command failure and panic unwind verified \
             (requested mount {requested_mount}, actual mounted {actual_mounted}, \
             loop associated {associated})"
        );
    }
    // A setup operation can acquire the mount and still report failure to its caller.
    // Inject that refusal inside setup, not after Scratch construction has already succeeded.
    let name = format!("cowshed-job-cgroup-{}", std::process::id());
    let scratch = Scratch::prepare(std::env::temp_dir().join(name), uid, gid);
    let root = scratch.root.clone();
    if scratch.loop_association.is_none() {
        drop(scratch);
        println!("blocked: setup-acquired mount refusal needs an available loop device");
        return;
    }
    let parent_mount = mount_identity(&root).expect("parent mount");
    let target = scratch.directory.clone();
    let mut acquired = None;
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scratch.mount_using(uid, gid, |command| {
            run(command)?;
            let identity = mount_identity(&target)
                .map_err(|error| format!("observe acquired setup mount: {error}"))?;
            assert_ne!(identity.id, parent_mount.id);
            acquired = Some(identity);
            println!("scratch setup refusal: acquired kernel mount {identity:?}");
            Err("intentional setup refusal after the kernel acquired the mount".to_owned())
        });
    }));
    assert!(outcome.is_err(), "the setup refusal unwinds");
    assert!(
        acquired.is_some(),
        "the refused setup actually acquired its kernel mount"
    );
    assert!(!root.exists(), "setup refusal removed {}", root.display());
    println!("scratch cleanup: setup-acquired mount refusal verified");
}

/// `program` from `PATH`, run as root by `sudo`, which never prompts for a password.
fn sudo(program: &str) -> Command {
    let mut command = Command::new(find_program("sudo"));
    command.arg("-n").arg(find_program(program));
    command
}

/// Run `command` to its end; a refusal is what it said.
fn run(command: &mut Command) -> Result<(), String> {
    let output = command
        .output_locked()
        .map_err(|error| format!("run {command:?}: {error}"))?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "{command:?}: {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
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

/// Allocate storage without writing payload bytes; finish allocation metadata before HELD.
fn allocate(path: &Path, mebibytes: u64) {
    use std::os::fd::AsRawFd as _;

    let file = std::fs::File::create(path)
        .unwrap_or_else(|error| panic!("create {}: {error}", path.display()));
    let length = libc::off_t::try_from(mebibytes << 20).expect("length");
    // SAFETY: a live descriptor and plain integers.
    let allocated = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    assert_eq!(allocated, 0, "posix_fallocate: errno {allocated}");
    file.sync_all().expect("flush the allocated file metadata");
    std::fs::File::open(path.parent().expect("allocated file directory"))
        .and_then(|directory| directory.sync_all())
        .expect("flush the allocation directory metadata");
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

/// Live tasks' own and waited-for-children bytes, not lifetime job I/O. Both byte keys
/// must actually be present; an unreadable member or absent key is not measured zero.
fn live_members_storage_io_bytes(procs: &str) -> Result<u64, String> {
    procs.lines().try_fold(0, |total, pid| {
        let path = format!("/proc/{pid}/io");
        let io = std::fs::read_to_string(&path).map_err(|error| format!("read {path}: {error}"))?;
        let counter = |name| -> Result<u64, String> {
            io.lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    (key == name).then_some(value.trim())
                })
                .ok_or_else(|| format!("{path} has no {name}"))?
                .parse()
                .map_err(|error| format!("{name} in {path}: {error}"))
        };
        Ok(total + counter("read_bytes")? + counter("write_bytes")?)
    })
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
