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
//!
//! The containment cases keep escapees alive past everything a process-group or parent-chain
//! census follows: a background child, a child that leaves its parent's session while the parent
//! runs, and a child that waits on its parent's pidfd to see the parent exit before it leaves the
//! session. Each admits its job through the supervisor's own sink, places the leader before it
//! runs and ends the job with `kill_and_wait`. Every handshake is a pipe or a pidfd and every
//! wait is the kernel's own event: no sleep, retry or clock stands in for one. The admission
//! refusals are provoked through real cgroup interfaces -- a hierarchy limit, a controller the
//! incarnation withholds, a `cgroup.kill` its owner cannot write, a cgroup removed under its
//! holder -- never through a stand-in directory.

#![cfg(target_os = "linux")]

use std::ffi::OsString;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::os::unix::fs::chown;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use cowshed_core::api::dto::{ExecClass, JobId};
use cowshed_core::error::ErrorCode;
use cowshed_core::runtime::job_cgroup::{
    CGROUP_MOUNT, CONTROLLER_LEAF, CgroupAuthority, CgroupError, CgroupMemberSignal, Controller,
    IncarnationCgroups, JobCgroup, JobCgroupIdentity,
};
use cowshed_core::runtime::supervisor::{
    SpawnSink as _, SystemSpawnSink, WorkspaceAuthoritySnapshot,
};
use cowshed_os::deadline::{Bound, LatencyClass, REAP};
use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::process::RunState;
use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
use cowshed_types::metadata::{WorkspaceIncarnation, WorkspaceName};
use cowshed_types::repository::RepoId;
use serde::{Deserialize, Serialize};

const ROLE: &str = "COWSHED_JOB_CGROUP_ROLE";
const ROLE_TEST: &str = "job_cgroup_role";
const REPORT: &str = "COWSHED_JOB_CGROUP_REPORT ";
const PAUSED: &str = "COWSHED_JOB_CGROUP_PAUSED";
const HELD: &str = "COWSHED_JOB_CGROUP_HELD";
/// One milestone of an escape fixture's process: this prefix, then a JSON [`Stage`].
const STAGE: &str = "COWSHED_JOB_CGROUP_STAGE ";
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
    /// A job's leader that starts a background descendant, then holds until it is released.
    Leader(Escapee),
    /// A background descendant of a job's leader: it outlives the leader's stdio, not the job.
    Escapee(Escapee),
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

/// How a background descendant relates to the session its job's leader runs in. Each is a way
/// for a process to leave a process group or a parent chain without leaving its job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum Detach {
    /// Stays in the leader's session and process group, running in the background.
    Background,
    /// Calls `setsid(2)` while its parent still runs.
    SessionWhileParentLives,
    /// Waits on its parent's pidfd until the parent has exited, and only then calls `setsid(2)`.
    SessionAfterParentExit,
}

/// What a job's leader starts: a descendant that ignores `SIGTERM`, closes the stdio it
/// inherited and ends only by a kill.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Escapee {
    detach: Detach,
    /// The controller's pid. The controller's exit is the one event besides a kill that ends an
    /// escapee, so a scenario that fails leaves no process behind in the delegated scope.
    controller: u32,
}

/// What a process of an escape fixture has done when it states a [`Stage`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum StageKind {
    /// The job's leader, placed before it ran.
    Leader,
    /// The escapee holds its parent's pidfd and has not yet done what its [`Detach`] says.
    Armed,
    /// The escapee has done it, and is about to close the stdio it inherited.
    Detached,
}

/// One milestone of an escape fixture's process, on its output after [`STAGE`].
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Stage {
    kind: StageKind,
    pid: u32,
    /// The cgroup the process read as its first action.
    first_cgroup: String,
    /// Whether its parent's pidfd was readable when it stated this; never for a leader, whose
    /// parent is the controller.
    parent_exited: bool,
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
        .output_bounded()
        .expect("run sudo");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{stdout}");
    cowshed_os::eprintln!("{stderr}");
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
        Role::Leader(escapee) => lead(&escapee),
        Role::Escapee(escapee) => escape(&escapee),
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
        .output_bounded()
        .expect("run the unprivileged controller");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    cowshed_os::eprintln!("{}", String::from_utf8_lossy(&output.stderr));
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
                .output_bounded()
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

/// A job's leader that starts an escapee: say where it was born, start the escapee on the stdio
/// it inherited itself, then hold until the controller releases it. The hold also waits on the
/// escapee's pidfd, so an escapee that ends first fails the scenario at once instead of leaving
/// the controller waiting for output nobody will write.
fn lead(escapee: &Escapee) {
    let first_cgroup = own_cgroup();
    stage(StageKind::Leader, &first_cgroup, false);
    #[expect(
        clippy::zombie_processes,
        reason = "this fixture must orphan a live escapee; the controller joins its cgroup and pidfd"
    )]
    let mut child = escapee_command(escapee)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn_locked()
        .expect("start the escapee");
    let escapee_pidfd = pidfd_of(child.id(), "escapee");
    // Released by one byte on stdin, the way a held workload waits.
    let mut watched = [
        libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        },
        escapee_pidfd.watch(),
    ];
    wait_for_events(&mut watched, -1);
    if watched[1].revents != 0 {
        let status = child
            .wait_bounded()
            .expect("reap the unexpectedly exited escapee");
        panic!("the escapee ended while its leader was held: {status}");
    }
    // The released leader deliberately exits without waiting: the live child must outlast it.
    let mut byte = [0];
    std::io::stdin().read_exact(&mut byte).expect("release");
}

/// The escapee: say where it was born, leave its session as `detach` says, close the stdio it
/// inherited, then live until the job's kill ends it. Every wait is a kernel event on a pidfd,
/// never a clock, and a polite `SIGTERM` is ignored: only a kill ends it.
fn escape(escapee: &Escapee) -> ! {
    let first_cgroup = own_cgroup();
    // SAFETY: signal(2) with a constant disposition.
    let previous = unsafe { libc::signal(libc::SIGTERM, libc::SIG_IGN) };
    assert_ne!(
        previous,
        libc::SIG_ERR,
        "ignore SIGTERM in the escapee: {}",
        last_error()
    );
    // The controller holds the leader until this has armed, so the pid is still the parent's.
    let parent_pid = current_parent();
    let parent = pidfd_of(parent_pid, "parent");
    let controller = pidfd_of(escapee.controller, "controller");
    stage(StageKind::Armed, &first_cgroup, parent.exited());
    match escapee.detach {
        Detach::Background => {}
        Detach::SessionWhileParentLives => new_session(),
        Detach::SessionAfterParentExit => {
            // A pidfd is readable once its process has exited, and by then the kernel has
            // already given that process's children to a new parent.
            parent.await_exit();
            assert_ne!(
                current_parent(),
                parent_pid,
                "the escapee is an orphan once its parent exited"
            );
            new_session();
        }
    }
    stage(StageKind::Detached, &first_cgroup, parent.exited());
    close_inherited_stdio();
    // Alive the way a daemon waits: on a kernel event. The controller's own exit is the only one
    // that is not a kill, so a failed scenario leaves no process behind in the scope.
    controller.await_exit();
    // SAFETY: _exit never returns, and nothing past here is wanted: no destructor, no harness.
    unsafe { libc::_exit(0) }
}

/// State a milestone of this process on the output it inherited: one line, written at once.
fn stage(kind: StageKind, first_cgroup: &str, parent_exited: bool) {
    let record = Stage {
        kind,
        pid: std::process::id(),
        first_cgroup: first_cgroup.to_owned(),
        parent_exited,
    };
    let line = serde_json::to_string(&record).expect("stage");
    println!("{STAGE}{line}");
    std::io::stdout().flush().expect("flush");
}

/// This binary again, in the escapee's role.
fn escapee_command(escapee: &Escapee) -> Command {
    role_command(&Role::Escapee(escapee.clone()))
}

/// This process's parent as the kernel names it now.
fn current_parent() -> u32 {
    // SAFETY: getppid cannot fail.
    u32::try_from(unsafe { libc::getppid() }).expect("a parent pid is never negative")
}

/// Leave the session and process group this process was born into.
fn new_session() {
    // SAFETY: setsid takes no arguments and fails only for a process group leader.
    let session = unsafe { libc::setsid() };
    assert_eq!(
        u32::try_from(session),
        Ok(std::process::id()),
        "setsid made this process its own session leader: {}",
        last_error()
    );
}

/// Close stdin, stdout and stderr the way a daemon does, so no pipe it inherited outlives the
/// process that opened it.
fn close_inherited_stdio() {
    std::io::stdout().flush().expect("flush stdout");
    for descriptor in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // SAFETY: closes a descriptor this process owns and never uses again.
        let closed = unsafe { libc::close(descriptor) };
        assert_eq!(closed, 0, "close fd {descriptor}: {}", last_error());
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
    escapees_die_with_their_job(&authority);
    an_unending_member_is_named_past_the_reap_bound(&authority);
    admission_refuses_what_it_cannot_contain(&authority);

    // Leave the scope as admission found it.
    retire_outstanding(&jobs);
}

// ---------------------------------------------------------------------------------------------
// Scenarios

fn descendants_inherit_the_job(jobs: &IncarnationCgroups) {
    let job = jobs.admit(job_id(1), ExecClass::Background).expect("admit");
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
    let small = jobs.admit(job_id(2), ExecClass::Background).expect("admit");
    let large = jobs.admit(job_id(3), ExecClass::Background).expect("admit");
    assert!(
        matches!(
            jobs.admit(job_id(2), ExecClass::Background),
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
    let job = jobs.admit(job_id(4), ExecClass::Background).expect("admit");
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
    let job = jobs.admit(job_id(5), ExecClass::Background).expect("admit");
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
    let burst = jobs.admit(job_id(8), ExecClass::Background).expect("admit");
    let unrelated = jobs.admit(job_id(9), ExecClass::Background).expect("admit");
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
    let job = jobs
        .admit(job_id(10), ExecClass::Background)
        .expect("admit");
    let page_cache_file = in_scratch("page-cache");
    let mut running = Spawned::placed(
        &job,
        &Workload {
            allocate_mib: 128,
            page_cache: Some((page_cache_file.clone(), 64)),
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
    // The job's own file pages still in the page cache, read after memory.stat and memory.current:
    // reclaim between the reads can only lower it. Page cache is reclaimable, so how much of the
    // 64 MiB is still cached is the host's business; that whatever is cached is charged to the job
    // as regular-file cache is the job's.
    let cached = page_cached_bytes(&page_cache_file);
    println!(
        "charged memory: current {current} B, peak {peak} B (direct before {direct_before:?}, \
         after {direct_after:?}); live members resident {resident} B; memory.stat file {file} B, \
         shmem {shmem} B, regular-file cache {regular_file} B; the job's file cached {cached} B; \
         filesystem {f_type:#x}"
    );
    if f_type == libc::EXT4_SUPER_MAGIC {
        assert!(
            cached > 0,
            "the job's 64 MiB file left no page in the cache to attribute"
        );
        assert!(
            regular_file >= cached,
            "every cached page of the job's regular file is charged as non-shmem cache: \
             regular-file cache {regular_file} B, the file's cached pages {cached} B"
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
    // No inequality between the charge and RSS holds by construction: RSS also counts pages
    // charged to other cgroups (the binary, shared libraries). The job maps none of its cached
    // file, so those pages are charged without being resident by how the workload is built.
    assert!(
        current >= cached,
        "the job's cached file pages are in its charge: charged {current} B, resident \
         {resident} B, the file's cached pages {cached} B"
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
    let job = jobs
        .admit(job_id(11), ExecClass::Background)
        .expect("admit");
    let neighbour_job = jobs
        .admit(job_id(12), ExecClass::Background)
        .expect("admit");
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
    let job = jobs.admit(job_id(6), ExecClass::Background).expect("admit");
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
    let identity = jobs
        .admit(job_id(7), ExecClass::Background)
        .expect("admit")
        .identity()
        .clone();
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
        ExecClass::Background,
        identity.cgroup_id() + 1,
    );
    assert!(matches!(
        restarted.lookup(&forged),
        Err(CgroupError::Replaced { .. })
    ));
}

/// Leave an incarnation as admission found it: every job cgroup still in it has ended, and goes.
fn retire_outstanding(jobs: &IncarnationCgroups) {
    for identity in jobs.outstanding().expect("outstanding") {
        jobs.lookup(&identity)
            .expect("lookup")
            .terminal()
            .expect("every workload ended")
            .retire()
            .expect("retire");
    }
}

/// A job's kill reaches a background escapee however it left the leader's session, and the job
/// is sealed only after the kernel reports every process of it gone (07_api.md, "Complete job
/// accounting and observation reconciliation"). The leader is placed before it runs and the
/// escapee is only ever born of it, so its membership is inherited: once it has left the
/// leader's session, or the leader has exited and the kernel has reparented it, nothing but the
/// cgroup ties it to the job.
fn escapees_die_with_their_job(authority: &CgroupAuthority) {
    let incarnation = incarnation(3);
    let snapshot = authority_snapshot(&incarnation);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");
    let mut sink = SystemSpawnSink::default();
    let escapes = [
        (1, Detach::Background),
        (2, Detach::SessionWhileParentLives),
        (3, Detach::SessionAfterParentExit),
    ];
    for (id, detach) in escapes {
        kill_reaches_the_escapee(&mut sink, &snapshot, &jobs, id, detach);
    }
    let left = jobs.outstanding().expect("outstanding");
    assert!(
        left.is_empty(),
        "every escape fixture retired its job cgroup: {left:?}"
    );
}

fn kill_reaches_the_escapee(
    sink: &mut SystemSpawnSink,
    authority: &WorkspaceAuthoritySnapshot,
    jobs: &IncarnationCgroups,
    id: u64,
    detach: Detach,
) {
    let scenario = format!("{detach:?}");
    println!("escape fixture {scenario}: start");
    let job = native_admission(sink, authority, job_id(id));
    let path = relative(job.path());
    let layout = format!(
        "/background/job-{}-{id}",
        authority.workspace_incarnation.as_str()
    );
    assert!(
        path.ends_with(&layout),
        "{scenario}: {path} is not the {layout} cgroup of the job"
    );
    let parent_exits_first = detach == Detach::SessionAfterParentExit;
    let escapee = Escapee {
        detach,
        controller: std::process::id(),
    };
    let mut leader = Spawned::leading(&job, &escapee);
    let leader_pid = leader.child.id();
    let born = leader.await_stage(StageKind::Leader);
    assert_eq!(
        (born.pid, born.first_cgroup.as_str()),
        (leader_pid, path.as_str()),
        "{scenario}: the leader's first instruction ran in the job"
    );
    let armed = leader.await_stage(StageKind::Armed);
    let escapee_pid = armed.pid;
    assert_eq!(
        armed.first_cgroup, path,
        "{scenario}: the escapee was born in the job"
    );
    assert!(
        !armed.parent_exited,
        "{scenario}: the leader still ran when the escapee armed"
    );
    // Held from a moment each process is known to run, so neither can name another process.
    let leader_pidfd = pidfd_of(leader_pid, "leader");
    let escapee_pidfd = pidfd_of(escapee_pid, "escapee");
    assert!(
        !escapee_pidfd.exited(),
        "{scenario}: the escapee runs when its pidfd is first held"
    );
    assert!(
        !leader_pidfd.exited(),
        "{scenario}: the leader runs when the escapee arms"
    );
    if parent_exits_first {
        // The escapee only waits on its parent's pidfd: until the parent exits it is still the
        // leader's child, in the leader's group and session.
        let leader_lineage = lineage(leader_pid);
        assert_eq!(
            lineage(escapee_pid),
            Lineage {
                parent: leader_pid,
                group: leader_lineage.group,
                session: leader_lineage.session,
            },
            "{scenario}: the waiting escapee has not left the leader's session"
        );
        leader.release();
    }
    let detached = leader.await_stage(StageKind::Detached);
    assert_eq!(
        detached.pid, escapee_pid,
        "{scenario}: one process stated both stages"
    );
    assert_eq!(
        detached.parent_exited, parent_exits_first,
        "{scenario}: the parent had exited when the escapee detached only if it exits first"
    );
    // What the kernel itself says about the escapee, not what it claims.
    let after = lineage(escapee_pid);
    let own = (escapee_pid, escapee_pid);
    let (group, session) = match detach {
        Detach::Background => {
            let leaders = lineage(leader_pid);
            (leaders.group, leaders.session)
        }
        Detach::SessionWhileParentLives | Detach::SessionAfterParentExit => own,
    };
    assert_eq!(
        (after.group, after.session),
        (group, session),
        "{scenario}: the escapee's process group and session"
    );
    if parent_exits_first {
        assert_ne!(
            after.parent, leader_pid,
            "{scenario}: the kernel reparented the escapee when its leader exited"
        );
    } else {
        assert_eq!(
            after.parent, leader_pid,
            "{scenario}: the escapee is still its leader's child"
        );
    }
    // A leader that exited first closes the output with its escapee: reading it to its end
    // proves the escapee let go of the pipe it inherited.
    let leader = if parent_exits_first {
        let (status, rest) = leader.reap();
        assert!(
            status.success(),
            "{scenario}: the released leader exited normally: {status}\n{rest}"
        );
        assert!(
            leader_pidfd.exited(),
            "{scenario}: the leader had exited before the escapee left its session"
        );
        None
    } else {
        Some(leader)
    };
    let mut members = vec![escapee_pid];
    if leader.is_some() {
        members.push(leader_pid);
    }
    members.sort_unstable();
    assert_eq!(
        census(&job),
        members,
        "{scenario}: every live process of the job is a member, whatever its session"
    );
    assert_eq!(
        cgroup_of(escapee_pid),
        path,
        "{scenario}: the kernel places the escapee in the job"
    );
    assert!(
        !escapee_pidfd.exited(),
        "{scenario}: the escapee is alive when the kill is issued"
    );
    assert_eq!(
        leader_pidfd.exited(),
        parent_exits_first,
        "{scenario}: the leader has exited only where the scenario ends it first"
    );
    assert!(
        job.populated().expect("populated before the kill"),
        "{scenario}: the job holds live processes before the kill"
    );

    job.kill_and_wait(REAP)
        .unwrap_or_else(|error| panic!("{scenario}: kill and wait: {error}"));
    // The wait joined the exit of every process the kill ended, so both pidfds are readable now:
    // the kernel's exit event, which an empty `cgroup.events` alone can precede.
    for (name, pidfd) in [("leader", &leader_pidfd), ("escapee", &escapee_pidfd)] {
        assert!(
            pidfd.exited(),
            "{scenario}: the {name}'s pidfd is readable when kill_and_wait returns"
        );
    }
    assert!(
        !job.populated().expect("populated after the kill"),
        "{scenario}: the job is empty after the kill"
    );
    assert!(
        !events_populated(&job),
        "{scenario}: cgroup.events reads populated 0 after the kill"
    );
    assert!(
        census(&job).is_empty(),
        "{scenario}: no process is left in cgroup.procs after the kill"
    );
    let (receipt, concurrent) = std::thread::scope(|scope| {
        let first = scope.spawn(|| job.retire(Duration::ZERO, REAP).expect("retire"));
        let second = scope.spawn(|| job.retire(Duration::ZERO, REAP).expect("concurrent retire"));
        (
            first.join().expect("first retire"),
            second.join().expect("second retire"),
        )
    });
    assert!(
        Arc::ptr_eq(&receipt, &concurrent),
        "{scenario}: retirement shares one proof"
    );
    assert_receipt(&receipt.members, &members, &scenario);
    assert!(
        receipt.metadata_gap,
        "{scenario}: atomic kill has no exhaustive member list"
    );
    {
        use std::os::unix::fs::PermissionsExt as _;
        let permissions = std::fs::metadata(job.path())
            .expect("job metadata")
            .permissions();
        std::fs::set_permissions(job.path(), std::fs::Permissions::from_mode(0o0))
            .expect("deny final counter reads");
        let refused = job.finish(REAP);
        std::fs::set_permissions(job.path(), permissions).expect("restore job permissions");
        assert!(
            refused.is_err(),
            "{scenario}: unavailable final counters refuse finalization"
        );
    }
    let retried = job
        .retire(Duration::ZERO, REAP)
        .expect("retry after refused finalization");
    assert!(
        Arc::ptr_eq(&receipt, &retried),
        "{scenario}: retry retains every signal row"
    );
    if let Some(leader) = leader {
        let (status, rest) = leader.reap();
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "{scenario}: the job's kill ended the leader: {status}\n{rest}"
        );
    }

    let identity = job.identity().clone();
    let job = Arc::try_unwrap(job).expect("nothing else holds the admitted cgroup");
    let terminal = job.terminal().expect("sealed");
    for (name, pidfd) in [("leader", &leader_pidfd), ("escapee", &escapee_pidfd)] {
        assert!(
            pidfd.exited(),
            "{scenario}: the {name}'s pidfd is readable when the job is sealed"
        );
    }
    let cpu = terminal.cpu().expect("final cpu");
    terminal.charged_memory().expect("final charged memory");
    terminal.storage_io().expect("final storage I/O");
    let removed = terminal.path().to_path_buf();
    let totals = terminal.retire().expect("retire");
    assert_eq!(
        totals.cpu, cpu,
        "{scenario}: retirement collects the final counters"
    );
    assert!(
        !removed.exists(),
        "{scenario}: {} was retired",
        removed.display()
    );
    assert!(matches!(
        jobs.lookup(&identity),
        Err(CgroupError::Missing { .. })
    ));
    let left = jobs.outstanding().expect("outstanding");
    assert!(
        !left.contains(&identity),
        "{scenario}: retired, yet outstanding: {left:?}"
    );
    println!("escape fixture {scenario}: leader {leader_pid}, escapee {escapee_pid}, {after:?}");
}

/// Admission is the one moment a job's cgroup can be refused, and a refusal leaves nothing behind
/// that a retry or a restart would trip over. Each case provokes a real kernel refusal through a
/// real cgroup interface; no stand-in directory plays a cgroup.
fn admission_refuses_what_it_cannot_contain(authority: &CgroupAuthority) {
    a_name_in_use_is_refused(authority);
    a_hierarchy_limit_refuses_creation(authority);
    a_cgroup_without_a_controller_is_removed_again(authority);
    an_unwritable_cgroup_kill_refuses_attachment(authority);
    a_vanished_cgroup_refuses_kill_and_wait();
    shared_finalization_removes_only_the_admitted_hierarchy(authority);
}

/// The supervisor's own sink refuses a job whose workspace incarnation already holds a cgroup of
/// that name, and leaves the cgroup it found alone; the same job id in another incarnation is
/// another cgroup.
fn a_name_in_use_is_refused(authority: &CgroupAuthority) {
    let (first, second) = (incarnation(4), incarnation(5));
    let (here, there) = (authority_snapshot(&first), authority_snapshot(&second));
    let jobs = authority.incarnation(&first).expect("incarnation");
    let others = authority.incarnation(&second).expect("incarnation");
    let existing = jobs
        .admit(job_id(1), ExecClass::Background)
        .expect("an earlier admission");
    let mut sink = SystemSpawnSink::default();
    let refusal = sink
        .admit_cgroup(&here, job_id(1), ExecClass::Background)
        .expect_err("a name in use is refused, never adopted or admitted uncontained");
    assert_eq!(
        refusal.code,
        ErrorCode::EnvironmentMissing,
        "a name in use is an environment refusal: {refusal:?}"
    );
    assert!(
        refusal
            .message
            .contains(&existing.path().display().to_string()),
        "the refusal names the cgroup in use: {refusal:?}"
    );
    let kept = jobs.lookup(existing.identity()).expect("lookup");
    assert_eq!(
        kept.identity(),
        existing.identity(),
        "the refused admission left the earlier one untouched"
    );
    let admitted = sink
        .admit_cgroup(&here, job_id(2), ExecClass::Background)
        .expect("admit")
        .expect("a cgroup");
    assert_eq!(admitted.path().parent(), existing.path().parent());
    assert_eq!(admitted.identity().job_id(), job_id(2));
    let elsewhere = sink
        .admit_cgroup(&there, job_id(1), ExecClass::Background)
        .expect("admit")
        .expect("a cgroup");
    assert_ne!(elsewhere.path(), existing.path());
    assert_eq!(elsewhere.identity().incarnation(), &second);
    retire_outstanding(&jobs);
    retire_outstanding(&others);
}

/// The kernel's own hierarchy limit refuses a new cgroup beneath a class cgroup. The library and
/// the supervisor's sink both report it and create nothing: the same job is admitted once the
/// limit is lifted.
fn a_hierarchy_limit_refuses_creation(authority: &CgroupAuthority) {
    let incarnation = incarnation(6);
    let snapshot = authority_snapshot(&incarnation);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");
    let mut sink = SystemSpawnSink::default();
    let first = native_admission(&mut sink, &snapshot, job_id(1));
    let limit = first
        .path()
        .parent()
        .expect("the class cgroup")
        .join("cgroup.max.descendants");
    std::fs::write(&limit, "0").expect("set the descendant limit");
    let library = jobs.admit(job_id(2), ExecClass::Background);
    let native = sink.admit_cgroup(&snapshot, job_id(2), ExecClass::Background);
    let listed = jobs.outstanding();
    // Lifted before anything is judged, so a failed assertion cannot leave the scope refusing.
    std::fs::write(&limit, "max").expect("lift the descendant limit");
    let refusal = library.expect_err("the kernel refuses a cgroup beneath the limit");
    assert!(
        matches!(refusal, CgroupError::Io { call: "mkdir", .. }),
        "{refusal:?}"
    );
    let refusal = native.expect_err("the production sink refuses it too");
    assert_eq!(
        refusal.code,
        ErrorCode::EnvironmentMissing,
        "a refused creation is an environment refusal: {refusal:?}"
    );
    let listed = listed.expect("outstanding");
    assert_eq!(
        listed.len(),
        1,
        "the refused admission created no cgroup beside the first: {listed:?}"
    );
    let retry = native_admission(&mut sink, &snapshot, job_id(2));
    assert_eq!(retry.identity().job_id(), job_id(2));
    retire_outstanding(&jobs);
}

/// A cgroup its class cgroup gives no memory controller cannot be opened for accounting. That
/// refusal comes after the kernel created the directory -- the place a missing `cgroup.kill` is
/// refused too -- so it must take the directory away again, and the job's name is free to try
/// once the controller is back.
fn a_cgroup_without_a_controller_is_removed_again(authority: &CgroupAuthority) {
    let incarnation = incarnation(7);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");
    let probe = jobs.admit(job_id(1), ExecClass::Background).expect("admit");
    let distribution = probe
        .path()
        .parent()
        .expect("the class cgroup")
        .join("cgroup.subtree_control");
    let sealed = probe.terminal().expect("sealed");
    sealed.retire().expect("retire");
    std::fs::write(&distribution, "-memory").expect("withhold the memory controller");
    let refused = jobs.admit(job_id(2), ExecClass::Background);
    let listed = jobs.outstanding();
    std::fs::write(&distribution, "+memory").expect("restore the memory controller");
    let refusal = refused.expect_err("a cgroup without the memory controller is refused");
    let CgroupError::ControllerUnavailable { controller, .. } = &refusal else {
        panic!("a missing controller refuses the admission as such: {refusal:?}");
    };
    assert_eq!(*controller, Controller::Memory, "{refusal:?}");
    let listed = listed.expect("outstanding");
    assert!(
        listed.is_empty(),
        "the refused admission left a cgroup behind: {listed:?}"
    );
    let retry = jobs
        .admit(job_id(2), ExecClass::Background)
        .expect("the refused job's name is free again");
    let sealed = retry.terminal().expect("sealed");
    sealed.retire().expect("retire");
}

/// A restarted controller reattaches to a job by looking it up, which opens `cgroup.kill` again.
/// Where its owner cannot write it, the lookup is refused with the kernel's reason and the cgroup
/// stays in place; once it is writable again, the same lookup attaches.
fn an_unwritable_cgroup_kill_refuses_attachment(authority: &CgroupAuthority) {
    use std::os::unix::fs::PermissionsExt as _;

    let incarnation = incarnation(8);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");
    let job = jobs.admit(job_id(1), ExecClass::Background).expect("admit");
    let identity = job.identity().clone();
    let kill = job.path().join("cgroup.kill");
    let writable = std::fs::metadata(&kill)
        .expect("stat cgroup.kill")
        .permissions();
    let denied = std::fs::Permissions::from_mode(0o0);
    std::fs::set_permissions(&kill, denied).expect("deny cgroup.kill");
    let attached = jobs.lookup(&identity);
    std::fs::set_permissions(&kill, writable).expect("restore cgroup.kill");
    let refusal = attached.expect_err("an unwritable cgroup.kill refuses the attachment");
    let CgroupError::KillUnavailable { path, source } = &refusal else {
        panic!("an unwritable cgroup.kill is KillUnavailable: {refusal:?}");
    };
    assert_eq!(path, &kill, "{refusal:?}");
    assert_eq!(
        source.kind(),
        std::io::ErrorKind::PermissionDenied,
        "{refusal:?}"
    );
    assert_eq!(
        jobs.outstanding().expect("outstanding").as_slice(),
        std::slice::from_ref(&identity),
        "a refused attachment leaves the job's cgroup in place"
    );
    let attached = jobs.lookup(&identity).expect("attach");
    let sealed = attached.terminal().expect("sealed");
    sealed.retire().expect("retire");
}

/// An empty cgroup can be removed under its holder: its descriptors then name nothing, and
/// `cgroup.kill` is gone with it. Ending or awaiting such a job is an error naming the cgroup,
/// never a silent success.
fn a_vanished_cgroup_refuses_kill_and_wait() {
    let incarnation = incarnation(9);
    let snapshot = authority_snapshot(&incarnation);
    let mut sink = SystemSpawnSink::default();
    let job = native_admission(&mut sink, &snapshot, job_id(1));
    std::fs::remove_dir(job.path()).expect("remove the empty cgroup");
    let refused = [
        ("kill", job.kill()),
        ("kill_and_wait", job.kill_and_wait(REAP)),
    ];
    let path = job.path().display().to_string();
    for (call, result) in refused {
        let error = result.expect_err("a vanished cgroup is never reported killed or empty");
        assert!(
            error.to_string().contains(&path),
            "{call} names the cgroup that vanished: {error}"
        );
    }
}

/// A member the kill never ends holds `kill_and_wait` for at most its reap bound, and the failure
/// names it with its pid, image and kernel state (05_gateway.md "Deadlines"; 07_api.md
/// "Complete job accounting and observation reconciliation"). The case it stands for is a member
/// in an uninterruptible kernel call, which SIGKILL reaches only once that call returns; no
/// unprivileged fixture can hold a process there. So the fixture holds the other half of the
/// same bounded wait: a member retirement pinned, whose exit never comes. It ignores SIGTERM, so
/// `terminate` pins it without ending it, and is then moved out of the job: the cgroup reads
/// empty, and the pinned pidfd stays unreadable.
fn an_unending_member_is_named_past_the_reap_bound(authority: &CgroupAuthority) {
    let jobs = authority
        .incarnation(&incarnation(11))
        .expect("incarnation");
    let job = jobs.admit(job_id(1), ExecClass::Background).expect("admit");
    let mut command = workload_command(&Workload {
        pause: true,
        ..Workload::default()
    });
    job.placement().expect("placement").on_exec(&mut command);
    // SAFETY: signal(2) is async-signal-safe; an ignored disposition survives exec.
    unsafe {
        command.pre_exec(|| {
            if libc::signal(libc::SIGTERM, libc::SIG_IGN) == libc::SIG_ERR {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut member = Spawned::spawn(command);
    member.await_pause();
    let pid = member.child.id();
    job.terminate()
        .expect("pin the member with a TERM it ignores");
    let leaf = own_cgroup_path().join("cgroup.procs");
    std::fs::write(&leaf, pid.to_string())
        .unwrap_or_else(|error| panic!("move {pid} into {}: {error}", leaf.display()));
    assert!(
        !job.populated().expect("populated"),
        "the job holds no process once its member moved out"
    );

    let bound = Bound::new(LatencyClass::Reap, Duration::from_millis(200));
    let started = std::time::Instant::now();
    let refused = job
        .kill_and_wait(bound)
        .expect_err("a member whose exit never comes is never reported reaped");
    let waited = started.elapsed();
    let CgroupError::Unreaped { expired, beside } = &refused else {
        panic!("an expiry naming the member, not {refused:?}");
    };
    assert!(
        waited >= bound.within,
        "the member had its whole bound: {waited:?}"
    );
    assert_eq!(expired.bound, bound.within);
    assert!(beside.is_empty(), "nothing else failed: {beside:?}");
    assert_eq!(expired.peer.cgroup, job.path());
    let members = expired
        .peer
        .members
        .as_ref()
        .expect("the members were read");
    assert_eq!(
        members.iter().map(|live| live.peer.pid).collect::<Vec<_>>(),
        [pid],
        "the expiry names the member whose exit never came"
    );
    assert_eq!(
        members[0].peer.state,
        RunState::Sleeping,
        "it waits on its stdin"
    );
    assert_eq!(
        members[0].image.as_deref(),
        test_binary().to_str(),
        "the expiry names the member's image"
    );
    let message = refused.to_string();
    assert!(
        message.starts_with("TIMEOUT: job cgroup kill on job cgroup ")
            && message.contains(&format!("pid {pid} (sleeping)")),
        "{message}"
    );

    member.resume();
    member.finish();
    job.retire(Duration::ZERO, REAP)
        .expect("retire once the member has exited");
    job.finish(REAP).expect("finish the empty job");
}

/// The actor can finalize while a late host release still holds an Arc and a placement fd.
/// Neither stale capability may enter or remove a replacement at the old path.
fn shared_finalization_removes_only_the_admitted_hierarchy(authority: &CgroupAuthority) {
    let incarnation = incarnation(10);
    let snapshot = authority_snapshot(&incarnation);
    let jobs = authority.incarnation(&incarnation).expect("incarnation");
    let mut sink = SystemSpawnSink::default();
    let job = native_admission(&mut sink, &snapshot, job_id(1));
    let late_host = Arc::clone(&job);
    let placement = job.placement().expect("placement retained by a late host");
    let original = job.identity().clone();
    std::fs::create_dir(job.path().join("empty-child")).expect("an empty owned child cgroup");
    let proof = job.retire(Duration::ZERO, REAP).expect("empty retirement");
    assert!(proof.members.is_empty());
    assert!(!proof.metadata_gap, "no kill ran for an empty admission");
    let totals = job
        .finish(REAP)
        .expect("finalize with shared descriptors still held");
    assert!(
        !job.path().exists(),
        "the empty hierarchy is removed before seal"
    );
    assert!(jobs.outstanding().expect("outstanding").is_empty());

    let replacement = jobs
        .admit(job_id(1), ExecClass::Background)
        .expect("a new cgroup at the old path");
    assert_ne!(replacement.identity(), &original);
    assert_eq!(
        late_host.finish(REAP).expect("cached final counters"),
        totals,
        "late finalization returns its own counters, never removes the replacement"
    );
    let late_proof = late_host
        .retire(Duration::ZERO, REAP)
        .expect("cached exit proof");
    assert!(
        Arc::ptr_eq(&proof, &late_proof),
        "late retirement never re-signals"
    );
    assert!(replacement.path().exists());
    let mut command = workload_command(&Workload::default());
    placement.on_exec(&mut command);
    assert!(
        command.spawn_locked().is_err(),
        "a removed cgroup's retained placement descriptor cannot start a command"
    );
    replacement
        .terminal()
        .expect("empty replacement")
        .retire()
        .expect("retire replacement");
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

    /// A job's leader that starts `escapee`, placed in `job` before it runs.
    fn leading(job: &JobCgroup, escapee: &Escapee) -> Self {
        let mut command = role_command(&Role::Leader(escapee.clone()));
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

    /// The next stage of `kind` that any process of the workload tree states; every other line
    /// is skipped. The output ending first fails the scenario naming the stage it never reached.
    fn await_stage(&mut self, kind: StageKind) -> Stage {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self.stdout.read_line(&mut line).expect("workload output");
            assert_ne!(read, 0, "the output ended before the {kind:?} stage");
            if let Some(json) = line.strip_prefix(STAGE) {
                let stage: Stage = serde_json::from_str(json)
                    .unwrap_or_else(|error| panic!("stage record {line:?}: {error}"));
                if stage.kind == kind {
                    return stage;
                }
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

    /// The workload's end: its output read to the end -- reached only once every process that
    /// inherited the pipe has closed or lost it -- and then its status, reaped with its rusage.
    fn reap(mut self) -> (std::process::ExitStatus, String) {
        let mut rest = String::new();
        self.stdout
            .read_to_string(&mut rest)
            .expect("workload output");
        let (status, _) = reap_workload(&self.child).expect("reap the workload with rusage");
        (status, rest)
    }
}

/// This binary again, in `role`.
fn role_command(role: &Role) -> Command {
    let mut command = Command::new(test_binary());
    command
        .args(role_arguments())
        .env(ROLE, serde_json::to_string(role).expect("role"));
    command
}

fn workload_command(workload: &Workload) -> Command {
    role_command(&Role::Workload(workload.clone()))
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
    loop {
        match cowshed_os::test_backstop::wait4(pid, 0) {
            Ok((waited, status, usage)) if waited == pid => {
                return Ok((
                    std::process::ExitStatus::from_raw(status),
                    rusage_cpu_us(&usage),
                ));
            }
            Ok((waited, ..)) => {
                return Err(std::io::Error::other(format!(
                    "wait4 {pid} answered {waited}"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
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

/// Bytes of `path` in the page cache now, by mincore(2) over a read-only shared mapping. Mapping
/// faults nothing in, and a page stays charged to the cgroup that first brought it into the cache.
fn page_cached_bytes(path: &Path) -> u64 {
    use std::os::fd::AsRawFd as _;

    let file = std::fs::File::open(path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
    let length = usize::try_from(file.metadata().expect("cached file metadata").len())
        .expect("cached file length");
    if length == 0 {
        return 0;
    }
    // SAFETY: sysconf has no preconditions.
    let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).expect("page size");
    // SAFETY: a fresh shared read-only mapping of the whole open file.
    let region = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            length,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(region, libc::MAP_FAILED, "mmap: {}", last_error());
    let mut status = vec![0_u8; length.div_ceil(page)];
    // SAFETY: the mapping made above, and one status byte per page of it.
    let probed = unsafe { libc::mincore(region, length, status.as_mut_ptr()) };
    let probe_error = (probed != 0).then(last_error);
    // SAFETY: the whole mapping made above, unmapped once.
    assert_eq!(unsafe { libc::munmap(region, length) }, 0, "munmap");
    if let Some(error) = probe_error {
        panic!("mincore {}: {error}", path.display());
    }
    let pages = status.iter().filter(|byte| **byte & 1 == 1).count();
    u64::try_from(pages * page).expect("cached bytes")
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
    /// Atomic AUTOCLEAR ownership. The backing file's final-close notification fences the
    /// kernel's deferred device retirement after unmount and our descriptor close.
    loop_association: Option<LoopAssociation>,
}

struct LoopAssociation {
    device: PathBuf,
    file: std::fs::File,
    backing: (u64, u64),
    closed: BackingClose,
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
const _: () = assert!(std::mem::size_of::<libc::inotify_event>() == 16);

/// Freshly armed on the exact inode pinned by the writable file description configured into
/// the loop. Inotify is inode-based: no later writable open of this owned image is allowed.
struct BackingClose {
    events: std::fs::File,
    watch: i32,
}

impl BackingClose {
    fn arm(image: &std::fs::File) -> std::io::Result<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        // SAFETY: creates a new descriptor; CLOEXEC prevents every setup child inheriting it.
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(last_error());
        }
        // SAFETY: this successful syscall returned one descriptor owned only here.
        let events = unsafe { std::fs::File::from_raw_fd(fd) };
        let image_fd = std::ffi::CString::new(format!("/proc/self/fd/{}", image.as_raw_fd()))
            .map_err(std::io::Error::other)?;
        // SAFETY: the live image descriptor pins the exact inode behind this NUL-terminated path.
        let watch = unsafe {
            libc::inotify_add_watch(
                fd,
                image_fd.as_ptr(),
                libc::IN_CLOSE_WRITE | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF,
            )
        };
        if watch < 0 {
            return Err(last_error());
        }
        Ok(Self { events, watch })
    }

    fn wait(mut self) -> std::io::Result<()> {
        // One blocking read consumes one complete record. A 16-byte buffer admits only a nameless
        // inotify_event; any other length is a failed delivery, never retried. The fixture's
        // existing overall guard bounds this wait: no new grace is added.
        let mut event = [0_u8; 16];
        let received = self.events.read(&mut event)?;
        if received != event.len() {
            return Err(std::io::Error::other(format!(
                "backing close notification short: read {received} of {} bytes",
                event.len(),
            )));
        }
        let watch = i32::from_ne_bytes(event[0..4].try_into().expect("fixed watch field"));
        let mask = u32::from_ne_bytes(event[4..8].try_into().expect("fixed mask field"));
        let cookie = u32::from_ne_bytes(event[8..12].try_into().expect("fixed cookie field"));
        let length = u32::from_ne_bytes(event[12..16].try_into().expect("fixed length field"));
        if watch != self.watch || mask != libc::IN_CLOSE_WRITE || cookie != 0 || length != 0 {
            return Err(std::io::Error::other(format!(
                "backing close notification unavailable: expected watch {} IN_CLOSE_WRITE, \
                 cookie 0, name length 0; got watch {watch}, mask {mask:#x}, cookie {cookie}, \
                 name length {length}",
                self.watch,
            )));
        }
        Ok(())
    }
}

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
    // mkfs and all earlier writable opens finished before this fresh exact-inode watch.
    let closed = BackingClose::arm(&image)
        .map_err(|error| format!("watch configured backing file's final close: {error}"))?;
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
        closed,
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
            closed,
        }) = self.loop_association.take()
        {
            drop(file);
            if unmounted {
                if let Err(error) = closed.wait() {
                    failures.push(format!(
                        "await the owned backing file's final close: {error}"
                    ));
                    detached = false;
                }
            } else {
                failures.push("unmount unproven: cannot await backing release".to_owned());
                detached = false;
            }
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
        .output_bounded()
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
    cgroup_of("self")
}

/// The cgroup v2 path `/proc/<process>/cgroup` names, for a pid or `self`.
fn cgroup_of(process: impl std::fmt::Display) -> String {
    let content = read_kernel(format!("/proc/{process}/cgroup"));
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

/// A kernel file read whole; one that cannot be read fails the scenario naming it.
fn read_kernel(path: impl AsRef<Path>) -> String {
    let path = path.as_ref();
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {path:?}: {error}"))
}

/// A process named by the one identity the kernel never gives another: its pidfd, readable once
/// the process has exited. Waiting on it is waiting on the kernel's own event.
struct Pidfd(std::os::fd::OwnedFd);

impl Pidfd {
    fn open(pid: u32) -> std::io::Result<Self> {
        use std::os::fd::FromRawFd as _;

        let pid = libc::pid_t::try_from(pid).map_err(std::io::Error::other)?;
        let (pid, flags): (libc::c_long, libc::c_long) = (pid.into(), 0);
        // SAFETY: pidfd_open takes plain integers; a non-negative answer is a new descriptor.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, flags) };
        if opened < 0 {
            return Err(last_error());
        }
        let descriptor = libc::c_int::try_from(opened).map_err(std::io::Error::other)?;
        // SAFETY: pidfd_open just returned this descriptor and nothing else owns it.
        Ok(Self(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(descriptor)
        }))
    }

    /// A poll entry that turns readable when the process has exited.
    fn watch(&self) -> libc::pollfd {
        use std::os::fd::AsRawFd as _;

        libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        }
    }

    /// Whether the process has exited, asked once.
    fn exited(&self) -> bool {
        let mut watched = [self.watch()];
        wait_for_events(&mut watched, 0);
        watched[0].revents & libc::POLLIN != 0
    }

    /// Block until the kernel reports that the process has exited. No clock bounds this wait.
    fn await_exit(&self) {
        let mut watched = [self.watch()];
        wait_for_events(&mut watched, -1);
        assert_ne!(
            watched[0].revents & libc::POLLIN,
            0,
            "the pidfd woke without reporting an exit (poll revents {})",
            watched[0].revents
        );
    }
}

/// A pidfd for `pid`, a process that must exist; `whose` names it in the failure.
fn pidfd_of(pid: u32, whose: &str) -> Pidfd {
    Pidfd::open(pid).unwrap_or_else(|error| panic!("open a pidfd for {whose} {pid}: {error}"))
}

/// `poll(2)` over `watched`, again after every signal. `timeout` is 0 to ask once and -1 to wait
/// for the kernel's event.
fn wait_for_events(watched: &mut [libc::pollfd], timeout: libc::c_int) {
    let count = libc::nfds_t::try_from(watched.len()).expect("a few descriptors");
    loop {
        // SAFETY: `watched` is a live array of exactly `count` pollfd entries.
        if unsafe { libc::poll(watched.as_mut_ptr(), count, timeout) } >= 0 {
            return;
        }
        let error = last_error();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::Interrupted,
            "poll {} descriptors: {error}",
            watched.len()
        );
    }
}

/// A process's parent, process group and session, as `/proc/<pid>/stat` states them.
#[derive(Debug, Eq, PartialEq)]
struct Lineage {
    parent: u32,
    group: u32,
    session: u32,
}

fn lineage(pid: u32) -> Lineage {
    let stat = read_kernel(format!("/proc/{pid}/stat"));
    // The command name may hold spaces and parentheses: the fields resume after its last one.
    let (_, fields) = stat
        .rsplit_once(") ")
        .unwrap_or_else(|| panic!("no command in {stat:?}"));
    // `fields` starts at the state, field 3; the parent, group and session are the next three.
    let mut fields = fields.split_whitespace().skip(1);
    let mut next = |name: &str| -> u32 {
        let field = fields
            .next()
            .unwrap_or_else(|| panic!("{name} missing from {stat:?}"));
        field
            .parse()
            .unwrap_or_else(|error| panic!("{name} {field:?} in {stat:?}: {error}"))
    };
    let (parent, group, session) = (next("ppid"), next("pgrp"), next("session"));
    Lineage {
        parent,
        group,
        session,
    }
}

/// The pids `cgroup.procs` lists for `job`, in order.
fn census(job: &JobCgroup) -> Vec<u32> {
    let path = job.path().join("cgroup.procs");
    let procs = read_kernel(&path);
    let mut pids = Vec::new();
    for line in procs.lines() {
        let pid = line
            .parse::<u32>()
            .unwrap_or_else(|error| panic!("member {line:?} of {path:?}: {error}"));
        pids.push(pid);
    }
    pids.sort_unstable();
    pids
}

/// `populated` of the job's `cgroup.events`, read directly.
fn events_populated(job: &JobCgroup) -> bool {
    let events = read_kernel(job.path().join("cgroup.events"));
    match events
        .lines()
        .find_map(|line| line.strip_prefix("populated "))
    {
        Some("0") => false,
        Some("1") => true,
        other => panic!(
            "{} has no populated state ({other:?}): {events:?}",
            job.path().display()
        ),
    }
}

/// What `retire` retained of the processes the kill ended: each pid, the image it ran, SIGKILL.
fn assert_receipt(receipt: &[CgroupMemberSignal], killed: &[u32], scenario: &str) {
    let image = test_binary().to_string_lossy().into_owned();
    let mut retained: Vec<(u32, i32)> = receipt
        .iter()
        .map(|member| (member.pid, member.signal))
        .collect();
    retained.sort_unstable();
    let expected: Vec<(u32, i32)> = killed.iter().map(|pid| (*pid, libc::SIGKILL)).collect();
    assert_eq!(
        retained, expected,
        "{scenario}: the receipt names every process the kill ended, each killed: {receipt:?}"
    );
    for member in receipt {
        assert_eq!(
            member.image.as_deref(),
            Some(image.as_str()),
            "{scenario}: the receipt names the image of process {}",
            member.pid
        );
    }
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

fn authority_snapshot(incarnation: &WorkspaceIncarnation) -> WorkspaceAuthoritySnapshot {
    WorkspaceAuthoritySnapshot {
        repo_id: RepoId::parse("local/default").expect("static repo id"),
        workspace: WorkspaceName::new("main").expect("static workspace name"),
        workspace_incarnation: incarnation.clone(),
        grant_revision: 1,
        lifecycle_revision: 1,
    }
}

/// A job's cgroup as the supervisor's production sink admits it before the job is recorded.
fn native_admission(
    sink: &mut SystemSpawnSink,
    authority: &WorkspaceAuthoritySnapshot,
    id: JobId,
) -> Arc<JobCgroup> {
    let admitted = sink
        .admit_cgroup(authority, id, ExecClass::Background)
        .unwrap_or_else(|error| panic!("native admission of job {}: {error}", id.get()));
    admitted.unwrap_or_else(|| {
        panic!(
            "the production sink admitted job {} without a cgroup",
            id.get()
        )
    })
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
