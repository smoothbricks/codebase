use super::*;
use cowshed_os::fork_lock::Spawn as _;
use scratch_apfs::ScratchRoot;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn capacity(value: &str) -> ImageCapacity {
    ImageCapacity::parse(value).expect("test capacities are well formed")
}

struct FixtureRetainedInput<'a>(&'a Path);

impl RetainedInputs for FixtureRetainedInput<'_> {
    fn names(&self, path: &Path) -> bool {
        self.0 == path
    }
}

fn assert_lease_excludes_another_descriptor(held: &fs::File) {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let mut path = [0u8; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes at most PATH_MAX bytes to the live descriptor's path buffer.
    assert_eq!(
        unsafe { libc::fcntl(held.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) },
        0
    );
    let path = std::ffi::CStr::from_bytes_until_nul(&path).expect("the kernel terminates the path");
    let probe =
        fs::File::open(std::ffi::OsStr::from_bytes(path.to_bytes())).expect("reopen held lease");
    // SAFETY: probe owns a live file descriptor; flock retains no pointers.
    assert_eq!(
        unsafe { libc::flock(probe.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        -1
    );
    assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
}
#[cfg(target_os = "macos")]
struct RealImageCleanup<'a> {
    backend: &'a MacOsApfsBackend<SystemCommandRunner>,
    image: PathBuf,
    attachment: Option<AttachedImage>,
    armed: bool,
}

#[cfg(target_os = "macos")]
impl<'a> RealImageCleanup<'a> {
    fn new(backend: &'a MacOsApfsBackend<SystemCommandRunner>, image: PathBuf) -> Self {
        Self {
            backend,
            image,
            attachment: None,
            armed: true,
        }
    }

    fn track(&mut self, attachment: AttachedImage) -> &AttachedImage {
        assert!(
            self.attachment.is_none(),
            "only one real image attachment may be tracked"
        );
        self.attachment.insert(attachment)
    }

    fn release_attachment(&self) -> Result<(), ApfsError> {
        if let Some(attachment) = self.attachment.as_ref() {
            return self.backend.detach(attachment, DetachIntent::Release);
        }
        // Creation can fail after attach but before the caller receives an AttachedImage.
        // No tracked handle is not proof of a detached fixture or permission to unlink it.
        if self.image.exists() && self.backend.attached_whole_devices(&self.image)?.is_empty() {
            return Err(ApfsError::InvalidAttachmentInventory(format!(
                "cannot prove fixture image {} was released; retaining its backing file",
                self.image.display()
            )));
        }
        self.backend
            .release_image(&self.image, DetachIntent::Release)
    }

    fn finish(mut self) -> Result<(), ApfsError> {
        // Exactly one cleanup attempt: a failure retains its media and original error.
        self.armed = false;
        self.release_attachment()?;
        if self.image.exists() {
            self.backend.delete_image(&self.image)?;
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl Drop for RealImageCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let released = self.release_attachment().and_then(|()| {
                if self.image.exists() {
                    self.backend.delete_image(&self.image)?;
                }
                Ok(())
            });
            if let Err(error) = released {
                eprintln!(
                    "cowshed: retained real APFS fixture {}: {error}",
                    self.image.display()
                );
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn finish_real_image_test(result: Result<(), ApfsError>, cleanup: RealImageCleanup<'_>) {
    match (result, cleanup.finish()) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) => panic!("real APFS scenario failed: {error}"),
        (Ok(()), Err(error)) => panic!("real APFS cleanup failed: {error}"),
        (Err(primary), Err(cleanup)) => {
            panic!("real APFS scenario failed: {primary}; cleanup failed: {cleanup}")
        }
    }
}
#[cfg(target_os = "macos")]
fn lease_test_image(staged_stem: PathBuf, volume_name: &str) -> CreateImageRequest {
    CreateImageRequest {
        staged_stem,
        capacity: capacity("64m"),
        volume_name: volume_name.into(),
        // SAFETY: `getuid` reads this process's credentials; it takes no pointers and
        // cannot fail.
        owner_uid: unsafe { libc::getuid() },
        // SAFETY: `getgid` reads this process's credentials; it takes no pointers and
        // cannot fail.
        owner_gid: unsafe { libc::getgid() },
    }
}

/// Plain filesystem paths a real-APFS test creates beside its images: alias links and mount
/// directories. Declared before the image cleanups, so on every exit — success, `?` or
/// unwind — it drops after them, once scoped threads have joined and devices are released.
/// A removal failure fails a passing test; during an unwind it is reported beside the
/// original failure instead of replacing it.
#[cfg(target_os = "macos")]
struct TestPaths(Vec<PathBuf>);

#[cfg(target_os = "macos")]
impl Drop for TestPaths {
    fn drop(&mut self) {
        let mut retained = Vec::new();
        for path in &self.0 {
            let removed = match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() => fs::remove_dir(path),
                Ok(_) => fs::remove_file(path),
                Err(error) => Err(error),
            };
            match removed {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => retained.push(format!("{}: {error}", path.display())),
            }
        }
        if retained.is_empty() {
            return;
        }
        let retained = retained.join("; ");
        if std::thread::panicking() {
            eprintln!("cowshed: retained test paths after the failure above: {retained}");
        } else {
            panic!("remove test paths: {retained}");
        }
    }
}

/// The device namespace is shared, but leases are per image: every cooperating step of one
/// image waits for that image's lease, and nothing of another image does.
///
/// Holding image A's production lease, the test drives image B through a complete real
/// lifecycle on another thread — blank create, attach, format and detach; attach and fsck;
/// mount; native unmount; detach — while a real detach of A, started first, waits. Under a
/// host-wide lease B could not begin; without a per-image lease A's detach would not wait
/// for the release. Aliases of A's file (a symlinked parent, a hard link) take other leases
/// yet gain nothing: the inventory never maps them, so they cannot release A's device, and A's
/// attachment holds the file exclusively against an open through either of them, so neither
/// can take it from A — checked without spending an attach of its own. Every wait is an
/// event; the only deadline is the hang bound, and it names the step still open.
///
/// A's attachment is also the image-growth witness, so growth spends no attach of its own: a
/// real attachment holds its image file exclusively (01_storage.md, "Images"), so growing A
/// while attached is refused outright and nothing is written; once A is detached, the grow
/// lands and A's capacity reports it.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_image_leases_overlap_across_images_and_exclude_within_one() {
    use std::sync::mpsc;

    enum Progress {
        Step(&'static str),
        Done,
    }

    let root = ScratchRoot::new("leases").expect("scratch root");
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let image_a = root
        .path()
        .join("lease-held")
        .with_extension(IMAGE_EXTENSION);
    let stem_b = root.path().join("lease-free");
    let image_b = stem_b.with_extension(IMAGE_EXTENSION);
    let mount_b = root.path().join("lease-free-mount");
    let alias_parent = root.path().join("lease-alias-parent");
    let hard_link = root
        .path()
        .join("lease-hard-link")
        .with_extension(IMAGE_EXTENSION);
    let _paths = TestPaths(vec![
        alias_parent.clone(),
        hard_link.clone(),
        mount_b.clone(),
    ]);
    let order = std::sync::Mutex::new(Vec::new());
    let mut cleanup_a = RealImageCleanup::new(&backend, image_a.clone());
    let result = (|| -> Result<(), ApfsError> {
        crate::blank_image::blank_image(&image_a);
        let attachment = cleanup_a.track(backend.attach_verified(&image_a)?);
        assert!(attachment.whole_device().starts_with("/dev/disk"));
        assert!(attachment.volume_device().starts_with("/dev/disk"));
        // Before A's lease is held below, since resizing takes it.
        let refused = SystemCommandRunner
            .grow_image(&image_a, capacity("2g"))
            .expect_err("an attached image's file is held exclusively");
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock, "{refused}");
        assert!(matches!(
            backend.resize_image(&image_a, capacity("2g")),
            Err(ApfsError::FileOperation { operation: "grow RAW image", source, .. })
                if source.kind() == io::ErrorKind::WouldBlock
        ));

        std::os::unix::fs::symlink(image_a.parent().expect("temp parent"), &alias_parent)
            .expect("symlinked-parent alias");
        fs::hard_link(&image_a, &hard_link).expect("hard-link alias");
        let symlinked = alias_parent.join(image_a.file_name().expect("image name"));

        let held = SystemCommandRunner
            .image_lease(&image_a)
            .expect("take the image's production lease")
            .expect("production takes a real lease");
        assert_lease_excludes_another_descriptor(&held);

        // Each alias takes its own lease, so these run while A's is held; none may take A's
        // file or disturb A's attachment. No alias is attached: its inventory key is its own
        // path, so an attach would reach `diskutil image attach` and spend a host attach. The
        // inventory shows no alias owns a device, and the exclusive open shows A holds the file.
        for alias in [&symlinked, &hard_link] {
            assert!(
                backend.recovered_image_attachment(alias)?.is_none(),
                "{} has an inventory mapping of its own",
                alias.display()
            );
            let refused = SystemCommandRunner
                .grow_image(alias, capacity("2g"))
                .expect_err("A's attachment holds the file every alias names");
            assert_eq!(
                refused.kind(),
                io::ErrorKind::WouldBlock,
                "{}: {refused}",
                alias.display()
            );
            // Releases only the alias's own (empty) inventory, never A's device.
            backend.release_image(alias, DetachIntent::Release)?;
            match backend.recovered_image_attachment(&image_a)? {
                Some(RecoveredImageAttachment::Apfs(still)) => assert_eq!(
                    (still.whole_device(), still.volume_device()),
                    (attachment.whole_device(), attachment.volume_device())
                ),
                _ => panic!("{} took A's attachment away", alias.display()),
            }
        }

        let (detach_started, detach_waiting) = mpsc::channel();
        let (progress, steps) = mpsc::channel();
        std::thread::scope(|scope| -> Result<(), ApfsError> {
            let (backend, order, mount_b) = (&backend, &order, &mount_b);
            let detach_a = scope.spawn(move || {
                detach_started.send(()).expect("the test awaits A's detach");
                let detached = backend.detach(attachment, DetachIntent::Release);
                order.lock().expect("order").push("A detached");
                detached
            });
            detach_waiting
                .recv_timeout(DISK_CHILD_DEADLINE)
                .expect("A's detach started");
            scope.spawn(move || {
                // A send fails only after the test panicked; the scope reports that panic.
                let report = |event: Progress| {
                    if let Progress::Step(step) = &event {
                        // On stderr too, so a harness kill still shows the open step.
                        eprintln!("cowshed: lease test image B step `{step}` started");
                    }
                    let _ = progress.send(event);
                };
                let mut cleanup = RealImageCleanup::new(backend, image_b);
                let result = (|| -> Result<(), ApfsError> {
                    report(Progress::Step("create"));
                    let created = backend
                        .create_staged_image(&lease_test_image(stem_b, "cowshed-lease-free"))?;
                    report(Progress::Step("attach-mount"));
                    let attachment = cleanup.track(backend.attach_mounted(
                        &created,
                        mount_b,
                        MountAccess::ReadWrite,
                        false,
                    )?);
                    fs::write(mount_b.join("written-beside-a-held-lease"), b"independent")
                        .expect("write into B's volume");
                    report(Progress::Step("unmount"));
                    backend.unmount_verified(attachment, DetachIntent::Release)?;
                    report(Progress::Step("detach"));
                    backend.detach(attachment, DetachIntent::Release)
                })();
                report(Progress::Step("cleanup"));
                finish_real_image_test(result, cleanup);
                report(Progress::Done);
            });
            let mut open = "spawn";
            loop {
                match steps.recv_timeout(DISK_CHILD_DEADLINE) {
                    Ok(Progress::Step(step)) => open = step,
                    Ok(Progress::Done) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                        "image B's `{open}` is still open after {DISK_CHILD_DEADLINE:?} \
                             while only A's lease is held"
                    ),
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        panic!("image B's lifecycle stopped inside `{open}`")
                    }
                }
            }
            {
                let mut order = order.lock().expect("order");
                order.push("B finished");
                order.push("A released");
            }
            drop(held);
            detach_a.join().expect("A's detach thread")
        })?;

        assert_eq!(
            *order.lock().expect("order"),
            ["B finished", "A released", "A detached"]
        );
        assert!(
            backend.recovered_image_attachment(&image_a)?.is_none(),
            "A's detach released its own device"
        );
        assert_eq!(backend.image_capacity(&image_a)?, capacity("1g"));
        backend.resize_image(&image_a, capacity("2g"))?;
        assert_eq!(backend.image_capacity(&image_a)?, capacity("2g"));
        Ok(())
    })();
    finish_real_image_test(result, cleanup_a);
}

/// How [`real_apfs_fixture_run_ended_by_its_parent`] ends: `panic`, `kill` or `sweep`.
#[cfg(target_os = "macos")]
const FIXTURE_ENDING: &str = "COWSHED_TEST_FIXTURE_ENDING";
/// The line a fixture run prints once each of its images is attached.
#[cfg(target_os = "macos")]
const FIXTURE_IMAGE: &str = "cowshed-test-fixture-image=";
/// The line a fixture run prints once a process it started works in its root: that pid and
/// the root, space-separated.
#[cfg(target_os = "macos")]
const FIXTURE_WORKER: &str = "cowshed-test-fixture-worker=";

/// A real-image fixture leaves nothing attached and nothing running however its run ends. A
/// panic unwinds through the scratch root's own teardown. A kill — the runner's no-progress
/// bound, a host-wide stop — runs no teardown at all, so its parent reclaims the
/// exact root after reaping the child. The same cleanup ends the dead owner's processes and
/// releases its images without sweeping other host runs.
/// Fixtures staged under `$TMPDIR` stayed attached after timed-out runs, and the stock Nx daemon a
/// timed-out test started kept running in its deleted root. The run's worker stands in for
/// that daemon: a process the run started that outlives it, selected, as the daemon is, by its
/// working directory alone. The run attaches one RAW image and one retained ASIF input, as
/// a failed re-adoption leaves both formats, so teardown and sweep must release both. The RAW
/// image is mounted, so teardown also unmounts; the ASIF input is only attached, all its
/// release needs, so it spends no mount.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end() {
    use cowshed_os::fork_lock::Spawn;
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, Command, Stdio};

    // Helpers retain the suite's run identity and own only the roots their parent recovers.
    // Terse child output keeps libtest's test-name progress off the first image record.
    // Forward stderr line by line so a hung helper still shows its open step, and report
    // whether it printed a panic: its output is this test's, so a panic would read as this
    // test's assertion failure behind any timeout.
    let run = |ending: &str| -> (Child, std::thread::JoinHandle<bool>) {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "tests::real_apfs_fixture_run_ended_by_its_parent",
                "--ignored",
                "--nocapture",
                "--quiet",
            ])
            .env(FIXTURE_ENDING, ending)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .expect("spawn a fixture run");
        let stderr = BufReader::new(child.stderr.take().expect("run stderr"));
        let forwarded = std::thread::spawn(move || {
            stderr.lines().fold(false, |panicked, line| {
                let line = line.expect("run stderr");
                eprintln!("{line}");
                panicked || line.contains("panicked at")
            })
        });
        (child, forwarded)
    };
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    for ending in ["panic", "kill"] {
        let (mut child, forwarded) = run(ending);
        let mut lines = BufReader::new(child.stdout.take().expect("run stdout"))
            .lines()
            .map(|line| line.expect("run stdout"));
        let images: Vec<PathBuf> = lines
            .by_ref()
            .filter_map(|line| line.strip_prefix(FIXTURE_IMAGE).map(PathBuf::from))
            .take(2)
            .collect();
        // A red run must not leak the images it proves leaked.
        let _leaks: Vec<_> = images
            .iter()
            .map(|image| RealImageCleanup::new(&backend, image.clone()))
            .collect();
        assert_eq!(images.len(), 2, "the {ending} run attached {images:?}");
        let (worker, root) = lines
            .find_map(|line| {
                let (pid, root) = line.strip_prefix(FIXTURE_WORKER)?.split_once(' ')?;
                Some((pid.parse::<libc::pid_t>().ok()?, PathBuf::from(root)))
            })
            .unwrap_or_else(|| panic!("the {ending} run never started its worker"));
        if ending == "kill" {
            child.kill().expect("kill the run mid-fixture");
        }
        lines.for_each(drop);
        let status = child.wait().expect("reap the run");
        assert!(
            !forwarded.join().expect("forward the run's stderr"),
            "the {ending} run's deliberate failure printed a panic"
        );
        match ending {
            "panic" => assert_eq!(status.code(), Some(101), "the run failed"),
            _ => {
                assert_eq!(status.signal(), Some(libc::SIGKILL), "the run was killed");
                crate::scratch_apfs::reclaim_dead_root(&root)
                    .expect("reclaim exactly the killed fixture's root");
            }
        }
        for image in &images {
            assert!(
                backend
                    .attached_whole_devices(image)
                    .expect("attachment inventory")
                    .is_empty(),
                "{ending}: {} is still attached",
                image.display()
            );
            assert!(!image.exists(), "{ending}: {} remains", image.display());
        }
        let left = crate::scratch_apfs::processes::processes_in(|cwd| cwd.starts_with(&root))
            .expect("list the processes working under the run's root");
        assert_eq!(left, [], "{ending}: worker {worker} outlived its run");
        assert!(!root.exists(), "{ending}: {} remains", root.display());
    }
}
/// Block until `pid`, which is not this process's child, exits, on the kernel's exit event. A
/// process already gone, or already exiting, has exited. The device tests reach no cowshed-core
/// internals, so this is the test's own copy of the supervisor manager's waiter.
#[cfg(target_os = "macos")]
fn wait_for_exit(pid: libc::pid_t) -> io::Result<()> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs are plain data that outlive the call; `queue` is live until
    // the close below.
    let result = unsafe {
        let change = libc::kevent {
            ident,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let mut event = std::mem::zeroed::<libc::kevent>();
        let returned = libc::kevent(queue, &change, 1, &mut event, 1, std::ptr::null());
        let outcome = match returned {
            ..0 => Err(io::Error::last_os_error()),
            // A registration that failed comes back as an event carrying the error.
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(()),
        };
        libc::close(queue);
        outcome
    };
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        other => other,
    }
}

/// The fixture root the killed-caller attach pair shares through the environment.
#[cfg(target_os = "macos")]
const FIXTURE_KILL_ROOT: &str = "COWSHED_TEST_FIXTURE_KILL_ROOT";

/// A disk-image attach outlives its caller: the caller is ended the way an agent tool
/// kills a tree — SIGTERM to every process working in its root, then SIGKILL to the
/// caller itself — mid-attach. The detached disk-image command finishes regardless, so
/// the next invocation (this parent, after the kill) finds the image attached and
/// releases it cleanly: no whole device of the image remains, no image driver still holds the
/// file, nothing keeps running in its root, and no diskimagesiod is orphaned. The host's
/// leaked-device count is the lane's check (specs/cowshed/08_testing.md, "Host limit"), not one
/// test's on a shared host.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_a_disk_image_attach_survives_its_callers_kill() {
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let orphans_before: std::collections::BTreeSet<libc::pid_t> =
        crate::disk_image_helpers::OrphanedHelpers::read()
            .expect("helper reading before")
            .orphans()
            .map(|orphans| orphans.pids().iter().copied().collect())
            .unwrap_or_default();
    let root = ScratchRoot::new_owned("fixture-kill").expect("owned scratch root");
    let image = root.path().join("attach.dmg");
    // Releases and deletes the image if the test fails before it does.
    let _cleanup = RealImageCleanup::new(&backend, image.clone());
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "tests::real_apfs_disk_image_attach_outliving_a_killed_caller",
            "--ignored",
            "--nocapture",
        ])
        .env(FIXTURE_KILL_ROOT, root.path())
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Its step log reaches this test's output; an unread pipe would fill and stop it.
        .stderr(Stdio::inherit())
        .spawn_locked()
        .expect("spawn the attaching caller");
    let caller = libc::pid_t::try_from(child.id()).expect("caller pid");
    let in_root = |cwd: &Path| cwd.starts_with(root.path());
    let mut lines = BufReader::new(child.stdout.take().expect("caller stdout")).lines();
    // Each attach's diskutil is announced once it runs. Stop the caller there, so it cannot
    // move past the attach, and strike only while that diskutil still runs: an announcement
    // read after its attach had already finished is passed over for the next one.
    let attach = loop {
        let line = lines
            .next()
            .expect("the caller's attach never came in flight")
            .expect("caller line");
        let Some(announced) = line.strip_prefix("inflight ") else {
            continue;
        };
        let announced: libc::pid_t = announced.parse().expect("announced diskutil pid");
        let mut status = 0;
        // SAFETY: kill and waitpid only name the caller, this test's unreaped child;
        // WUNTRACED returns once the stop has taken effect.
        unsafe {
            assert_eq!(libc::kill(caller, libc::SIGSTOP), 0, "stop the caller");
            assert_eq!(libc::waitpid(caller, &mut status, libc::WUNTRACED), caller);
        }
        assert!(libc::WIFSTOPPED(status), "the caller ended before its stop");
        let tree =
            crate::scratch_apfs::processes::processes_in(in_root).expect("list the caller's tree");
        if tree.iter().any(|&(pid, _)| pid == announced) {
            break announced;
        }
        // SAFETY: as above.
        assert_eq!(unsafe { libc::kill(caller, libc::SIGCONT) }, 0, "resume");
    };
    // The tree-killer's two strokes: SIGTERM to every process working in the root
    // (the disk-image child among them, which holds it blocked), SIGKILL to the caller.
    for (pid, _) in
        crate::scratch_apfs::processes::processes_in(in_root).expect("list the caller's tree")
    {
        // SAFETY: kill only names a process; a pid gone since the listing reports ESRCH.
        unsafe { libc::kill(pid, libc::SIGTERM) };
    }
    child.kill().expect("kill the caller mid-attach");
    let _ = child.wait();
    lines.for_each(drop);
    // The detached attach, and anything else still working in the root, ends on its own:
    // wait for each one's exit event, then reconcile as the next invocation does.
    loop {
        let left = crate::scratch_apfs::processes::processes_in(in_root).expect("list the root");
        if left.is_empty() {
            break;
        }
        for (pid, _) in left {
            wait_for_exit(pid).expect("watch a detached disk child's exit");
        }
    }
    assert!(
        !backend
            .attached_whole_devices(&image)
            .expect("attachment inventory")
            .is_empty(),
        "the killed caller's attach (diskutil {attach}) did not complete"
    );
    backend
        .release_image(&image, DetachIntent::Release)
        .expect("release the attach the dead caller finished");
    assert!(
        backend
            .attached_whole_devices(&image)
            .expect("attachment inventory")
            .is_empty(),
        "the released image is still attached"
    );
    // The image is free to attach again: an attachment holds its file exclusively
    // (01_storage.md, "Images"), and detached growth's own nonblocking exclusive open
    // succeeds, so no image driver still holds it — proved without another attach. The
    // released image is then deleted, so a passing run retains no fixture.
    SystemCommandRunner
        .grow_image(&image, capacity("2g"))
        .expect("no image driver still holds the released image");
    backend
        .delete_image(&image)
        .expect("delete the released image");
    assert_eq!(
        crate::scratch_apfs::processes::processes_in(in_root).expect("list the root"),
        [],
        "a process outlived the released image in its root"
    );
    let orphans_after: std::collections::BTreeSet<libc::pid_t> =
        crate::disk_image_helpers::OrphanedHelpers::read()
            .expect("helper reading after")
            .orphans()
            .map(|orphans| orphans.pids().iter().copied().collect())
            .unwrap_or_default();
    assert!(
        orphans_after.is_subset(&orphans_before),
        "new orphaned diskimagesiod helpers: {:?}",
        orphans_after
            .difference(&orphans_before)
            .collect::<Vec<_>>()
    );
}

/// The caller [`real_apfs_a_disk_image_attach_survives_its_callers_kill`] kills: it
/// attaches its image, detaches it, and begins again, so the kill lands mid-attach.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "spawned by real_apfs_a_disk_image_attach_survives_its_callers_kill"]
fn real_apfs_disk_image_attach_outliving_a_killed_caller() {
    let Some(root) = std::env::var_os(FIXTURE_KILL_ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    std::env::set_current_dir(&root).expect("work in the fixture root");
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let image = root.join("attach.dmg");
    crate::blank_image::blank_image(&image);
    // Announce each attach's own diskutil by pid once it runs, so the parent kills
    // mid-attach, not during the inventory reads before it or the eject after it: the detach
    // is a `diskutil eject` too, and it runs only once the attach returned, so a diskutil
    // working in this root while the loop is attaching is the attach itself.
    let watched = root.clone();
    let attaching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watching = std::sync::Arc::clone(&attaching);
    std::thread::spawn(move || {
        let mut announced = std::collections::BTreeSet::new();
        loop {
            let found =
                crate::scratch_apfs::processes::processes_in(|cwd| cwd.starts_with(&watched))
                    .unwrap_or_default();
            for (pid, _) in found {
                let mut path = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
                // SAFETY: `path` is writable for its whole length, the size passed.
                let length =
                    unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
                let diskutil = usize::try_from(length).is_ok_and(|length| {
                    length > 0 && String::from_utf8_lossy(&path[..length]).ends_with("/diskutil")
                });
                if diskutil
                    && watching.load(std::sync::atomic::Ordering::SeqCst)
                    && announced.insert(pid)
                {
                    println!("inflight {pid}");
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    loop {
        println!("attaching");
        attaching.store(true, std::sync::atomic::Ordering::SeqCst);
        let attachment = backend
            .attach_verified(&image)
            .expect("attach a fixture image");
        attaching.store(false, std::sync::atomic::Ordering::SeqCst);
        backend
            .detach(&attachment, DetachIntent::Release)
            .expect("detach a fixture image");
    }
}

/// One real-image fixture run, ended as [`FIXTURE_ENDING`] says: it fails or waits to be
/// killed once its images are attached and its worker started. The parent owns recovery of
/// this exact root after a kill; no child starts a host-wide sweep of unrelated runs.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "spawned by real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end"]
fn real_apfs_fixture_run_ended_by_its_parent() {
    let Some(ending) = std::env::var_os(FIXTURE_ENDING) else {
        return;
    };
    let root = ScratchRoot::new_owned("fixture-run").expect("owned scratch root");
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    for (name, extension) in [("first", IMAGE_EXTENSION), ("retained/second", "asif")] {
        let image = root.path().join(name).with_extension(extension);
        let raw = image.with_extension(IMAGE_EXTENSION);
        crate::blank_image::blank_image(&raw);
        if extension == IMAGE_EXTENSION {
            let mount = root.path().join(format!("{name}-volume"));
            fs::create_dir(&mount).expect("a fixture mountpoint");
            backend
                .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)
                .expect("attach and mount a RAW fixture");
        } else {
            let converted = SystemCommandRunner
                .run(&CommandRequest::new(
                    "/usr/sbin/diskutil",
                    [
                        OsString::from("image"),
                        OsString::from("create"),
                        OsString::from("from"),
                        OsString::from("--format"),
                        OsString::from("ASIF"),
                        raw.as_os_str().to_owned(),
                        image.as_os_str().to_owned(),
                    ],
                ))
                .expect("convert the detached fixture input");
            assert!(
                converted.succeeded(),
                "convert retained fixture input: {converted:?}"
            );
            assert!(
                matches!(
                    backend.attach_verified(&image),
                    Err(ApfsError::InvalidImagePath(_))
                ),
                "ordinary runtime attachment still refuses a retained ASIF input"
            );
            // Attached only: its release is the claim, and a mount would add none.
            let record = FixtureRetainedInput(&image);
            let input = RetainedInput::named_by(&record, &image)
                .expect("the fixture records only its own converted input");
            backend
                .attach_retained_verified(&input)
                .expect("attach the recorded historical input");
        }
        println!("{FIXTURE_IMAGE}{}", image.display());
    }
    // Bounded, so a worker no teardown or sweep ends still ends; a passing run ends it within
    // seconds.
    #[expect(
        clippy::zombie_processes,
        reason = "the worker must outlive this run; the scratch root or the next sweep ends it"
    )]
    let worker = cowshed_os::fork_lock::Spawn::spawn_locked(
        std::process::Command::new("/bin/sleep")
            .arg("600")
            .current_dir(root.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    )
    .expect("start a worker in the root");
    println!("{FIXTURE_WORKER}{} {}", worker.id(), root.path().display());
    if ending == "panic" {
        // Unwinds through the scratch root's teardown as a failed assertion does, without the
        // panic hook: its `panicked at` would read as the parent's own assertion failure.
        std::panic::resume_unwind(Box::new("the fixture's run fails with its images attached"));
    }
    // Killed here; the parent's stdin closes only if it failed first.
    let _ = io::stdin().read_line(&mut String::new());
}
