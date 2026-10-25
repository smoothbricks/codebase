//! Deleting released images (05_gateway.md, "Deleting released images").
//!
//! A verb releases an image by renaming it into a `.deleting` directory ([`super::release`]),
//! then starts its deletion in the background ([`delete_in_background`]) and returns: a detached
//! child of the cowshed process frees that one entry in bounded steps ([`dispatch`]). Should the
//! child die, or never start, the gateway's startup pass ([`startup_pass`]) finishes the entry.
//! Nothing polls and nothing is queued.

use std::io;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use cowshed_os::fork_lock::Spawn as _;

use super::release::{DELETING_DIRECTORY, Pass, Released, drain, free_committed};

/// `<store>/.deleting`: where a project's final unbinding moves the released images its tree still
/// holds, so the tree goes at once and they are freed by their deletion children or the startup
/// scan. Entries are named `<fresh id>.dmg` ([`crate::build_volume::BuildVolumeId`]).
pub fn store_deleting(store: &Path) -> PathBuf {
    store.join(DELETING_DIRECTORY)
}

/// The argument a deletion child is started with, before the entry it frees.
pub const DELETE_VERB: &str = "__delete-released";

/// Where a deletion child's stderr goes, below the home directory: the host's daemon log, the
/// launchd `StandardErrorPath` the gateway, its supervisors and their children already write
/// (13_telemetry.md, "The one text-file survivor").
pub const DAEMON_LOG: &str = "Library/Logs/cowshed/daemon-stderr.log";

/// What a release starts to free its entry (05_gateway.md, "Deleting released images"): the
/// program, run as `<program> __delete-released <entry>`, and the log its stderr appends to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionChild {
    pub program: Arc<Path>,
    pub log: Arc<Path>,
}

impl DeletionChild {
    /// `program` logging to `home`'s daemon log: what a host store's releases start.
    pub fn host(program: &Path, home: &Path) -> Self {
        Self {
            program: Arc::from(program),
            log: Arc::from(home.join(DAEMON_LOG)),
        }
    }
}

/// Run as the deletion child when the arguments say so, before anything else starts: free the
/// one entry named and answer the exit status. `None` when this process is not one.
pub fn dispatch() -> Option<i32> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next()? != DELETE_VERB {
        return None;
    }
    let (Some(entry), None) = (arguments.next(), arguments.next()) else {
        cowshed_os::eprintln!("cowshed: {DELETE_VERB} takes exactly one released image");
        return Some(2);
    };
    Some(if delete(Path::new(&entry)) { 0 } else { 1 })
}

/// Free `entry`, saying a failure; the entry then stays for the gateway's next start.
fn delete(entry: &Path) -> bool {
    match free_committed(entry) {
        Ok(Released::Freed { .. } | Released::Absent | Released::Busy) => true,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: released image {} stays for the gateway's next start: {error}",
                entry.display()
            );
            false
        }
    }
}

/// Start freeing `entry`, a release this process just committed, and return at once. The
/// deletion runs in a detached child, `<program> __delete-released <entry>` (its own process
/// group, so the verb's terminal signals do not reach it), that outlives this process. It holds
/// nothing of the verb's: it works in `/`, so no checkout a later `rm` asks about is its
/// working directory; its stderr appends to `child.log`, never the verb's own, whose reader
/// (a terminal, a script's pipe, an agent's capture) would otherwise wait for the child's end of
/// file and see the verb answer only once its image was freed; and it inherits no descriptor but
/// its stdio, sealed at the one child-descriptor boundary (`crate::exec::prepare_child_descriptors`).
/// A verb run under `cowshed land-turn` holds its repository's turn through an inherited
/// descriptor; measured 2026-10-25, two deletion children of sb-train4's land kept that
/// descriptor, and with it smoothbricks' land turn, after the land exited, until their frees ended. A child that cannot start, or a
/// log that cannot be opened, is the release's error; one that ends badly is said, and the
/// gateway's next start frees what it left.
pub fn delete_in_background(child: &DeletionChild, entry: &Path) -> io::Result<()> {
    let program = &child.program;
    let log = daemon_log(&child.log)?;
    let mut command = Command::new(program.as_ref());
    command
        .arg(DELETE_VERB)
        .arg(entry)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .process_group(0);
    crate::exec::prepare_child_descriptors(&mut command).map_err(|failure| {
        io::Error::new(
            failure.source.kind(),
            format!(
                "cannot seal the descriptors of {} to delete released image {}: {}",
                program.display(),
                entry.display(),
                failure.source
            ),
        )
    })?;
    let mut child = command.spawn_locked().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot start {} to delete released image {}: {error}",
                program.display(),
                entry.display()
            ),
        )
    })?;
    // Reaped here while this process lives, a bad end said; reparented to launchd once this
    // process exits.
    let entry = entry.to_owned();
    std::thread::Builder::new()
        .name("image-deleter-reaper".to_owned())
        .spawn(move || match child.wait() {
            Ok(status) if status.success() => {}
            Ok(status) => cowshed_os::eprintln!(
                "cowshed: the deletion child of released image {} ended {status}; the gateway's next start frees what it left",
                entry.display()
            ),
            Err(error) => cowshed_os::eprintln!(
                "cowshed: the deletion child of released image {} cannot be reaped: {error}",
                entry.display()
            ),
        })?;
    Ok(())
}

/// `log` opened to append, its directory made first, so a host whose log directory nothing made
/// yet still keeps what its deletion children say.
fn daemon_log(log: &Path) -> io::Result<std::fs::File> {
    let opened = || {
        if let Some(directory) = log.parent() {
            std::fs::create_dir_all(directory)?;
        }
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
    };
    opened().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot open the deletion log {}: {error}", log.display()),
        )
    })
}

/// The gateway's startup scan of the store at `store`: every project's `build/.deleting` and
/// `sessions/.deleting` (archived projects under `.retired-projects/` included) and the store's own
/// `.deleting`. It finishes whatever a deletion child left (it died, or never started); an entry
/// another process holds is left to it.
pub fn startup_pass(store: &Path) -> Pass {
    let mut pass = crate::build_volume::drain_store_released(store);
    pass.add(super::drain_store_session_releases(store));
    let deleting = store_deleting(store);
    match drain(&deleting, crate::build_volume::is_image_name) {
        Ok(store_level) => pass.add(store_level),
        Err(error) => pass.left.push(super::release::Left {
            path: deleting,
            why: super::release::Why::Failed(error.to_string()),
        }),
    }
    pass
}

/// What the startup pass says: what it freed, and each entry or directory it left.
pub fn startup_report(pass: &Pass) -> Vec<String> {
    let freed = (pass.drained.images > 0).then(|| {
        format!(
            "cowshed: deleted {} released image(s), {} bytes, left from before this gateway",
            pass.drained.images, pass.drained.bytes
        )
    });
    freed
        .into_iter()
        .chain(pass.left.iter().map(|left| format!("cowshed: {left}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn store(label: &str) -> PathBuf {
        let root = PathBuf::from(format!(
            "/private/tmp/csdel-{label}-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        fs::create_dir_all(&root).expect("store");
        root
    }

    fn released(path: &Path) {
        fs::create_dir_all(path.parent().expect("deleting directory")).expect("directory");
        fs::write(path, vec![7_u8; 64 * 1024]).expect("released image");
    }

    fn volume(store: &Path) -> PathBuf {
        store.join(format!(
            "acme/widget/build/.deleting/{}.dmg",
            crate::build_volume::BuildVolumeId::mint()
        ))
    }

    /// The deletion child holds nothing of the verb that started it: its stderr appends to the
    /// daemon log it is given, never the verb's own, whose reader would otherwise wait for the
    /// child's end of file (an `rm` piped into a script read as taking as long as the free), and
    /// it works in `/`, never in the verb's working directory. The program stands in for the
    /// cowshed binary: it says its arguments and directory on stderr, then marks itself done.
    #[test]
    fn the_deletion_child_logs_to_the_daemon_log_from_the_root_directory() {
        use std::os::unix::fs::PermissionsExt as _;
        let store = store("log");
        let program = store.join("deleter");
        fs::write(
            &program,
            "#!/bin/sh\necho \"$1 $2 $(pwd)\" >&2\n: > \"$2.done\"\n",
        )
        .expect("program");
        fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).expect("executable");
        let entry = volume(&store);
        released(&entry);
        let log = store.join("Logs/cowshed/daemon-stderr.log");
        let child = DeletionChild {
            program: Arc::from(program.as_path()),
            log: Arc::from(log.as_path()),
        };
        delete_in_background(&child, &entry).expect("started");
        let done = entry.with_extension("dmg.done");
        while !done.exists() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            fs::read_to_string(&log).expect("the daemon log"),
            format!("{DELETE_VERB} {} /\n", entry.display())
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }

    /// The deletion child inherits none of the verb's descriptors: a verb run under
    /// `cowshed land-turn` holds its repository's turn through an inheritable descriptor on the
    /// turn's ticket, and a child that kept it held the turn until its free ended. Here the verb
    /// holds an exclusive lock through such a descriptor, starts the child, and closes its own: the
    /// lock is free while the child still runs. The program stands in for the cowshed binary; it
    /// says it started through one FIFO and waits on another, so nothing is timed.
    #[test]
    fn the_deletion_child_holds_none_of_the_verbs_descriptors() {
        use std::io::{Read as _, Write as _};
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::PermissionsExt as _;
        let store = store("fds");
        let program = store.join("deleter");
        fs::write(
            &program,
            "#!/bin/sh\nprintf started > \"$2.ready\"\nread line < \"$2.go\"\n",
        )
        .expect("program");
        fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).expect("executable");
        let entry = volume(&store);
        released(&entry);
        let fifo = |suffix: &str| {
            let path = PathBuf::from(format!("{}.{suffix}", entry.display()));
            let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("path");
            // SAFETY: `name` is a NUL-terminated path that outlives the call.
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");
            path
        };
        let (ready, go) = (fifo("ready"), fifo("go"));
        let ticket = store.join("ticket");
        let held = fs::File::create(&ticket).expect("ticket");
        // SAFETY: plain descriptor operations on a descriptor this test owns.
        unsafe {
            assert_eq!(libc::flock(held.as_raw_fd(), libc::LOCK_EX), 0, "lock");
            assert_eq!(
                libc::fcntl(held.as_raw_fd(), libc::F_SETFD, 0),
                0,
                "inheritable"
            );
        }
        let child = DeletionChild {
            program: Arc::from(program.as_path()),
            log: Arc::from(store.join("daemon-stderr.log").as_path()),
        };
        delete_in_background(&child, &entry).expect("started");
        let mut said = String::new();
        fs::File::open(&ready)
            .expect("the child's ready FIFO")
            .read_to_string(&mut said)
            .expect("the child says it started");
        assert_eq!(said, "started");
        drop(held);
        let again = fs::File::open(&ticket).expect("ticket again");
        // SAFETY: a non-blocking lock attempt on a descriptor this test owns.
        let locked = unsafe { libc::flock(again.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        let error = io::Error::last_os_error();
        fs::OpenOptions::new()
            .write(true)
            .open(&go)
            .and_then(|mut go| go.write_all(b"\n"))
            .expect("let the child end");
        assert_eq!(
            locked, 0,
            "the running deletion child keeps the verb's lock: {error}"
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }

    /// The startup pass finishes every release left from before the gateway, in every project's,
    /// every archived project's and the store's own deleting directory, names a stray, and leaves a
    /// legacy `.releasing` alone; an owner marker is no project.
    #[test]
    fn the_startup_pass_frees_every_leftover() {
        let store = store("startup");
        let left = [
            volume(&store),
            store.join(format!(
                ".deleting/{}.dmg",
                crate::build_volume::BuildVolumeId::mint()
            )),
            store.join(format!(
                "acme/widget/sessions/.deleting/topic-{}.dmg",
                "1".repeat(32)
            )),
            store.join(format!(
                ".retired-projects/0123/build/.deleting/{}.dmg",
                crate::build_volume::BuildVolumeId::mint()
            )),
        ];
        for entry in &left {
            released(entry);
        }
        let stray = store.join("acme/widget/sessions/.deleting/notes.txt");
        fs::write(&stray, b"not an image").expect("stray");
        fs::write(store.join("acme/.cowshed-owner"), b"owner").expect("owner marker");
        fs::write(store.join(".retired-projects/0123.json"), b"{}").expect("archive record");
        let legacy = store.join(format!(
            "acme/widget/build/.releasing/{}.dmg",
            crate::build_volume::BuildVolumeId::mint()
        ));
        released(&legacy);

        // Main's image lock is held, as a client's opening or a fork of main holds it: the pass
        // takes no project's image lock, so it frees all the same and never cuts under one.
        let main_lock =
            fs::File::create(store.join("acme/widget/main.dmg.lock")).expect("main's image lock");
        main_lock.try_lock().expect("hold main's image lock");
        let pass = startup_pass(&store);
        assert_eq!(pass.drained.images, left.len(), "{pass:?}");
        assert!(left.iter().all(|entry| !entry.exists()));
        assert!(legacy.exists(), "no legacy directory is scanned");
        assert_eq!(
            startup_report(&pass),
            vec![
                format!(
                    "cowshed: deleted {} released image(s), {} bytes, left from before this gateway",
                    left.len(),
                    pass.drained.bytes
                ),
                format!(
                    "cowshed: {} is not a released image; it is left",
                    stray.display()
                ),
            ]
        );
        fs::remove_dir_all(&store).expect("cleanup");
    }
}
