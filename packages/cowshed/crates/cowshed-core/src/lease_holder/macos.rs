//! A process's kernel unique id on Darwin: `p_uniqueid`, which no two lives of a pid share.

use std::io;

/// The kernel's unique id of the running process `pid`; `None` once it has exited.
pub(super) fn unique_id(pid: u32) -> io::Result<Option<crate::api::held_lock::DecimalU64>> {
    let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
    Ok(crate::runtime::job_groups::process_record(pid)?
        .map(|record| crate::api::held_lock::DecimalU64::new(record.unique_id)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs::{self, File};
    use std::io;

    use super::unique_id;
    use crate::lease_holder::{Process, clear, record, running_lifecycle};

    /// A lease names this process with its unique id from the moment it is recorded, and records
    /// nothing once cleared.
    #[test]
    fn a_recorded_holder_names_this_life_and_a_cleared_lease_names_nothing() {
        let root = crate::temp_root::TempRoot::new("lease-holder");
        let path = root.join("topic.intent.lock");
        let file = File::create(&path).expect("lease file");
        record(&file, &Process::this().expect("this process")).expect("record");
        let written: Process =
            serde_json::from_slice(&fs::read(&path).expect("read")).expect("parse");
        assert_eq!(written.pid, std::process::id());
        assert_eq!(
            written.unique_id,
            unique_id(std::process::id()).expect("own record")
        );
        assert!(written.unique_id.is_some());
        clear(&file, &path);
        assert!(fs::read(&path).expect("read").is_empty());
    }

    /// Only a marked owner's leases pass a process over (01_storage.md, "Owners are marked, never
    /// inferred"; 02_workspaces.md, "Holders cowshed does not run"): a live process a marked
    /// owner's lease names is found, while one a lease under the reserved `run/` namespace names
    /// (marked or not, among thousands of entries there) or an unmarked directory's lease names is
    /// asked like any other. A marked owner this user cannot list names nothing and fails nothing.
    #[test]
    fn only_a_marked_owners_leases_pass_a_process_over() {
        use cowshed_os::fork_lock::Spawn as _;
        use cowshed_os::test_backstop::ChildBackstop as _;
        use std::os::unix::fs::PermissionsExt as _;
        let root = crate::temp_root::TempRoot::new("lease-owners");
        let store = root.join("store");
        let live = || {
            std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg("read _")
                .stdin(std::process::Stdio::piped())
                .spawn_locked()
                .expect("a live lease holder")
        };
        let lease = |owner: &str, holder: &std::process::Child| {
            let sessions = store
                .join(owner)
                .join("widget")
                .join(crate::repository::SESSIONS_DIRECTORY);
            fs::create_dir_all(&sessions).expect("sessions");
            let record = Process {
                pid: holder.id(),
                unique_id: unique_id(holder.id()).expect("its record"),
                since: "2026-11-21T00:00:00Z".to_owned(),
                command: "cowshed fork main topic".to_owned(),
            };
            assert!(record.unique_id.is_some(), "the holder runs");
            fs::write(
                sessions.join("topic.intent.lock"),
                serde_json::to_vec(&record).expect("record"),
            )
            .expect("lease");
        };
        let mut holders = [live(), live(), live(), live()];
        let [marked, reserved, unmarked, unreadable] = &holders;
        lease("acme", marked);
        crate::storage::bootstrap::write_owner_marker(&store.join("acme")).expect("mark acme");
        lease("run", reserved);
        crate::storage::bootstrap::write_owner_marker(&store.join("run")).expect("mark run");
        for index in 0..4096 {
            fs::write(store.join(format!("run/{index:032x}.sock.lock")), b"").expect("run entry");
        }
        lease("drifter", unmarked);
        lease("locked", unreadable);
        let locked = store.join("locked");
        crate::storage::bootstrap::write_owner_marker(&locked).expect("mark locked");
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).expect("lock owner");
        let denied = fs::read_dir(&locked)
            .map(|_| ())
            .expect_err("this user lists no project there");

        let running = running_lifecycle(&store, |lease| fs::read(lease));

        fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).expect("unlock owner");
        assert_eq!(denied.kind(), io::ErrorKind::PermissionDenied, "{denied}");
        let pid = |holder: &std::process::Child| libc::pid_t::try_from(holder.id()).expect("pid");
        assert_eq!(
            running,
            BTreeSet::from([pid(marked)]),
            "only the marked owner's lease passes its holder over"
        );
        for holder in &mut holders {
            drop(holder.stdin.take());
            holder.wait_bounded().expect("the holder ends");
        }
    }
}
