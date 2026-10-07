#[cfg(target_os = "macos")]
pub(crate) mod build_volumes;
pub mod commitment_feed;
pub mod job_groups;
mod job_resources;
pub(crate) mod nx_daemon;
#[cfg(target_os = "macos")]
pub mod process_events;
pub mod process_stream;
pub mod process_tree;
pub mod process_usage;
pub mod project;
pub mod shell_host;
mod shell_job;
pub mod shell_pool;
pub mod shell_watch;
pub mod supervisor;
pub mod supervisor_manager;
pub mod supervisor_socket;

pub use project::{
    JobAnswer, ProjectDescriptor, ProjectRuntime, ProjectRuntimeHost, RecoveryScope,
    RuntimeLogChunk, WorkspaceSnapshot,
};
pub use supervisor::{
    CheckpointBarrier, CommitmentDraft, CommitmentPublisher, CommitmentPublisherHandle, LogChunk,
    SessionSnapshot, SessionToken, WorkspaceAuthoritySnapshot, WorkspaceSupervisor,
    WorkspaceSupervisorConfig, WorkspaceSupervisorHandle,
};

#[cfg(all(test, target_os = "linux"))]
mod cgroup_environment_probe {
    use crate::fork_lock::Run as _;

    #[test]
    fn report_the_linux_cgroup_environment() {
        let script = r#"
set -x
uname -a
id
cat /proc/self/status | grep -E 'Cap|NoNewPrivs|Seccomp'
cat /proc/self/cgroup
grep cgroup /proc/self/mountinfo
cat /sys/fs/cgroup/cgroup.controllers
cat /sys/fs/cgroup/cgroup.subtree_control
own=/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)
echo "own=$own"
ls -la "$own"
cat "$own/cgroup.controllers" "$own/cgroup.subtree_control" "$own/cgroup.type"
cat "$own/cgroup.procs" | wc -l
parent=$(dirname "$own"); ls -ld "$parent"; cat "$parent/cgroup.subtree_control"
cat "$own/cpu.stat" "$own/memory.current" "$own/memory.peak" "$own/io.stat"
mkdir "$own/probe-child" && echo MKDIR_OK && ls -la "$own/probe-child" && rmdir "$own/probe-child"
sudo -n true && echo SUDO_OK
command -v systemd-run && systemd-run --user --scope -p Delegate=yes true && echo USER_SCOPE_OK
systemctl --user show -p Delegate,ControlGroup 2>&1 | head -5
ls -l /run/user/$(id -u) 2>&1 | head -3
unshare -Ur -C sh -c 'id; cat /proc/self/cgroup' && echo USERNS_OK
cat /proc/sys/kernel/unprivileged_userns_clone /proc/sys/user/max_user_namespaces
env | grep -E '^(INVOCATION_ID|SYSTEMD|RUNNER|GITHUB_ACTIONS|container)'
cat /proc/1/cgroup; ls -la /sys/fs/cgroup | head -40
test -w "$own/cgroup.procs" && echo OWN_PROCS_WRITABLE
test -w "$own/cgroup.subtree_control" && echo OWN_SUBTREE_WRITABLE
stat -c '%U:%G %a %n' "$own" "$own/cgroup.procs" "$own/cgroup.subtree_control" "$parent/cgroup.procs"
cat "$own/cgroup.events" "$own/cgroup.max.depth" "$own/cgroup.max.descendants"
sudo -n systemd-run --scope -p Delegate=yes --uid=$(id -u) --gid=$(id -g) sh -c 'cat /proc/self/cgroup; d=/sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup); stat -c "%U %n" "$d" "$d/cgroup.procs"; cat "$d/cgroup.controllers"' && echo SUDO_DELEGATED_SCOPE_OK
ls /sys/fs/cgroup/*.slice 2>&1 | head -20
"#;
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .output_locked()
            .expect("sh runs");
        panic!(
            "CGROUP PROBE\n--stdout--\n{}\n--stderr--\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
