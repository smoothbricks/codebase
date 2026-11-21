//! Linux ships no sampler of another process's stacks that an unprivileged process may run on
//! any peer: `/proc/<pid>/stack` needs `CAP_SYS_ADMIN`, and `ptrace` of a process that is not a
//! descendant is refused under Yama's default scope. A capture therefore answers that none was
//! taken, which a wait recording reports as it does any failed capture.

pub fn capture(pid: i32) -> Result<Vec<u8>, String> {
    Err(format!(
        "stack sampling pid {pid} is unsupported on this platform"
    ))
}
