//! A thread's kernel wait on Linux: its task's state in procfs.

/// A thread as the kernel names it: its task id.
pub(super) type Thread = libc::pid_t;

/// The calling thread.
pub(super) fn this_thread() -> Thread {
    // SAFETY: gettid reads the calling thread's id; it takes no pointers and cannot fail.
    unsafe { libc::gettid() }
}

/// Whether `thread` sleeps interruptibly in the kernel, as a flock waiter does: state `S` in
/// `/proc/self/task/<tid>/stat`, the field after the parenthesised command. A thread that
/// already ended has no such file and answers no: it answered before it ended.
pub(super) fn waits_in_kernel(thread: Thread) -> bool {
    std::fs::read_to_string(format!("/proc/self/task/{thread}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(')')
            .is_some_and(|(_, after)| after.split_whitespace().next() == Some("S"))
    })
}
