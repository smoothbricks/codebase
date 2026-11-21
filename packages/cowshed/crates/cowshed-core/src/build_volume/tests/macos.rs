//! A thread's kernel wait on macOS: its Mach port's basic info.

/// A thread as the kernel names it: its Mach port.
pub(super) type Thread = libc::mach_port_t;

/// The calling thread.
pub(super) fn this_thread() -> Thread {
    // SAFETY: names this thread's own Mach port and transfers no right.
    unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) }
}

/// Whether `thread` sleeps interruptibly in the kernel, as a flock waiter does. A thread
/// that already ended answers no: it answered before it ended.
pub(super) fn waits_in_kernel(thread: Thread) -> bool {
    let mut info = std::mem::MaybeUninit::<libc::thread_basic_info>::zeroed();
    let mut count = libc::THREAD_BASIC_INFO_COUNT;
    let flavor = libc::thread_flavor_t::try_from(libc::THREAD_BASIC_INFO).unwrap();
    // SAFETY: `info` has room for the `count` integers the basic flavor writes.
    let status = unsafe { libc::thread_info(thread, flavor, info.as_mut_ptr().cast(), &mut count) };
    // SAFETY: a successful call wrote the whole basic info.
    status == libc::KERN_SUCCESS
        && unsafe { info.assume_init() }.run_state == libc::TH_STATE_WAITING
}
