//! A real JSON publication, awaited by native directory writes rather than elapsed time.
//!
//! The watch runs on Tokio's reactor through [`tokio::io::unix::AsyncFd`] (a non-blocking
//! `kqueue` on macOS, a non-blocking `inotify` descriptor on Linux) rather than a blocking
//! `spawn_blocking` thread, so cancelling or bounding [`read_when`] with `tokio::time::timeout`
//! drops the watch immediately and never hangs runtime teardown.
use serde_json::Value;
use std::path::{Path, PathBuf};

pub async fn read_when(path: PathBuf, ready: impl Fn(&Value) -> bool + Send + 'static) -> Value {
    let watch = DirectoryWatch::arm(path.parent().expect("the publication's directory"));
    loop {
        match std::fs::read(&path) {
            Ok(bytes) => {
                let value = serde_json::from_slice(&bytes).expect("a JSON publication");
                if ready(&value) {
                    return value;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("read publication {}: {error}", path.display()),
        }
        watch.wait().await;
    }
}

#[cfg(target_os = "macos")]
struct DirectoryWatch {
    queue: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
    _directory: std::fs::File,
}

#[cfg(target_os = "macos")]
impl DirectoryWatch {
    fn arm(path: &Path) -> Self {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let directory = std::fs::File::open(path).expect("open the publication directory");
        // SAFETY: kqueue takes no arguments; its new descriptor is checked before use.
        let raw = unsafe { libc::kqueue() };
        assert!(raw >= 0, "kqueue: {}", std::io::Error::last_os_error());
        // SAFETY: this value alone owns the newly returned descriptor.
        let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        let watch = libc::kevent {
            ident: usize::try_from(directory.as_raw_fd()).expect("a descriptor"),
            filter: libc::EVFILT_VNODE,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_WRITE,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one live change, no event output. Subscribe before the first read so
        // a publication between a read and its wait cannot be missed.
        let added = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &watch,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        assert_eq!(
            added,
            0,
            "watch {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        // macOS kqueue descriptors support `EVFILT_READ` only (registering `EVFILT_WRITE`
        // returns `EINVAL`), so request readable interest alone.
        let queue = tokio::io::unix::AsyncFd::with_interest(queue, tokio::io::Interest::READABLE)
            .expect("register kqueue on tokio reactor");
        Self {
            queue,
            _directory: directory,
        }
    }

    async fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        loop {
            let mut guard = self
                .queue
                .readable()
                .await
                .expect("wait for publication readiness");
            let mut event = std::mem::MaybeUninit::<libc::kevent>::zeroed();
            // SAFETY: non-blocking drain of one pending kevent from our live queue.
            let received = unsafe {
                libc::kevent(
                    self.queue.get_ref().as_raw_fd(),
                    std::ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    &zero,
                )
            };
            if received < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                panic!("wait for publication: {error}");
            }
            if received == 0 {
                guard.clear_ready();
                continue;
            }
            // SAFETY: kevent returned exactly one initialized event.
            let event = unsafe { event.assume_init() };
            assert_eq!(
                event.flags & libc::EV_ERROR,
                0,
                "publication watch: errno {}",
                { event.data }
            );
            guard.clear_ready();
            return;
        }
    }
}

#[cfg(target_os = "linux")]
struct DirectoryWatch {
    events: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
}

#[cfg(target_os = "linux")]
impl DirectoryWatch {
    fn arm(path: &Path) -> Self {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;
        // SAFETY: flags only; a negative answer is checked before taking custody.
        let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        assert!(
            raw >= 0,
            "inotify_init1: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: this value alone owns the newly returned descriptor.
        let events = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("a directory path");
        // SAFETY: the path is NUL-terminated, live for the call and the descriptor is owned.
        let added = unsafe {
            libc::inotify_add_watch(
                events.as_raw_fd(),
                name.as_ptr(),
                libc::IN_CREATE | libc::IN_MOVED_TO,
            )
        };
        assert!(
            added >= 0,
            "watch {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        let events = tokio::io::unix::AsyncFd::with_interest(events, tokio::io::Interest::READABLE)
            .expect("register inotify on tokio reactor");
        Self { events }
    }

    async fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut buffer = [0_u8; 4096];
        loop {
            let mut guard = self
                .events
                .readable()
                .await
                .expect("wait for publication readiness");
            // SAFETY: reading from our live non-blocking inotify fd into a valid buffer.
            let n = unsafe {
                libc::read(
                    self.events.get_ref().as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if n > 0 {
                // Drain any further queued events before returning.
                loop {
                    // SAFETY: same live non-blocking inotify fd.
                    let more = unsafe {
                        libc::read(
                            self.events.get_ref().as_raw_fd(),
                            buffer.as_mut_ptr().cast(),
                            buffer.len(),
                        )
                    };
                    if more <= 0 {
                        break;
                    }
                }
                guard.clear_ready();
                return;
            }
            if n == 0 {
                panic!("publication watch ended");
            }
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                guard.clear_ready();
                continue;
            }
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            panic!("wait for publication: {error}");
        }
    }
}
