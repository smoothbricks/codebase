//! A real JSON publication, awaited by native directory writes rather than elapsed time.
use serde_json::Value;
use std::path::{Path, PathBuf};

pub async fn read_when(path: PathBuf, ready: impl Fn(&Value) -> bool + Send + 'static) -> Value {
    tokio::task::spawn_blocking(move || {
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
            watch.wait();
        }
    })
    .await
    .expect("the publication watcher")
}

#[cfg(target_os = "macos")]
struct DirectoryWatch {
    queue: std::os::fd::OwnedFd,
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
        Self {
            queue,
            _directory: directory,
        }
    }

    fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut event = std::mem::MaybeUninit::<libc::kevent>::zeroed();
        loop {
            // SAFETY: a live queue and room for one event. The null timeout waits for
            // the kernel's event; the target owns any no-progress bound.
            let received = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    std::ptr::null(),
                )
            };
            if received < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            assert_eq!(
                received,
                1,
                "wait for publication: {}",
                std::io::Error::last_os_error()
            );
            // SAFETY: kevent returned exactly one initialized event.
            let event = unsafe { event.assume_init() };
            assert_eq!(
                event.flags & libc::EV_ERROR,
                0,
                "publication watch: errno {}",
                { event.data }
            );
            return;
        }
    }
}

#[cfg(target_os = "linux")]
struct DirectoryWatch {
    events: std::fs::File,
}

#[cfg(target_os = "linux")]
impl DirectoryWatch {
    fn arm(path: &Path) -> Self {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;
        // SAFETY: flags only; a negative answer is checked before taking custody.
        let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        assert!(
            raw >= 0,
            "inotify_init1: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: this value alone owns the newly returned descriptor.
        let events = unsafe { std::fs::File::from_raw_fd(raw) };
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
        Self { events }
    }

    fn wait(&self) {
        use std::io::Read as _;
        let mut buffer = [0_u8; 4096];
        loop {
            match (&self.events).read(&mut buffer) {
                Ok(0) => panic!("publication watch ended"),
                Ok(_) => return,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => panic!("wait for publication: {error}"),
            }
        }
    }
}
