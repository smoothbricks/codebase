//! Child descriptor preparation on Linux: one `close_range(2)` marks every non-stdio descriptor
//! close-on-exec between fork and exec, with a bounded per-descriptor walk for a kernel that
//! lacks it.

use std::io;
use std::process::Command;

use super::{
    DESCRIPTOR_PREPARATION_ERRNO, SUPERVISOR_FD_CEILING, SpawnFailure, WrapperStage,
    mark_descriptor_close_on_exec,
};

pub(super) fn mark_non_stdio_close_on_exec(limit: libc::rlim_t) -> io::Result<()> {
    mark_non_stdio_close_on_exec_with(
        limit,
        |first, last, flags| {
            // SAFETY: close_range takes integer bounds and flags only; invalid values return errno.
            let result = unsafe { libc::syscall(libc::SYS_close_range, first, last, flags) };
            close_range_result_with(result, io::Error::last_os_error)
        },
        mark_descriptor_close_on_exec,
    )
}

pub(super) fn prepare_child_descriptors(command: &mut Command) -> Result<(), SpawnFailure> {
    let descriptor_limit = descriptor_limit().map_err(|source| SpawnFailure {
        stage: WrapperStage::PrepareChildDescriptors,
        source,
    })?;

    install_cloexec_pre_exec(command, descriptor_limit, mark_non_stdio_close_on_exec);
    Ok(())
}

fn descriptor_limit_with<GetLimit>(get_limit: GetLimit) -> io::Result<libc::rlim_t>
where
    GetLimit: FnOnce(*mut libc::rlimit) -> libc::c_int,
{
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    if get_limit(limit.as_mut_ptr()) == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a non-negative callback result is the contract that it initialized `limit`.
    Ok(unsafe { limit.assume_init() }.rlim_cur)
}

fn descriptor_limit() -> io::Result<libc::rlim_t> {
    descriptor_limit_with(|limit| {
        // SAFETY: `limit` points to writable storage for one `rlimit`; getrlimit initializes it on success.
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit) }
    })
}

fn install_cloexec_pre_exec(
    command: &mut Command,
    descriptor_limit: libc::rlim_t,
    mark: fn(libc::rlim_t) -> io::Result<()>,
) {
    use std::os::unix::process::CommandExt as _;

    // SAFETY: the closure captures only the numeric limit and function pointer; `mark` performs
    // only async-signal-safe descriptor syscalls and every failure becomes a fixed errno.
    unsafe {
        command.pre_exec(move || {
            mark(descriptor_limit)
                .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
        });
    }
}

fn fallback_descriptor_limit(limit: libc::rlim_t) -> io::Result<libc::c_int> {
    if limit > SUPERVISOR_FD_CEILING as libc::rlim_t {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "descriptor fallback limit exceeds the supervisor FD ceiling",
        ));
    }
    Ok(limit as libc::c_int)
}

fn mark_descriptor_range_close_on_exec_with<MarkDescriptor>(
    limit: libc::rlim_t,
    mut mark_descriptor: MarkDescriptor,
) -> io::Result<()>
where
    MarkDescriptor: FnMut(libc::c_int) -> io::Result<()>,
{
    let limit = fallback_descriptor_limit(limit)?;
    for descriptor in 3..limit {
        mark_descriptor(descriptor)?;
    }
    Ok(())
}

const CLOSE_RANGE_CLOEXEC: libc::c_uint = 1 << 2;

fn close_range_result_with<LastError>(result: libc::c_long, last_error: LastError) -> io::Result<()>
where
    LastError: FnOnce() -> io::Error,
{
    if result == 0 {
        Ok(())
    } else {
        Err(last_error())
    }
}

fn mark_non_stdio_close_on_exec_with<CloseRange, MarkDescriptor>(
    limit: libc::rlim_t,
    close_range: CloseRange,
    mark_descriptor: MarkDescriptor,
) -> io::Result<()>
where
    CloseRange: FnOnce(libc::c_uint, libc::c_uint, libc::c_uint) -> io::Result<()>,
    MarkDescriptor: FnMut(libc::c_int) -> io::Result<()>,
{
    match close_range(3, libc::c_uint::MAX, CLOSE_RANGE_CLOEXEC) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::EINVAL)
            ) =>
        {
            mark_descriptor_range_close_on_exec_with(limit, mark_descriptor)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;

    #[test]
    fn descriptor_limit_reports_the_kernel_soft_limit() {
        let mut expected = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, expected.as_mut_ptr()) },
            0
        );
        let expected = unsafe { expected.assume_init() }.rlim_cur;

        assert!(expected > libc::STDERR_FILENO as libc::rlim_t);
        assert_eq!(descriptor_limit().unwrap(), expected);
    }

    #[test]
    fn descriptor_limit_injection_preserves_boundaries_and_errors() {
        for expected in [
            SUPERVISOR_FD_CEILING as libc::rlim_t - 1,
            SUPERVISOR_FD_CEILING as libc::rlim_t,
            libc::RLIM_INFINITY,
        ] {
            let actual = descriptor_limit_with(|limit| {
                unsafe {
                    limit.write(libc::rlimit {
                        rlim_cur: expected,
                        rlim_max: libc::RLIM_INFINITY,
                    });
                }
                0
            })
            .unwrap();
            assert_eq!(actual, expected);
        }

        let positive_status_limit = 7;
        let actual = descriptor_limit_with(|limit| {
            unsafe {
                limit.write(libc::rlimit {
                    rlim_cur: positive_status_limit,
                    rlim_max: positive_status_limit,
                });
            }
            1
        })
        .unwrap();
        assert_eq!(actual, positive_status_limit);

        unsafe {
            libc::close(-1);
        }
        let error = descriptor_limit_with(|_| -1).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    }

    #[test]
    fn descriptor_fallback_enforces_infinity_ceiling_and_boundaries() {
        for accepted in [
            0,
            SUPERVISOR_FD_CEILING as libc::rlim_t - 1,
            SUPERVISOR_FD_CEILING as libc::rlim_t,
        ] {
            assert_eq!(
                fallback_descriptor_limit(accepted).unwrap(),
                accepted as libc::c_int
            );
        }

        for rejected in [
            SUPERVISOR_FD_CEILING as libc::rlim_t + 1,
            libc::RLIM_INFINITY,
        ] {
            let error = fallback_descriptor_limit(rejected).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "descriptor fallback limit exceeds the supervisor FD ceiling"
            );
        }
    }

    #[test]
    fn close_range_uses_the_cloexec_flag_and_full_non_stdio_bounds() {
        let invocation = RefCell::new(None);
        let fallback_called = std::cell::Cell::new(false);

        mark_non_stdio_close_on_exec_with(
            SUPERVISOR_FD_CEILING as libc::rlim_t,
            |first, last, flags| {
                invocation.replace(Some((first, last, flags)));
                Ok(())
            },
            |_| {
                fallback_called.set(true);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            invocation.into_inner(),
            Some((3, libc::c_uint::MAX, CLOSE_RANGE_CLOEXEC))
        );
        assert_eq!(CLOSE_RANGE_CLOEXEC, 4);
        assert!(!fallback_called.get());
    }

    #[test]
    fn close_range_result_accepts_only_the_success_sentinel() {
        close_range_result_with(0, || panic!("success must not read errno")).unwrap();

        for result in [-1, 1] {
            let error =
                close_range_result_with(result, || io::Error::from_raw_os_error(libc::EPERM))
                    .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EPERM));
        }
    }

    #[test]
    fn close_range_fallback_visits_every_descriptor_below_the_bound() {
        for unavailable_errno in [libc::ENOSYS, libc::EINVAL] {
            let visited = RefCell::new(Vec::new());
            mark_non_stdio_close_on_exec_with(
                7,
                |_, _, _| Err(io::Error::from_raw_os_error(unavailable_errno)),
                |descriptor| {
                    visited.borrow_mut().push(descriptor);
                    Ok(())
                },
            )
            .unwrap();

            assert_eq!(visited.into_inner(), vec![3, 4, 5, 6]);
        }
    }

    #[test]
    fn descriptor_range_obeys_exclusive_limit_at_stdio_boundary() {
        for (limit, expected) in [(2, &[][..]), (3, &[][..]), (4, &[3][..]), (5, &[3, 4][..])] {
            let visited = RefCell::new(Vec::new());
            mark_descriptor_range_close_on_exec_with(limit, |descriptor| {
                visited.borrow_mut().push(descriptor);
                Ok(())
            })
            .unwrap();
            assert_eq!(visited.into_inner(), expected);
        }
    }

    #[test]
    fn close_range_and_fallback_errors_propagate() {
        let close_range_error = mark_non_stdio_close_on_exec_with(
            7,
            |_, _, _| Err(io::Error::from_raw_os_error(libc::EPERM)),
            |_| panic!("fallback must not run for an unexpected close_range error"),
        )
        .unwrap_err();
        assert_eq!(close_range_error.raw_os_error(), Some(libc::EPERM));

        let visited = RefCell::new(Vec::new());
        let fallback_error = mark_non_stdio_close_on_exec_with(
            7,
            |_, _, _| Err(io::Error::from_raw_os_error(libc::ENOSYS)),
            |descriptor| {
                visited.borrow_mut().push(descriptor);
                if descriptor == 5 {
                    Err(io::Error::from_raw_os_error(libc::EIO))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(fallback_error.raw_os_error(), Some(libc::EIO));
        assert_eq!(visited.into_inner(), vec![3, 4, 5]);
    }

    #[test]
    fn descriptor_fallback_marks_every_descriptor_below_its_bound() {
        use std::os::fd::AsRawFd;

        let file = fs::File::open(std::env::temp_dir()).unwrap();
        let descriptor = file.as_raw_fd();
        let original = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(original, -1);
        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, original & !libc::FD_CLOEXEC) },
            -1
        );

        mark_non_stdio_close_on_exec(libc::rlim_t::try_from(descriptor).unwrap() + 1).unwrap();
        let prepared = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, original) },
            -1
        );

        assert_ne!(prepared, -1);
        assert_ne!(prepared & libc::FD_CLOEXEC, 0);
    }
}
