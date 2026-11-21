//! The loopback connector of a launch (04_sandbox.md, "Linux enforcement").
//!
//! A launch's network namespace has loopback and nothing else. Its clients speak HTTP to a
//! proxy, so one process inside the namespace listens on `127.0.0.1:7644` and copies each
//! connection's bytes, unread, to and from the workspace's gateway socket: a pathname Unix
//! socket, which a namespace does not confine. The connector supplies reachability only. It
//! holds no grant, policy, token or key, and the gateway still authenticates every request.
//!
//! The launcher starts it before it becomes the command, from the launch's user and network
//! namespaces and the host's mount namespace: the mounts that hide controller sockets from the
//! command come later and never cover the connector's upstream. It is forked twice, so the
//! command never has a child it did not start, and it leads its own session, so a signal to the
//! command's process group does not end it. It then gives up everything it does not need: its
//! standard streams and every other descriptor, every capability of the namespace it was born
//! owning, and, under a Landlock ruleset with no rule at all, every file open and (ABI 4) every
//! TCP bind and connect. The command's own Landlock domain cannot trace it, and from ABI 6
//! cannot signal it. It exits when the process that started it, the launch's leader, exits.

use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::os::fd::{AsFd as _, AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use super::failure::Failure;
use super::landlock::{Abi, Network, Ruleset};
use cowshed_gateway_types::LINUX_CONNECTOR_PORT;

/// The one address the connector binds: never another address, never another port
/// ([`LINUX_CONNECTOR_PORT`], the port every Linux job's environment names).
pub(super) const ADDRESS: SocketAddrV4 =
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, LINUX_CONNECTOR_PORT);

/// How long the launcher waits for the connector to say it serves. Everything before that is a
/// handful of system calls on this process's own descriptors.
const READY_WITHIN: Duration = Duration::from_secs(5);

/// What the connector tells its launcher once it serves.
const READY: &[u8] = b"ok";

const WHAT: &str = "the loopback connector";

/// Whether an accepted connection is one the connector serves: it arrived on [`ADDRESS`] from
/// an IPv4 loopback peer. In a namespace whose only interface is loopback nothing else can
/// arrive; a connection that somehow did is closed with no byte sent upstream.
pub(super) fn admitted(local: Option<SocketAddr>, peer: Option<SocketAddr>) -> bool {
    matches!(
        (local, peer),
        (Some(SocketAddr::V4(local)), Some(SocketAddr::V4(peer)))
            if local == ADDRESS && peer.ip().is_loopback()
    )
}

/// The connector's listener, bound in the calling process's network namespace.
pub(super) fn bind() -> io::Result<TcpListener> {
    TcpListener::bind(ADDRESS)
}

/// `socket` as the address a Unix stream connects to.
#[derive(Clone, Copy)]
struct Upstream {
    address: libc::sockaddr_un,
    length: libc::socklen_t,
}

impl Upstream {
    fn at(socket: &Path) -> io::Result<Self> {
        let name = socket.as_os_str().as_bytes();
        // SAFETY: an all-zero `sockaddr_un` is a valid one.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        // One byte of the path stays zero: the name's terminator.
        if name.len() >= address.sun_path.len() || name.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the path does not fit a Unix socket address",
            ));
        }
        address.sun_family = libc::sa_family_t::try_from(libc::AF_UNIX)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        for (slot, byte) in address.sun_path.iter_mut().zip(name) {
            *slot = libc::c_char::from_ne_bytes([*byte]);
        }
        let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + name.len() + 1;
        Ok(Self {
            address,
            length: libc::socklen_t::try_from(length)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?,
        })
    }

    /// Connect now or not at all. A Unix stream connect on a nonblocking socket completes or
    /// fails at once (a full listen queue is `EAGAIN`), so this never waits on the gateway.
    fn connect(&self) -> io::Result<UnixStream> {
        // SAFETY: socket takes integers only.
        let socket = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        if socket == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel just returned this descriptor to this call alone.
        let socket = unsafe { OwnedFd::from_raw_fd(socket) };
        // SAFETY: `address` is a live `sockaddr_un` whose first `length` bytes are initialized.
        let connected = unsafe {
            libc::connect(
                socket.as_raw_fd(),
                (&raw const self.address).cast(),
                self.length,
            )
        };
        if connected != 0 {
            return Err(io::Error::last_os_error());
        }
        let stream = UnixStream::from(socket);
        stream.set_nonblocking(false)?;
        Ok(stream)
    }
}

/// Start the connector serving `listener` towards the gateway socket `upstream`, and return
/// once it serves. The caller is one thread, in the launch's user and network namespaces, and
/// is the process the connector's life is tied to.
pub(super) fn start(listener: TcpListener, upstream: &Path, abi: Abi) -> Result<(), Failure> {
    let upstream = Upstream::at(upstream)
        .map_err(|cause| Failure::new("name the gateway socket", upstream.display(), cause))?;
    let (mut ready, report) =
        io::pipe().map_err(|cause| Failure::new("open a pipe to", WHAT, cause))?;
    // SAFETY: getpid cannot fail.
    let leader = unsafe { libc::getpid() };
    // SAFETY: this process is one thread, so the child is a whole copy of it and no lock is held
    // by a thread that does not exist there.
    match unsafe { libc::fork() } {
        -1 => Err(Failure::last("fork", WHAT)),
        0 => {
            drop(ready);
            // SAFETY: setsid, fork and _exit take no pointers. The first child exits at once, so
            // the connector is nobody's child and the launcher collects the first child now.
            unsafe {
                if libc::setsid() == -1 {
                    libc::_exit(1);
                }
                match libc::fork() {
                    0 => {}
                    -1 => libc::_exit(1),
                    _ => libc::_exit(0),
                }
            }
            let code = serve(listener, upstream, leader, abi, report);
            // SAFETY: _exit takes an integer; this copy of the launcher must not run its
            // caller's code.
            unsafe { libc::_exit(code) }
        }
        first => {
            drop(report);
            drop(listener);
            reap(first)
                .map_err(|cause| Failure::new("collect the fork that started", WHAT, cause))?;
            await_ready(&mut ready)
        }
    }
}

/// Collect the launcher's own first fork, which exits as soon as it has forked the connector.
fn reap(pid: libc::pid_t) -> io::Result<()> {
    use cowshed_os::deadline;
    const ROLE: &str = "connector fork";
    let id =
        u32::try_from(pid).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let budget = deadline::start("start loopback connector", deadline::REAP);
    deadline::await_exit(ROLE, id, &budget, deadline::Progress::Nothing)
        .and_then(|()| {
            deadline::collect_exited(ROLE, id, &budget, deadline::Progress::Nothing, || {
                collect(pid)
            })
        })
        .map_err(io::Error::from)
}

// WHY unbounded: `WNOHANG` returns at once.
#[allow(clippy::disallowed_methods)]
fn collect(pid: libc::pid_t) -> io::Result<Option<()>> {
    // SAFETY: `pid` is this process's own child and a null status pointer is allowed.
    match unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) } {
        0 => Ok(None),
        -1 => Err(io::Error::last_os_error()),
        _ => Ok(Some(())),
    }
}

/// Read what the connector reports until it closes its end: [`READY`], or why it could not
/// serve, or nothing when it ended first.
fn await_ready(ready: &mut io::PipeReader) -> Result<(), Failure> {
    let failed = |cause: io::Error| Failure::new("start", WHAT, cause);
    let until = Instant::now() + READY_WITHIN;
    let mut said = Vec::new();
    let mut buffer = [0_u8; 256];
    loop {
        let Some(remaining) = until.checked_duration_since(Instant::now()) else {
            return Err(failed(io::Error::new(
                io::ErrorKind::TimedOut,
                "it did not say it serves",
            )));
        };
        let mut watched = libc::pollfd {
            fd: ready.as_fd().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: `watched` is one live pollfd.
        match unsafe {
            libc::poll(
                &raw mut watched,
                1,
                cowshed_os::deadline::poll_timeout(remaining),
            )
        } {
            0 => continue,
            -1 => {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(failed(error));
            }
            _ => {}
        }
        match ready.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => said.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(failed(error)),
        }
    }
    if said == READY {
        return Ok(());
    }
    Err(failed(io::Error::other(if said.is_empty() {
        "it ended before it served".to_owned()
    } else {
        String::from_utf8_lossy(&said).into_owned()
    })))
}

/// The connector process: give up what it does not need, say so, then relay until the launch's
/// leader exits. Returns its exit code.
fn serve(
    listener: TcpListener,
    upstream: Upstream,
    leader: libc::pid_t,
    abi: Abi,
    mut report: io::PipeWriter,
) -> i32 {
    let leader_exit = match confine(&listener, leader, abi, &report) {
        Ok(leader_exit) => leader_exit,
        Err(failure) => {
            let _ = report.write_all(failure.to_string().as_bytes());
            return 1;
        }
    };
    if report.write_all(READY).is_err() {
        return 1;
    }
    drop(report);
    relay_until_exit(&listener, upstream, &leader_exit)
}

/// Reduce this process to its listener and a watch on the leader's exit.
fn confine(
    listener: &TcpListener,
    leader: libc::pid_t,
    abi: Abi,
    report: &io::PipeWriter,
) -> Result<OwnedFd, Failure> {
    const SELF: &str = "itself";
    // SAFETY: pidfd_open takes integers only.
    let watch = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::c_long::from(leader), 0_i64) };
    let watch = libc::c_int::try_from(watch)
        .ok()
        .filter(|descriptor| *descriptor >= 0)
        .ok_or_else(|| Failure::last("watch the exit of", format_args!("its leader {leader}")))?;
    // SAFETY: the kernel just returned this descriptor to this call alone.
    let leader_exit = unsafe { OwnedFd::from_raw_fd(watch) };
    null_stdio().map_err(|cause| Failure::new("close the standard streams of", SELF, cause))?;
    close_others(&[
        listener.as_raw_fd(),
        report.as_fd().as_raw_fd(),
        leader_exit.as_raw_fd(),
    ])
    .map_err(|cause| Failure::new("close the inherited descriptors of", SELF, cause))?;
    listener
        .set_nonblocking(true)
        .map_err(|cause| Failure::new("make nonblocking the listener of", SELF, cause))?;
    Ruleset::create(abi, Network::Closed)
        .and_then(Ruleset::restrict_self)
        .map_err(|cause| Failure::new("confine", SELF, cause))?;
    drop_capabilities().map_err(|cause| Failure::new("drop the capabilities of", SELF, cause))?;
    let (unset, unused): (libc::c_ulong, libc::c_ulong) = (0, 0);
    // SAFETY: PR_SET_DUMPABLE takes integers only.
    if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, unset, unused, unused, unused) } != 0 {
        return Err(Failure::last("make untraceable", SELF));
    }
    Ok(leader_exit)
}

/// Point the standard streams at the null device: the connector says nothing, and must not hold
/// the command's output open past the command.
fn null_stdio() -> io::Result<()> {
    let null = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    for stream in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // SAFETY: dup2 takes two descriptors, both open.
        if unsafe { libc::dup2(null.as_raw_fd(), stream) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Close every descriptor above the standard streams but `keep`.
fn close_others(keep: &[RawFd]) -> io::Result<()> {
    let mut keep: Vec<u32> = keep
        .iter()
        .filter_map(|descriptor| u32::try_from(*descriptor).ok())
        .collect();
    keep.sort_unstable();
    let mut first = 3_u32;
    for kept in keep {
        if kept > first {
            close_range(first, kept - 1)?;
        }
        first = first.max(kept.saturating_add(1));
    }
    close_range(first, u32::MAX)
}

fn close_range(first: u32, last: u32) -> io::Result<()> {
    // SAFETY: close_range takes integer bounds and flags only.
    let closed = unsafe {
        libc::syscall(
            libc::SYS_close_range,
            libc::c_long::from(first),
            libc::c_long::from(last),
            0_i64,
        )
    };
    if closed == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// `struct __user_cap_header_struct`.
#[repr(C)]
struct CapabilityHeader {
    version: u32,
    pid: libc::c_int,
}

/// `struct __user_cap_data_struct`.
#[repr(C)]
#[derive(Clone, Copy)]
struct CapabilityData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

/// `_LINUX_CAPABILITY_VERSION_3`: two data structs, 64 capability bits.
const CAPABILITY_VERSION_3: u32 = 0x2008_0522;

/// Hold no capability. The connector was forked holding every capability over its launch's user
/// namespace, and never execs, so nothing else would drop them.
fn drop_capabilities() -> io::Result<()> {
    let mut header = CapabilityHeader {
        version: CAPABILITY_VERSION_3,
        pid: 0,
    };
    let none = [CapabilityData {
        effective: 0,
        permitted: 0,
        inheritable: 0,
    }; 2];
    // SAFETY: `header` and the two data structs version 3 reads are live across the call.
    if unsafe { libc::syscall(libc::SYS_capset, &raw mut header, none.as_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Accept and relay until the leader exits; the exit code.
fn relay_until_exit(listener: &TcpListener, upstream: Upstream, leader_exit: &OwnedFd) -> i32 {
    loop {
        let mut watched = [
            libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: leader_exit.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // The connector serves until its launch's leader exits, which is what this poll watches:
        // a pidfd is readable from the moment its process has exited.
        // SAFETY: `watched` is two live pollfds.
        if unsafe { libc::poll(watched.as_mut_ptr(), 2, -1) } == -1 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return 1;
        }
        if watched[1].revents != 0 {
            return 0;
        }
        if watched[0].revents == 0 {
            continue;
        }
        match listener.accept() {
            Ok((client, _)) => {
                if !admitted(client.local_addr().ok(), client.peer_addr().ok()) {
                    continue;
                }
                // A connection whose thread cannot start is closed: its client sees a reset.
                let _ = std::thread::Builder::new()
                    .name("relay".to_owned())
                    .stack_size(RELAY_STACK)
                    .spawn(move || {
                        // A gateway that is not there closes the connection, as a proxy that is
                        // down would.
                        if let Ok(gateway) = upstream.connect() {
                            relay(&client, &gateway);
                        }
                    });
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::Interrupted
                        | io::ErrorKind::ConnectionAborted
                ) => {}
            // Out of descriptors or memory: the listener stays readable, so wait for a relay to
            // end rather than spin on the same refusal.
            Err(_) => std::thread::sleep(ACCEPT_BACKOFF),
        }
    }
}

/// A relay thread copies through the kernel and keeps nothing on its stack.
const RELAY_STACK: usize = 64 * 1024;

const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

/// Copy bytes both ways, unread, until both directions end. One direction's end of stream is
/// passed on as a half-close, so a request whose client stops sending still gets its answer; a
/// direction that fails ends both.
fn relay(client: &TcpStream, gateway: &UnixStream) {
    std::thread::scope(|scope| {
        let outbound = std::thread::Builder::new()
            .name("relay-out".to_owned())
            .stack_size(RELAY_STACK)
            .spawn_scoped(scope, || {
                let copied = io::copy(&mut &*client, &mut &*gateway);
                end_direction(
                    copied.is_ok(),
                    || gateway.shutdown(Shutdown::Write),
                    || client.shutdown(Shutdown::Both),
                );
            });
        if outbound.is_err() {
            let _ = client.shutdown(Shutdown::Both);
            let _ = gateway.shutdown(Shutdown::Both);
            return;
        }
        let copied = io::copy(&mut &*gateway, &mut &*client);
        end_direction(
            copied.is_ok(),
            || client.shutdown(Shutdown::Write),
            || gateway.shutdown(Shutdown::Both),
        );
    });
}

/// End one direction of a relay: half-close its destination, and when the copy failed, close
/// the other socket too so the opposite copy ends instead of waiting on a dead peer.
fn end_direction(
    copied: bool,
    half_close: impl FnOnce() -> io::Result<()>,
    close_other: impl FnOnce() -> io::Result<()>,
) {
    let _ = half_close();
    if !copied {
        let _ = close_other();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv6Addr, SocketAddrV6};

    fn v4(address: [u8; 4], port: u16) -> Option<SocketAddr> {
        Some(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from(address),
            port,
        )))
    }

    #[test]
    fn the_connector_binds_one_loopback_address_and_port() {
        assert_eq!(ADDRESS.to_string(), "127.0.0.1:7644");
    }

    #[test]
    fn only_a_loopback_peer_on_the_connector_address_is_served() {
        let v6_loopback = Some(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            7644,
            0,
            0,
        )));
        for (local, peer, expected) in [
            (v4([127, 0, 0, 1], 7644), v4([127, 0, 0, 1], 40_000), true),
            (v4([127, 0, 0, 1], 7644), v4([127, 9, 9, 9], 40_000), true),
            (v4([127, 0, 0, 1], 7644), v4([10, 0, 0, 7], 40_000), false),
            (v4([127, 0, 0, 1], 7645), v4([127, 0, 0, 1], 40_000), false),
            (v4([127, 0, 0, 2], 7644), v4([127, 0, 0, 1], 40_000), false),
            (v4([0, 0, 0, 0], 7644), v4([127, 0, 0, 1], 40_000), false),
            (v6_loopback, v4([127, 0, 0, 1], 40_000), false),
            (v4([127, 0, 0, 1], 7644), v6_loopback, false),
            (None, v4([127, 0, 0, 1], 40_000), false),
            (v4([127, 0, 0, 1], 7644), None, false),
        ] {
            assert_eq!(admitted(local, peer), expected, "{local:?} <- {peer:?}");
        }
    }

    #[test]
    fn an_upstream_is_a_socket_path_that_fits_its_address() {
        let fits = Upstream::at(Path::new("/store/run/gateway/0123.sock")).unwrap();
        let header = std::mem::offset_of!(libc::sockaddr_un, sun_path);
        assert_eq!(
            usize::try_from(fits.length).unwrap(),
            header + "/store/run/gateway/0123.sock".len() + 1
        );
        let long = format!("/{}", "x".repeat(200));
        for refused in [long.as_str(), "/store/run/\0/gateway.sock"] {
            assert_eq!(
                Upstream::at(Path::new(refused))
                    .map(|_| ())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput,
                "{refused:?}"
            );
        }
    }
}
