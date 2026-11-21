//! Escape tests: the compiled profile of a workspace on real directories, launched through the
//! real launcher under this kernel, and what a process inside is refused (04_sandbox.md, "Escape
//! tests"; 08_testing.md). Nothing here is skipped on a host that lacks Landlock or unprivileged
//! user namespaces: the launcher refuses the launch (exit 71) and the test fails saying so.
//!
//! The process inside is this test binary, started as [`probe_role`]: it makes the system calls
//! a test names and reports each one's errno, so a refusal is read from the kernel and never
//! from a tool's message. The profile reads the binary through a read grant on its directory.

use std::fmt;
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use cowshed_os::test_backstop::{CommandBackstop as _, UnixStreamBackstop as _};

use super::super::{RunSandboxMode, SandboxGrants, SandboxProfileRole, seatbelt_profile};
use super::{SandboxConfig, mounts, profile};
use crate::exec::{ProfileFile, ProfileLaunch};
use crate::temp_root::TempRoot;

/// The environment variable that makes [`probe_role`] a probe: one `<operation>\t<target>` per
/// line.
const PROBES: &str = "COWSHED_SANDBOX_PROBES";
/// What leads each line a probe reports, among the lines the test harness prints.
const REPORT: &str = "sandbox-probe: ";
const PROBE_ROLE: &str = "sandbox::linux::escape_tests::probe_role";

/// One system call a process inside the sandbox attempts.
#[derive(Clone, Debug)]
enum Probe {
    /// `open(2)` for reading, then `read(2)` to the end.
    Read(PathBuf),
    /// `open(2)` for writing with `O_CREAT`, then `write(2)`.
    Write(PathBuf),
    /// `connect(2)` on a pathname Unix stream socket.
    ConnectUnix(PathBuf),
    /// `connect(2)` on a TCP socket.
    ConnectTcp(SocketAddr),
    /// `connect(2)` on a UDP socket, which only chooses a route.
    ConnectUdp(SocketAddr),
}

impl fmt::Display for Probe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(path) => write!(formatter, "read\t{}", path.display()),
            Self::Write(path) => write!(formatter, "write\t{}", path.display()),
            Self::ConnectUnix(path) => write!(formatter, "connect-unix\t{}", path.display()),
            Self::ConnectTcp(address) => write!(formatter, "connect-tcp\t{address}"),
            Self::ConnectUdp(address) => write!(formatter, "connect-udp\t{address}"),
        }
    }
}

/// How one probe ended.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Done,
    Errno(i32),
    /// An error the kernel gave no number for: a connect that neither completed nor failed.
    Other(String),
}

impl Outcome {
    fn of(result: std::io::Result<()>) -> Self {
        match result {
            Ok(()) => Self::Done,
            Err(error) => error
                .raw_os_error()
                .map_or_else(|| Self::Other(error.kind().to_string()), Self::Errno),
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Done => formatter.write_str("done"),
            Self::Errno(errno) => write!(formatter, "errno {errno}"),
            Self::Other(kind) => write!(formatter, "other {kind}"),
        }
    }
}

/// How long a probe's TCP connect may take. A private network namespace answers at once; only a
/// launch that kept the host's network would wait here, and its probe then fails by this bound.
const CONNECT_BOUND: Duration = Duration::from_secs(5);

fn attempt(operation: &str, target: &str) -> Outcome {
    let address = || target.parse::<SocketAddr>().expect("a socket address");
    Outcome::of(match operation {
        "read" => fs::read(target).map(drop),
        "write" => fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(target)
            .and_then(|mut file| std::io::Write::write_all(&mut file, b"written inside\n")),
        "connect-unix" => UnixStream::connect_bounded(target).map(drop),
        "connect-tcp" => TcpStream::connect_timeout(&address(), CONNECT_BOUND).map(drop),
        "connect-udp" => {
            let unspecified: SocketAddr = if address().is_ipv4() {
                "0.0.0.0:0".parse().expect("an address")
            } else {
                "[::]:0".parse().expect("an address")
            };
            UdpSocket::bind(unspecified).and_then(|socket| socket.connect(address()))
        }
        other => panic!("no probe is called {other}"),
    })
}

/// The process inside the sandbox. Run as a test of its own it is told nothing and does nothing.
#[test]
fn probe_role() {
    let Some(probes) = std::env::var_os(PROBES) else {
        return;
    };
    for line in probes.to_str().expect("UTF-8 probes").lines() {
        let (operation, target) = line.split_once('\t').expect("an operation and its target");
        println!("{REPORT}{}", attempt(operation, target));
    }
}

/// A workspace on real directories under the canonical `/tmp`, which no profile opens: its
/// HOME, its mount root with one workspace and a sibling, its exec temp dir, its store, a
/// read-only grant, and `outside`, which no rule names.
struct Workspace {
    scratch: TempRoot,
    config: SandboxConfig,
}

impl Workspace {
    fn new(mode: RunSandboxMode) -> Self {
        super::super::tests::name_launcher();
        let scratch = TempRoot::new("cowshed-escape");
        for directory in [
            "home/.ssh",
            "mnt/workspace",
            "mnt/sibling",
            "tmp",
            "store/run",
            "shared",
            "outside",
        ] {
            fs::create_dir_all(scratch.join(directory)).expect("fixture directory");
        }
        let probe = std::env::current_exe().expect("this test binary");
        let config = SandboxConfig {
            home: scratch.join("home"),
            mount_root: scratch.join("mnt"),
            workspace_mount: scratch.join("mnt/workspace"),
            shed_links: Vec::new(),
            exec_temp_dir: scratch.join("tmp"),
            store_root: scratch.join("store"),
            cpu_pool_directory: None,
            port_block: None,
            retained_port_blocks: Vec::new(),
            mode,
            grants: SandboxGrants {
                read: vec![
                    probe.parent().expect("its directory").to_owned(),
                    scratch.join("shared"),
                ],
                ..SandboxGrants::default()
            },
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            git_worktree_repository: None,
            main_mount: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            program_directories: Vec::new(),
            capabilities: Default::default(),
        };
        Self { scratch, config }
    }

    /// A file of the fixture holding `text`.
    fn file(&self, relative: &str, text: &str) -> PathBuf {
        let path = self.scratch.join(relative);
        fs::write(&path, text).expect("fixture file");
        path
    }

    /// The launch of this test binary as [`probe_role`] under the workspace's executed-child
    /// profile, as a job's command is launched: the profile from its file in the fixture's own
    /// store, the workspace as working directory, and no environment but the probes.
    fn launch(&self, probes: &[Probe]) -> Output {
        let profile = seatbelt_profile(&self.config, SandboxProfileRole::ExecutedChild)
            .expect("a profile Landlock enforces");
        let launch = ProfileLaunch::write(&self.config.profile_directory(), &profile)
            .expect("the profile's file");
        let launcher = super::super::launcher().expect("the launcher this process named");
        let listed: Vec<String> = probes.iter().map(Probe::to_string).collect();
        Command::new(launcher.program())
            .args(launch.args(launcher))
            .arg(std::env::current_exe().expect("this test binary"))
            .args(["--exact", PROBE_ROLE, "--nocapture", "--quiet"])
            .current_dir(&self.config.workspace_mount)
            .env_clear()
            .env(PROBES, listed.join("\n"))
            .output_bounded()
            .expect("start the launcher")
    }

    /// How each of `probes` ended inside the sandbox, in order.
    fn probe(&self, probes: &[Probe]) -> Vec<Outcome> {
        let output = self.launch(probes);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "the launch ended {}: {}{stdout}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let outcomes: Vec<Outcome> = stdout
            .lines()
            .filter_map(|line| line.strip_prefix(REPORT))
            .map(|report| match report.split_once(' ') {
                Some(("errno", errno)) => Outcome::Errno(errno.parse().expect("an errno")),
                Some(("other", kind)) => Outcome::Other(kind.to_owned()),
                None if report == "done" => Outcome::Done,
                _ => panic!("a probe reported {report:?}"),
            })
            .collect();
        assert_eq!(
            outcomes.len(),
            probes.len(),
            "one report per probe: {stdout}"
        );
        outcomes
    }
}

const REFUSED: Outcome = Outcome::Errno(libc::EACCES);
const ABSENT: Outcome = Outcome::Errno(libc::ENOENT);

/// A read of a path no rule names is refused by the kernel, whoever owns the file: beside the
/// workspace, a secret in HOME, a sibling workspace, the store. What a rule names is read, so
/// the refusals are the profile's and not a launch that reads nothing.
#[test]
fn a_read_of_a_path_no_rule_names_is_refused() {
    let workspace = Workspace::new(RunSandboxMode::ReadWrite);
    let closed = [
        workspace.file("outside/secret", "outside\n"),
        workspace.file("home/.ssh/id_ed25519", "key\n"),
        workspace.file("home/notes", "home\n"),
        workspace.file("mnt/sibling/source", "sibling\n"),
        workspace.file("store/grants.json", "{}\n"),
    ];
    let open = [
        workspace.file("mnt/workspace/source", "workspace\n"),
        workspace.file("shared/data", "granted\n"),
        workspace.file("tmp/scratch", "temp\n"),
    ];
    let probes: Vec<Probe> = closed
        .iter()
        .chain(&open)
        .cloned()
        .map(Probe::Read)
        .collect();
    let outcomes = workspace.probe(&probes);
    for (path, outcome) in closed.iter().zip(&outcomes) {
        assert_eq!(*outcome, REFUSED, "{} has no rule", path.display());
        assert!(
            fs::read(path).is_ok(),
            "{} is readable outside",
            path.display()
        );
    }
    for (path, outcome) in open.iter().zip(&outcomes[closed.len()..]) {
        assert_eq!(*outcome, Outcome::Done, "{} is admitted", path.display());
    }
}

/// A write outside the write grants is refused and leaves nothing behind: beside the workspace,
/// in HOME, in a sibling workspace, in the store, and in a tree granted for reading only. A
/// read-only job is refused its own mount too, and still writes its exec temp dir.
#[test]
fn a_write_outside_the_write_grants_is_refused() {
    let workspace = Workspace::new(RunSandboxMode::ReadWrite);
    let granted = workspace.file("shared/data", "granted\n");
    let closed = [
        workspace.scratch.join("outside/planted"),
        workspace.scratch.join("home/.profile"),
        workspace.scratch.join("mnt/sibling/planted"),
        workspace.scratch.join("store/planted"),
        workspace.scratch.join("shared/planted"),
    ];
    let open = [
        workspace.scratch.join("mnt/workspace/written"),
        workspace.scratch.join("tmp/written"),
    ];
    let probes: Vec<Probe> = closed
        .iter()
        .chain(&open)
        .cloned()
        .chain([granted.clone()])
        .map(Probe::Write)
        .collect();
    let outcomes = workspace.probe(&probes);
    for (path, outcome) in closed.iter().zip(&outcomes) {
        assert_eq!(*outcome, REFUSED, "{} is no write grant", path.display());
        assert!(!path.exists(), "{} was created", path.display());
    }
    for (path, outcome) in open.iter().zip(&outcomes[closed.len()..]) {
        assert_eq!(*outcome, Outcome::Done, "{} is writable", path.display());
        assert_eq!(fs::read(path).expect("written"), b"written inside\n");
    }
    assert_eq!(outcomes[closed.len() + open.len()], REFUSED);
    assert_eq!(
        fs::read(&granted).expect("the granted file"),
        b"granted\n",
        "a file granted for reading is untouched"
    );

    let read_only = Workspace::new(RunSandboxMode::ReadOnly);
    let source = read_only.file("mnt/workspace/source", "source\n");
    let outcomes = read_only.probe(&[
        Probe::Write(source.clone()),
        Probe::Write(read_only.scratch.join("mnt/workspace/planted")),
        Probe::Read(source.clone()),
        Probe::Write(read_only.scratch.join("tmp/written")),
    ]);
    assert_eq!(outcomes, [REFUSED, REFUSED, Outcome::Done, Outcome::Done]);
    assert_eq!(fs::read(&source).expect("the source"), b"source\n");
    assert!(!read_only.scratch.join("mnt/workspace/planted").exists());
}

/// A listener at `path`, and the proof that a process outside the sandbox reaches it.
fn listening(path: &Path) -> UnixListener {
    let listener =
        UnixListener::bind(path).unwrap_or_else(|error| panic!("bind {}: {error}", path.display()));
    UnixStream::connect_bounded(path).unwrap_or_else(|error| {
        panic!(
            "{} is reachable outside the sandbox: {error}",
            path.display()
        )
    });
    listener
}

/// Removes a socket this test bound in a directory it does not own.
struct Unlinked(PathBuf);

impl Drop for Unlinked {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Landlock does not decide `connect(2)` on a pathname socket, so the directories whose sockets
/// a job must not reach are hidden: the user's runtime directory, where the session bus and the
/// user service manager listen, and the store's `run` directory. A socket in either is absent
/// inside the sandbox while a process outside connects to it; a socket the controller selected
/// stays reachable beneath the same mask.
#[test]
fn the_users_bus_socket_and_the_stores_sockets_are_unreachable() {
    let mut workspace = Workspace::new(RunSandboxMode::ReadWrite);
    let supervisor = workspace.scratch.join("store/run/sibling.sock");
    let selected = workspace.scratch.join("store/run/selected.sock");
    let _supervisor = listening(&supervisor);
    let _selected = listening(&selected);
    workspace.config.allowed_unix_sockets = vec![selected.clone()];

    // SAFETY: getuid cannot fail.
    let runtime = mounts::user_runtime_directory(unsafe { libc::getuid() });
    let bus = runtime.join("bus");
    // A listener of this test's own in the user's runtime directory, where there is one: a
    // host without a session has no bus to reach, and the bus's own name is probed either way.
    let planted = runtime
        .is_dir()
        .then(|| runtime.join(format!("cowshed-escape-{}.sock", std::process::id())));
    let _unlinked = planted.clone().map(Unlinked);
    let _planted = planted.as_deref().map(listening);

    let mut probes = vec![
        Probe::ConnectUnix(supervisor),
        Probe::ConnectUnix(bus.clone()),
        Probe::ConnectUnix(selected),
    ];
    probes.extend(planted.clone().map(Probe::ConnectUnix));
    let outcomes = workspace.probe(&probes);
    assert_eq!(
        outcomes[0], ABSENT,
        "a sibling's supervisor socket is hidden"
    );
    assert_eq!(outcomes[1], ABSENT, "{} is hidden", bus.display());
    assert_eq!(
        outcomes[2],
        Outcome::Done,
        "the controller-selected socket is bound back beneath the mask"
    );
    if let Some(planted) = &planted {
        assert_eq!(outcomes[3], ABSENT, "{} is hidden", planted.display());
    }
}

/// The launch has a network namespace of its own with loopback and nothing else: a connect to
/// any address beyond loopback has no route, over TCP and UDP, and the host's loopback listeners
/// are not this namespace's. IPv6 is probed where the host has it.
#[test]
fn a_connect_beyond_loopback_fails() {
    let workspace = Workspace::new(RunSandboxMode::ReadWrite);
    let host = TcpListener::bind("127.0.0.1:0").expect("a host loopback listener");
    let host = host.local_addr().expect("its address");
    TcpStream::connect_timeout(&host, CONNECT_BOUND)
        .expect("the host's listener is reachable outside the sandbox");
    // TEST-NET-1 and the IPv6 documentation prefix: addresses no host answers on, so a launch
    // that kept the host's network would wait out its bound here instead of connecting.
    let mut beyond: Vec<SocketAddr> = vec!["192.0.2.1:9".parse().expect("an address")];
    if UdpSocket::bind("[::1]:0").is_ok() {
        beyond.push("[2001:db8::1]:9".parse().expect("an address"));
    }
    let mut probes: Vec<Probe> = beyond
        .iter()
        .flat_map(|address| [Probe::ConnectTcp(*address), Probe::ConnectUdp(*address)])
        .collect();
    probes.push(Probe::ConnectTcp(host));
    let outcomes = workspace.probe(&probes);
    let (loopback, routed) = outcomes.split_last().expect("the host probe");
    for (probe, outcome) in probes.iter().zip(routed) {
        assert_eq!(*outcome, Outcome::Errno(libc::ENETUNREACH), "{probe}");
    }
    assert_eq!(
        *loopback,
        Outcome::Errno(libc::ECONNREFUSED),
        "loopback is up and holds none of the host's listeners"
    );
}

/// A launcher enforces only the profiles of its own build: one another build wrote is refused
/// with `sandbox-exec`'s exit for a profile that cannot be enforced, both builds named, and the
/// command never starts. So does a file that is no profile, and a command line that is no launch
/// is a usage error.
#[test]
fn a_profile_of_another_build_is_refused_and_starts_nothing() {
    let workspace = Workspace::new(RunSandboxMode::ReadWrite);
    let launcher = super::super::launcher().expect("the launcher this process named");
    let own = seatbelt_profile(&workspace.config, SandboxProfileRole::ExecutedChild)
        .expect("a profile")
        .into_text();
    let theirs = own.replace(&profile::BUILD.to_string(), "00000000000000ff");
    assert_ne!(theirs, own, "the profile names its build");
    let marker = workspace.scratch.join("mnt/workspace/started");
    let launch = |profile: &str| {
        let file = ProfileFile::write(&workspace.config.profile_directory(), profile)
            .expect("the profile's file");
        Command::new(launcher.program())
            .args(launcher.launch_arguments(file.path(), None))
            .args(["touch", "started"])
            .current_dir(&workspace.config.workspace_mount)
            .output_bounded()
            .expect("start the launcher")
    };

    let refused = launch(&theirs);
    let said = String::from_utf8_lossy(&refused.stderr);
    assert_eq!(refused.status.code(), Some(65), "{said}");
    assert!(
        said.contains("sandbox build 00000000000000ff wrote it")
            && said.contains(&format!(
                "this launcher is sandbox build {}",
                profile::BUILD
            )),
        "{said}"
    );
    assert!(!marker.exists(), "the command started");

    let malformed = launch("(version 1)(allow default)");
    assert_eq!(
        malformed.status.code(),
        Some(65),
        "{}",
        String::from_utf8_lossy(&malformed.stderr)
    );
    assert!(!marker.exists(), "the command started");

    let usage = Command::new(launcher.program())
        .arg("--version")
        .output_bounded()
        .expect("start the launcher");
    assert_eq!(
        usage.status.code(),
        Some(64),
        "no launch names its profile so"
    );

    let enforced = launch(&own);
    assert!(
        enforced.status.success(),
        "{}",
        String::from_utf8_lossy(&enforced.stderr)
    );
    assert!(
        marker.exists(),
        "its own build's profile launches the command"
    );
}
