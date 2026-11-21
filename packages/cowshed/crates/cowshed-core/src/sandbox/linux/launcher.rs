//! The launcher, the Linux counterpart of `sandbox-exec` (04_sandbox.md, "Linux enforcement"):
//! `[-D BUILD_VOLUME=<path>] -f <profile> -- <command> [argument…]`, as the whole command line
//! of `cowshed-sandbox-exec` ([`main`]) and after the verb `__sandbox-exec` of a binary that is
//! its own launcher ([`dispatch`]). Both are this one function, [`run`].
//!
//! It is the launched process itself: it enters the launch's namespaces, starts the loopback
//! connector, hides what the profile hides, restricts itself with the profile's Landlock rules
//! and then becomes the command, so the command keeps the pid, the process group, the standard
//! streams and the environment its supervisor gave the launch. Nothing here runs between a fork
//! and an exec of the supervisor: a ruleset is built from files this process opens, which is
//! not async-signal-safe work.
//!
//! It answers as `sandbox-exec` does, because the same callers read both: 64 for a command line
//! it does not understand, 65 for a profile it cannot enforce, 71 for a system call that failed,
//! `execvp() of '<program>' failed` on stderr for a command that could not start.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::os::fd::{AsFd as _, AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::FileExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::connector;
use super::failure::Failure;
use super::landlock::{Abi, Network, Ruleset};
use super::mounts;
use super::profile::{Access, ExecAuthority, Profile, Scope};

/// The first argument of a launcher invocation.
pub(in crate::sandbox) const VERB: &str = "__sandbox-exec";

/// `EX_USAGE`.
const EXIT_USAGE: i32 = 64;
/// `EX_DATAERR`.
const EXIT_PROFILE: i32 = 65;
/// `EX_OSERR`.
const EXIT_SYSTEM: i32 = 71;

/// One launch as its command line names it.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct Invocation {
    pub(super) profile: PathBuf,
    pub(super) build_volume: Option<PathBuf>,
    /// The command and its arguments: never empty.
    pub(super) command: Vec<OsString>,
}

/// A command line that is not a launch.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct Usage(&'static str);

impl fmt::Display for Usage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; a launch is [-D <NAME>=<path>] -f <profile> -- <command> [argument…]",
            self.0
        )
    }
}

/// The launch `arguments` name: a launcher's whole command line, a binary's after [`VERB`].
/// `parameter` is the one name a `-D` may bind.
pub(super) fn parse(
    arguments: impl IntoIterator<Item = OsString>,
    parameter: &str,
) -> Result<Invocation, Usage> {
    let mut arguments = arguments.into_iter();
    let mut profile = None;
    let mut build_volume = None;
    loop {
        let Some(option) = arguments.next() else {
            return Err(Usage("no `--` before the command"));
        };
        if option == "--" {
            break;
        }
        let Some(value) = arguments.next() else {
            return Err(Usage("an option without its value"));
        };
        if option == "-f" {
            if profile.replace(PathBuf::from(value)).is_some() {
                return Err(Usage("more than one profile"));
            }
        } else if option == "-D" {
            let binding = value.as_bytes();
            let Some(name) = binding
                .strip_prefix(parameter.as_bytes())
                .and_then(|rest| rest.strip_prefix(b"="))
            else {
                return Err(Usage("a parameter the profile format does not know"));
            };
            if build_volume
                .replace(PathBuf::from(OsString::from_vec(name.to_vec())))
                .is_some()
            {
                return Err(Usage("a parameter bound twice"));
            }
        } else {
            return Err(Usage("an option this launcher does not know"));
        }
    }
    let command: Vec<OsString> = arguments.collect();
    let Some(profile) = profile else {
        return Err(Usage("no profile"));
    };
    if command.first().is_none_or(|program| program.is_empty()) {
        return Err(Usage("no command"));
    }
    Ok(Invocation {
        profile,
        build_volume,
        command,
    })
}

/// Run this process as a launcher when its first argument is [`VERB`]: the exit code of a
/// launch that failed, or `None` when this process is not a launcher. A launch that succeeds
/// never returns.
pub(in crate::sandbox) fn dispatch(parameter: &str) -> Option<i32> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next()? != VERB {
        return None;
    }
    Some(run(arguments, parameter))
}

/// Run this process, which is a launcher and nothing else: the exit code of a launch that
/// failed. A launch that succeeds never returns.
pub(in crate::sandbox) fn main(parameter: &str) -> i32 {
    run(std::env::args_os().skip(1), parameter)
}

fn run(arguments: impl Iterator<Item = OsString>, parameter: &str) -> i32 {
    let invocation = match parse(arguments, parameter) {
        Ok(invocation) => invocation,
        Err(usage) => {
            cowshed_os::eprintln!("cowshed: {usage}");
            return EXIT_USAGE;
        }
    };
    let refused = |cause: &dyn fmt::Display| {
        cowshed_os::eprintln!(
            "cowshed: cannot launch under the sandbox profile {}: {cause}",
            invocation.profile.display()
        );
        EXIT_PROFILE
    };
    let profile = match fs::read_to_string(&invocation.profile) {
        Ok(text) => match Profile::parse(&text) {
            Ok(profile) => profile,
            Err(error) => return refused(&error),
        },
        Err(error) => return refused(&error),
    };
    if profile.build_volume && invocation.build_volume.is_none() {
        return refused(&format_args!(
            "it names a build volume and the launch binds none (-D {parameter}=<path>)"
        ));
    }
    let failure = enter(&profile, &invocation);
    cowshed_os::eprintln!("cowshed: {failure}");
    EXIT_SYSTEM
}

/// Why a launch did not become its command.
#[derive(Debug)]
enum LaunchFailure {
    Unsupported(super::landlock::Unsupported),
    System(Failure),
    /// The command itself could not start: said as `sandbox-exec` says it.
    Exec {
        program: OsString,
        cause: io::Error,
    },
}

impl fmt::Display for LaunchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(unsupported) => write!(formatter, "{unsupported}"),
            Self::System(failure) => write!(formatter, "the sandbox {failure}"),
            Self::Exec { program, cause } => write!(
                formatter,
                "execvp() of '{}' failed: {cause}",
                program.display()
            ),
        }
    }
}

impl From<Failure> for LaunchFailure {
    fn from(failure: Failure) -> Self {
        Self::System(failure)
    }
}

/// Become `invocation`'s command under `profile`. Returns only the reason it did not.
fn enter(profile: &Profile, invocation: &Invocation) -> LaunchFailure {
    match confine(profile, invocation) {
        Ok(mut command) => {
            let cause = command.exec();
            LaunchFailure::Exec {
                program: invocation.command[0].clone(),
                cause,
            }
        }
        Err(failure) => failure,
    }
}

/// Everything before the exec; the command to become.
fn confine(profile: &Profile, invocation: &Invocation) -> Result<Command, LaunchFailure> {
    let abi = Abi::probe().map_err(LaunchFailure::Unsupported)?;
    // SAFETY: getuid and getgid cannot fail.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let directory = std::env::current_dir()
        .map_err(|cause| Failure::new("name", "the working directory", cause))?;

    enter_user_and_network_namespace(uid, gid)?;
    if let Some(gateway) = &profile.gateway {
        let listener = connector::bind().map_err(|cause| {
            Failure::new("bind the loopback connector to", connector::ADDRESS, cause)
        })?;
        connector::start(listener, Path::new(gateway), abi)?;
    }

    // After the connector, which keeps the host's view: its upstream lies in a hidden directory.
    let mut reachable: Vec<PathBuf> = profile
        .rules
        .iter()
        .map(|rule| PathBuf::from(&rule.path))
        .chain(profile.sockets.iter().map(PathBuf::from))
        .collect();
    reachable.extend(invocation.build_volume.clone());
    let mut hidden: Vec<PathBuf> = profile.masks.iter().map(PathBuf::from).collect();
    hidden.push(mounts::user_runtime_directory(uid));
    mounts::enter(&mounts::masks(&hidden, &reachable))?;
    // Through the namespace's own mounts, so a working directory bound back is entered there.
    std::env::set_current_dir(&directory)
        .map_err(|cause| Failure::new("return to", directory.display(), cause))?;

    let ruleset = Ruleset::create(abi, Network::Unrestricted)
        .map_err(|cause| Failure::new("create", "a Landlock ruleset", cause))?;
    let exec = profile.exec == ExecAuthority::FollowsRead;
    for rule in &profile.rules {
        allow(
            &ruleset,
            abi,
            Path::new(&rule.path),
            rule.scope,
            rule.access,
            exec,
        )?;
    }
    if profile.build_volume
        && let Some(volume) = &invocation.build_volume
    {
        allow(
            &ruleset,
            abi,
            volume,
            Scope::Subtree,
            Access::ReadWrite,
            exec,
        )?;
    }
    let program = PathBuf::from(&invocation.command[0]);
    let program = match profile.exec {
        ExecAuthority::FollowsRead => program,
        ExecAuthority::LaunchedProgram => {
            let search = std::env::var_os("PATH");
            match resolve_program(&program, search.as_deref(), |candidate| candidate.is_file()) {
                Some(resolved) => {
                    for startable in startable_with(&resolved)? {
                        allow(
                            &ruleset,
                            abi,
                            &startable,
                            Scope::Subtree,
                            Access::Read,
                            true,
                        )?;
                    }
                    resolved
                }
                // Nothing to start: the exec below says so.
                None => program,
            }
        }
    };
    ruleset
        .restrict_self()
        .map_err(|cause| Failure::new("enforce", "the Landlock ruleset", cause))?;

    let mut command = Command::new(program);
    command
        .arg0(&invocation.command[0])
        .args(&invocation.command[1..]);
    Ok(command)
}

/// Tie one rule to the file its path names now. A path that names nothing has no rule, and
/// neither has a literal that names a directory.
fn allow(
    ruleset: &Ruleset,
    abi: Abi,
    path: &Path,
    scope: Scope,
    access: Access,
    exec: bool,
) -> Result<(), Failure> {
    let named = match cowshed_os::fsio::open_path(path) {
        Ok(named) => File::from(named),
        Err(error) if mounts::is_absent(&error) => return Ok(()),
        Err(error) => return Err(Failure::new("open", path.display(), error)),
    };
    let is_directory = named
        .metadata()
        .map_err(|cause| Failure::new("inspect", path.display(), cause))?
        .is_dir();
    if scope == Scope::Literal && is_directory {
        return Ok(());
    }
    ruleset
        .allow(named.as_fd(), abi.rights(access, exec, is_directory))
        .map_err(|cause| Failure::new("add a Landlock rule for", path.display(), cause))
}

/// A user namespace that maps this user and group to themselves, and a network namespace it
/// owns, with loopback up: no interface, route or socket of the host is in it.
fn enter_user_and_network_namespace(uid: libc::uid_t, gid: libc::gid_t) -> Result<(), Failure> {
    // SAFETY: unshare takes flags only.
    if unsafe { libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNET) } != 0 {
        return Err(Failure::last("enter", "a user and network namespace"));
    }
    for (file, content) in [
        // A group map is writable by an unprivileged owner only once setgroups is denied.
        ("/proc/self/setgroups", "deny".to_owned()),
        ("/proc/self/uid_map", format!("{uid} {uid} 1")),
        ("/proc/self/gid_map", format!("{gid} {gid} 1")),
    ] {
        fs::OpenOptions::new()
            .write(true)
            .open(file)
            .and_then(|mut opened| opened.write_all(content.as_bytes()))
            .map_err(|cause| Failure::new("write", file, cause))?;
    }
    loopback_up().map_err(|cause| Failure::new("bring up", "the loopback interface", cause))
}

fn loopback_up() -> io::Result<()> {
    // SAFETY: socket takes integers only.
    let socket = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if socket == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kernel just returned this descriptor to this call alone.
    let socket = unsafe { OwnedFd::from_raw_fd(socket) };
    // SAFETY: an all-zero `ifreq` is a valid one.
    let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
    for (slot, byte) in request.ifr_name.iter_mut().zip(b"lo") {
        *slot = libc::c_char::from_ne_bytes([*byte]);
    }
    let up = libc::c_short::try_from(libc::IFF_UP)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    // The request argument of `ioctl` is `c_ulong` under glibc and `c_int` under musl, and libc
    // types these two constants `c_ulong` under both; each fits either.
    #[allow(clippy::unnecessary_cast)]
    let (get_flags, set_flags) = (
        libc::SIOCGIFFLAGS as libc::Ioctl,
        libc::SIOCSIFFLAGS as libc::Ioctl,
    );
    // SAFETY: `request` is a live `ifreq` naming an interface; SIOCGIFFLAGS fills its flags and
    // SIOCSIFFLAGS reads them, and the flags are the union member both calls use.
    unsafe {
        if libc::ioctl(socket.as_raw_fd(), get_flags, &raw mut request) != 0 {
            return Err(io::Error::last_os_error());
        }
        request.ifr_ifru.ifru_flags |= up;
        if libc::ioctl(socket.as_raw_fd(), set_flags, &raw const request) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// The file an exec of `program` starts: `program` itself when it names a path, else the first
/// entry of `search` (a `PATH`) that holds a file of that name.
pub(super) fn resolve_program(
    program: &Path,
    search: Option<&OsStr>,
    is_file: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    if program.as_os_str().as_bytes().contains(&b'/') {
        return Some(program.to_owned());
    }
    std::env::split_paths(search?)
        .map(|directory| directory.join(program))
        .find(|candidate| is_file(candidate))
}

/// What an executes-only-the-launched-program launch may start: the program, the ELF
/// interpreter the kernel starts it through, and the `libexec/git-core` beside its `bin`, where
/// Git keeps the helpers it re-executes.
fn startable_with(program: &Path) -> Result<Vec<PathBuf>, Failure> {
    let resolved = match fs::canonicalize(program) {
        Ok(resolved) => resolved,
        Err(error) if mounts::is_absent(&error) => return Ok(Vec::new()),
        Err(error) => return Err(Failure::new("resolve", program.display(), error)),
    };
    let mut startable = Vec::with_capacity(3);
    let image =
        File::open(&resolved).map_err(|cause| Failure::new("open", resolved.display(), cause))?;
    let interpreter = elf_interpreter(|offset, length| {
        let mut bytes = vec![0_u8; length];
        image.read_exact_at(&mut bytes, offset)?;
        Ok(bytes)
    })
    .map_err(|cause| Failure::new("read the program header of", resolved.display(), cause))?;
    startable.extend(interpreter);
    if let Some(prefix) = resolved.parent().and_then(Path::parent) {
        startable.push(prefix.join("libexec/git-core"));
    }
    startable.push(resolved);
    Ok(startable)
}

/// `PT_INTERP`.
const PROGRAM_INTERPRETER: u32 = 3;
/// The longest interpreter path read: `PATH_MAX`.
const INTERPRETER_LIMIT: u64 = 4096;

/// The interpreter a 64-bit little-endian ELF image names in its program header, read through
/// `read(offset, length)`. `None` for a static image and for anything that is not such an image
/// (a script, another class or byte order): an exec of it then starts nothing this function
/// would have admitted.
pub(super) fn elf_interpreter(
    read: impl Fn(u64, usize) -> io::Result<Vec<u8>>,
) -> io::Result<Option<PathBuf>> {
    let header = match read(0, 64) {
        Ok(header) => header,
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    };
    // Magic, ELFCLASS64, ELFDATA2LSB.
    if header[..6] != [0x7f, b'E', b'L', b'F', 2, 1] {
        return Ok(None);
    }
    let word = |bytes: &[u8], at: usize| {
        u64::from_le_bytes(
            bytes[at..at + 8]
                .try_into()
                .expect("an eight-byte slice is eight bytes"),
        )
    };
    let half = |bytes: &[u8], at: usize| u16::from_le_bytes([bytes[at], bytes[at + 1]]);
    let table = word(&header, 0x20);
    let entry_size = half(&header, 0x36);
    let entries = half(&header, 0x38);
    if usize::from(entry_size) < 56 {
        return Ok(None);
    }
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "a malformed ELF image");
    for index in 0..entries {
        let offset = u64::from(index)
            .checked_mul(u64::from(entry_size))
            .and_then(|skipped| table.checked_add(skipped))
            .ok_or_else(malformed)?;
        let entry = read(offset, 56)?;
        if u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) != PROGRAM_INTERPRETER {
            continue;
        }
        let length = word(&entry, 0x20);
        if length == 0 || length > INTERPRETER_LIMIT {
            return Err(malformed());
        }
        let length = usize::try_from(length).map_err(|_| malformed())?;
        let mut name = read(word(&entry, 0x08), length)?;
        // The name is stored with its terminator.
        if let Some(end) = name.iter().position(|byte| *byte == 0) {
            name.truncate(end);
        }
        return Ok(Some(PathBuf::from(OsString::from_vec(name))));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARAMETER: &str = "BUILD_VOLUME";

    fn arguments(arguments: &[&str]) -> Vec<OsString> {
        arguments.iter().map(OsString::from).collect()
    }

    #[test]
    fn a_launch_names_its_profile_its_binding_and_its_command() {
        assert_eq!(
            parse(
                arguments(&["-f", "/s/run/profiles/a.sb", "--", "true"]),
                PARAMETER
            ),
            Ok(Invocation {
                profile: PathBuf::from("/s/run/profiles/a.sb"),
                build_volume: None,
                command: arguments(&["true"]),
            })
        );
        assert_eq!(
            parse(
                arguments(&[
                    "-D",
                    "BUILD_VOLUME=/m/.build/o/r/01=23",
                    "-f",
                    "a.sb",
                    "--",
                    "sh",
                    "-c",
                    "echo -- -f",
                    "--",
                ]),
                PARAMETER
            ),
            Ok(Invocation {
                profile: PathBuf::from("a.sb"),
                build_volume: Some(PathBuf::from("/m/.build/o/r/01=23")),
                command: arguments(&["sh", "-c", "echo -- -f", "--"]),
            }),
            "everything after the first `--` is the command's"
        );
    }

    #[test]
    fn a_command_line_that_is_not_a_launch_is_refused_and_says_why() {
        for (line, why) in [
            (&["-f", "a.sb"][..], "no `--` before the command"),
            (&[][..], "no `--` before the command"),
            (&["-f", "a.sb", "--"][..], "no command"),
            (&["-f", "a.sb", "--", ""][..], "no command"),
            (&["--", "true"][..], "no profile"),
            (&["-f"][..], "an option without its value"),
            (
                &["-f", "a.sb", "-f", "b.sb", "--", "true"][..],
                "more than one profile",
            ),
            (
                &["-D", "HOME=/x", "-f", "a.sb", "--", "true"][..],
                "a parameter the profile format does not know",
            ),
            (
                &["-D", "BUILD_VOLUME", "-f", "a.sb", "--", "true"][..],
                "a parameter the profile format does not know",
            ),
            (
                &[
                    "-D",
                    "BUILD_VOLUME=/a",
                    "-D",
                    "BUILD_VOLUME=/b",
                    "-f",
                    "a.sb",
                    "--",
                    "true",
                ][..],
                "a parameter bound twice",
            ),
            (
                &["-p", "(version 1)", "--", "true"][..],
                "an option this launcher does not know",
            ),
            (&["true"][..], "an option without its value"),
        ] {
            let usage = parse(arguments(line), PARAMETER).unwrap_err();
            assert_eq!(usage, Usage(why), "{line:?}");
            assert!(usage.to_string().contains("-f <profile> --"), "{usage}");
        }
    }

    #[test]
    fn a_program_is_the_path_it_names_or_the_first_file_of_that_name_on_the_search_path() {
        let on_path =
            |candidate: &Path| candidate == Path::new("/b/git") || candidate == Path::new("/c/git");
        let search = OsStr::new("/a:/b:/c");
        for (program, search, expected) in [
            ("git", Some(search), Some("/b/git")),
            ("./git", Some(search), Some("./git")),
            ("/opt/git/bin/git", None, Some("/opt/git/bin/git")),
            ("hg", Some(search), None),
            ("git", None, None),
        ] {
            assert_eq!(
                resolve_program(Path::new(program), search, on_path),
                expected.map(PathBuf::from),
                "{program}"
            );
        }
    }

    /// A 64-bit little-endian image with one program header per `(type, offset, size)`.
    fn image(headers: &[(u32, u64, u64)], tail: &[u8]) -> Vec<u8> {
        let mut image = vec![0_u8; 64];
        image[..6].copy_from_slice(&[0x7f, b'E', b'L', b'F', 2, 1]);
        image[0x20..0x28].copy_from_slice(&64_u64.to_le_bytes());
        image[0x36..0x38].copy_from_slice(&56_u16.to_le_bytes());
        image[0x38..0x3a].copy_from_slice(&u16::try_from(headers.len()).unwrap().to_le_bytes());
        for (kind, offset, size) in headers {
            let mut header = [0_u8; 56];
            header[..4].copy_from_slice(&kind.to_le_bytes());
            header[0x08..0x10].copy_from_slice(&offset.to_le_bytes());
            header[0x20..0x28].copy_from_slice(&size.to_le_bytes());
            image.extend_from_slice(&header);
        }
        image.extend_from_slice(tail);
        image
    }

    fn reader(image: Vec<u8>) -> impl Fn(u64, usize) -> io::Result<Vec<u8>> {
        move |offset, length| {
            let start = usize::try_from(offset).unwrap();
            image
                .get(start..start + length)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))
        }
    }

    #[test]
    fn an_image_names_its_interpreter_and_anything_else_names_none() {
        let name = b"/lib64/ld-linux-x86-64.so.2\0";
        let at = 64 + 2 * 56;
        let dynamic = image(
            &[
                (1, 0, 0),
                (
                    PROGRAM_INTERPRETER,
                    u64::try_from(at).unwrap(),
                    u64::try_from(name.len()).unwrap(),
                ),
            ],
            name,
        );
        assert_eq!(
            elf_interpreter(reader(dynamic)).unwrap(),
            Some(PathBuf::from("/lib64/ld-linux-x86-64.so.2"))
        );
        assert_eq!(
            elf_interpreter(reader(image(&[(1, 0, 0)], b""))).unwrap(),
            None
        );
        assert_eq!(
            elf_interpreter(reader(b"#!/bin/sh\nexit 0\n".to_vec())).unwrap(),
            None,
            "a script shorter than an ELF header"
        );
        let mut script = b"#!/bin/sh\n".to_vec();
        script.resize(128, b'#');
        assert_eq!(elf_interpreter(reader(script)).unwrap(), None);
        let truncated = image(&[(PROGRAM_INTERPRETER, 4096, 28)], b"");
        assert!(elf_interpreter(reader(truncated)).is_err());
        let oversized = image(&[(PROGRAM_INTERPRETER, 64, 1 << 20)], b"");
        assert_eq!(
            elf_interpreter(reader(oversized)).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
