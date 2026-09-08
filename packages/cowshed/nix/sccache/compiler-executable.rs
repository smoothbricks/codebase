//! Real-compiler regressions, copied into upstream tests by the standalone flake.
//! Run explicitly: cargo test --no-default-features --test compiler_executable -- --ignored
//! Requires rustc and clang on PATH; the Rustup control also requires rustup,
//! but uses private homes and the already-installed real toolchain. No downloads.
//! SCCACHE_TEST_CLANG may select an unwrapped clang for the multicall control;
//! cc on PATH supplies the platform linker and its SDK/libc configuration.
//! SCCACHE_TEST_BINARY may name the installed binary for before/after comparison.
//! Set XDG_RUNTIME_DIR to a writable short path when the default temp path is long.
#![cfg(unix)]

use sccache::server::ServerInfo;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod harness;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

fn runtime_directory() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

struct OwnedProcess {
    child: Child,
    group: Option<libc::pid_t>,
}

impl OwnedProcess {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        let child = command.process_group(0).stdin(Stdio::null()).spawn()?;
        let group = libc::pid_t::try_from(child.id()).expect("POSIX child ID fits pid_t");
        Ok(Self {
            child,
            group: Some(group),
        })
    }

    fn has_exited(&self) -> io::Result<bool> {
        // WNOWAIT leaves the child (and its PID) owned until group termination.
        // Child::try_wait would reap it, allowing Drop to signal a reused PGID.
        // SAFETY: zero is a valid initial siginfo_t and waitid initializes it.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let id = libc::id_t::try_from(self.child.id()).expect("POSIX child ID fits id_t");
        // SAFETY: info is writable; this observes only our unreaped child.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful waitid initializes the SIGCHLD fields.
        Ok(unsafe { info.si_pid() } != 0)
    }

    fn wait_until(&self, deadline: Instant) -> io::Result<()> {
        loop {
            if self.has_exited()? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "subprocess deadline exceeded",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn terminate(&mut self) -> io::Result<ExitStatus> {
        if let Some(group) = self.group.take() {
            // SAFETY: the unreaped child reserves this positive process-group
            // ID. Signal descendants before wait releases that reservation.
            if unsafe { libc::kill(-group, libc::SIGKILL) } != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    eprintln!("private subprocess group kill failed: {error}");
                    self.child.kill()?;
                }
            }
        }
        self.child.wait()
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if self.group.is_some() {
            if let Err(error) = self.terminate() {
                eprintln!("private subprocess termination/reap failed: {error}");
            }
        }
    }
}

fn run_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    // Files, not pipes: an inherited pipe held by a stuck grandchild would make
    // reader-thread joins unbounded even after the immediate child is killed.
    let mut stdout = tempfile::tempfile_in(runtime_directory())?;
    let mut stderr = tempfile::tempfile_in(runtime_directory())?;
    let mut process = OwnedProcess::spawn(
        command
            .stdout(Stdio::from(stdout.try_clone()?))
            .stderr(Stdio::from(stderr.try_clone()?)),
    )?;
    process.wait_until(Instant::now() + timeout)?;
    let status = process.terminate()?;
    stdout.seek(SeekFrom::Start(0))?;
    stderr.seek(SeekFrom::Start(0))?;
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    stdout.read_to_end(&mut output.stdout)?;
    stderr.read_to_end(&mut output.stderr)?;
    Ok(output)
}

fn run(command: &mut Command) -> Output {
    let output = run_with_timeout(command, COMMAND_TIMEOUT).unwrap_or_else(|error| {
        panic!(
            "{} failed: {error}",
            command.get_program().to_string_lossy()
        )
    });
    assert!(
        output.status.success(),
        "{} exited {}: {}",
        command.get_program().to_string_lossy(),
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

struct Daemon {
    process: OwnedProcess,
    // Fields drop in declaration order: terminate children before deleting files.
    root: TempDir,
}

impl Daemon {
    fn start() -> Self {
        let root = tempfile::Builder::new()
            .prefix("sc-")
            .tempdir_in(runtime_directory())
            .unwrap();
        assert!(
            root.path().join("socket").as_os_str().as_bytes().len() < 104,
            "set XDG_RUNTIME_DIR to a writable short path for the private Unix socket"
        );
        let config = harness::sccache_client_cfg(root.path(), false);
        harness::write_json_cfg(root.path(), "config.json", &config);
        let log = File::create(root.path().join("daemon.log")).unwrap();
        // Own the actual server process: never ask sccache to fork a daemon.
        let process = OwnedProcess::spawn(
            Self::client_command(root.path())
                .env("SCCACHE_START_SERVER", "1")
                .env("SCCACHE_NO_DAEMON", "1")
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log)),
        )
        .unwrap();
        let daemon = Self { root, process };
        let deadline = Instant::now() + COMMAND_TIMEOUT;
        loop {
            assert!(
                !daemon.process.has_exited().unwrap(),
                "private sccache exited during startup: {}",
                daemon.log()
            );
            if UnixStream::connect(daemon.root.path().join("socket")).is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "private sccache startup timed out: {}",
                daemon.log()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        daemon.stats();
        daemon
    }

    fn client_command(root: &Path) -> Command {
        let mut command = match std::env::var_os("SCCACHE_TEST_BINARY") {
            Some(binary) => harness::prune_command(Command::new(binary)),
            None => harness::sccache_command(),
        };
        command
            .current_dir(root)
            .env("SCCACHE_SERVER_UDS", root.join("socket"))
            .env("SCCACHE_CONF", root.join("config.json"))
            .env("SCCACHE_CACHED_CONF", root.join("cached-config"))
            // A reconnect must never detach an unowned replacement server.
            .env("SCCACHE_NO_DAEMON", "1")
            // A final safety net if the test runner itself is abruptly killed.
            .env("SCCACHE_IDLE_TIMEOUT", "60");
        command
    }

    fn command(&self) -> Command {
        Self::client_command(self.root.path())
    }

    fn log(&self) -> String {
        fs::read_to_string(self.root.path().join("daemon.log"))
            .unwrap_or_else(|error| format!("cannot read private daemon log: {error}"))
    }

    fn stats(&self) -> ServerInfo {
        let output = run(self.command().args(["--show-stats", "--stats-format=json"]));
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn wait_for_writes(&self, expected: u64) {
        let deadline = Instant::now() + COMMAND_TIMEOUT;
        loop {
            let stats = self.stats().stats;
            assert_eq!(stats.cache_write_errors, 0);
            if stats.cache_writes >= expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "cache publication timed out: {stats:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn rust_compile(&self, compiler: &Path) {
        run(self.command().arg(compiler).args([
            "--crate-name",
            "alias_value",
            "--crate-type",
            "rlib",
            "--emit=link",
            "--out-dir",
            ".",
            "value.rs",
        ]));
    }

    fn assert_rust_value(&self, rustc: &Path, expected: u32) {
        fs::write(self.root.path().join("main.rs"), format!(
            "extern crate alias_value; fn main() {{ assert_eq!(alias_value::value(), {expected}); }}\n"
        )).unwrap();
        run(Command::new(rustc).current_dir(self.root.path()).args([
            "main.rs",
            "--extern",
            "alias_value=libalias_value.rlib",
            "-o",
            "check",
        ]));
        run(&mut Command::new(self.root.path().join("check")));
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Bounded graceful stop, then OwnedProcess kills the entire private
        // process group and reaps the server, even while a test is unwinding.
        match run_with_timeout(self.command().arg("--stop-server"), STOP_TIMEOUT) {
            Ok(output) if output.status.success() => {}
            Ok(output) => eprintln!("private sccache graceful stop failed: {output:?}"),
            Err(error) => eprintln!("private sccache graceful stop failed: {error}"),
        }
        if let Err(error) = self.process.wait_until(Instant::now() + STOP_TIMEOUT) {
            eprintln!("private sccache graceful exit failed: {error}");
        }
    }
}

fn output_path(output: Output) -> PathBuf {
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
}

fn real_rustc() -> PathBuf {
    let sysroot = output_path(run(Command::new("rustc")
        .env("RUSTUP_AUTO_INSTALL", "0")
        .arg("--print=sysroot")));
    let rustc = fs::canonicalize(sysroot.join("bin/rustc")).unwrap();
    assert_eq!(
        rustc.file_name().unwrap(),
        "rustc",
        "test requires same-basename rustc"
    );
    rustc
}

#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn retired_rust_alias_keeps_compiling_and_shares_cache() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let root = daemon.root.path();
    for alias in ["alias-a", "alias-b"] {
        fs::create_dir(root.join(alias)).unwrap();
        symlink(&rustc, root.join(alias).join("rustc")).unwrap();
    }
    fs::write(root.join("value.rs"), "pub fn value() -> u32 { 17 }\n").unwrap();
    daemon.rust_compile(&root.join("alias-a/rustc"));
    daemon.assert_rust_value(&rustc, 17);
    daemon.wait_for_writes(1);
    fs::remove_dir_all(root.join("alias-a")).unwrap();
    fs::remove_file(root.join("libalias_value.rlib")).unwrap();
    fs::write(root.join("value.rs"), "pub fn value() -> u32 { 29 }\n").unwrap();
    // The old daemon retains alias-a/rustc in RustHasher and fails ENOENT here.
    daemon.rust_compile(&root.join("alias-b/rustc"));
    daemon.assert_rust_value(&rustc, 29);
    daemon.wait_for_writes(2);
    fs::remove_file(root.join("libalias_value.rlib")).unwrap();
    // The stable path and surviving alias must still address one artifact key.
    daemon.rust_compile(&rustc);
    daemon.assert_rust_value(&rustc, 29);
    let stats = daemon.stats().stats;
    assert_eq!(stats.cache_misses.all(), 2);
    assert_eq!(stats.cache_hits.all(), 1);
}

#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn retired_c_alias_and_clang_multicall() {
    let clang = fs::canonicalize(
        std::env::var_os("SCCACHE_TEST_CLANG")
            .map(PathBuf::from)
            .unwrap_or_else(|| which::which("clang").unwrap()),
    )
    .unwrap();
    let daemon = Daemon::start();
    let root = daemon.root.path();
    let name = clang.file_name().unwrap();
    for alias in ["alias-a", "alias-b"] {
        fs::create_dir(root.join(alias)).unwrap();
        symlink(&clang, root.join(alias).join(name)).unwrap();
    }
    let compile = |compiler: &Path| {
        run(daemon
            .command()
            .arg(compiler)
            .args(["-c", "value.c", "-o", "value.o"]));
    };
    fs::write(root.join("value.c"), "int value(void) { return 17; }\n").unwrap();
    compile(&root.join("alias-a").join(name));
    daemon.wait_for_writes(1);
    fs::remove_dir_all(root.join("alias-a")).unwrap();
    fs::remove_file(root.join("value.o")).unwrap();
    fs::write(root.join("value.c"), "int value(void) { return 29; }\n").unwrap();
    compile(&root.join("alias-b").join(name));
    fs::write(
        root.join("main.c"),
        "int value(void); int main(void) { return value() != 29; }\n",
    )
    .unwrap();
    run(Command::new("cc")
        .current_dir(root)
        .args(["main.c", "value.o", "-o", "check"]));
    run(&mut Command::new(root.join("check")));

    // A .c file must be treated as C++ because argv[0] is clang++, even though
    // that symlink points to clang. Canonicalizing unconditionally breaks this.
    symlink(&clang, root.join("clang++")).unwrap();
    fs::write(
        root.join("value.c"),
        "template<class T> T value() { return 41; }\nint main() { return value<int>() != 41; }\n",
    )
    .unwrap();
    compile(&root.join("clang++"));
    run(Command::new("cc")
        .current_dir(root)
        .args(["value.o", "-o", "check"]));
    run(&mut Command::new(root.join("check")));
}

#[test]
#[ignore = "requires rustup and a real rustc; launches a private daemon"]
fn rustup_proxy_preserves_basename_and_toolchain_resolution() {
    let rustup = which::which("rustup").unwrap();
    let rustc = real_rustc();
    let sysroot = output_path(run(Command::new(&rustc).arg("--print=sysroot")));
    let version = String::from_utf8(run(Command::new(&rustc).arg("-vV")).stdout).unwrap();
    let host = version
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let daemon = Daemon::start();
    let root = daemon.root.path();
    let rustup_home = root.join("rustup-home");
    let cargo_home = root.join("cargo-home");
    fs::create_dir(&rustup_home).unwrap();
    fs::create_dir(&cargo_home).unwrap();
    run(Command::new(&rustup)
        .env("RUSTUP_HOME", &rustup_home)
        .env("CARGO_HOME", &cargo_home)
        .env("RUSTUP_AUTO_INSTALL", "0")
        .args(["toolchain", "link", "local-installed"])
        .arg(&sysroot));
    // sccache probes `rustc +stable` to recognize Rustup. Supply that installed
    // name locally too; automatic installation remains explicitly disabled.
    symlink(
        &sysroot,
        rustup_home
            .join("toolchains")
            .join(format!("stable-{host}")),
    )
    .unwrap();
    symlink(&rustup, root.join("rustup")).unwrap();
    symlink(&rustup, root.join("rustc")).unwrap();
    for expected in [53, 67] {
        fs::write(
            root.join("value.rs"),
            format!("pub fn value() -> u32 {{ {expected} }}\n"),
        )
        .unwrap();
        run(daemon
            .command()
            .env("RUSTUP_HOME", &rustup_home)
            .env("CARGO_HOME", &cargo_home)
            .env("RUSTUP_AUTO_INSTALL", "0")
            .env("RUSTUP_TOOLCHAIN", "local-installed")
            .arg(root.join("rustc"))
            .args([
                "--crate-name",
                "alias_value",
                "--crate-type",
                "rlib",
                "--emit=link",
                "--out-dir",
                ".",
                "value.rs",
            ]));
        daemon.assert_rust_value(&rustc, expected);
    }
    assert_eq!(daemon.stats().stats.cache_misses.all(), 2);
}
