//! Real-compiler regressions, copied into upstream tests by the standalone flake.
//! Run explicitly: cargo test --no-default-features --test real_compiler -- --ignored
//! Requires rustc and clang on PATH; the Rustup control also requires rustup,
//! but uses private homes and the already-installed real toolchain. No downloads.
//! SCCACHE_TEST_CLANG may select an unwrapped clang for the multicall control;
//! cc on PATH supplies the platform linker and its SDK/libc configuration.
//! SCCACHE_TEST_BINARY may name the installed binary for before/after comparison.
//! Set XDG_RUNTIME_DIR to a writable short path when the default temp path is long.
#![cfg(unix)]

use sccache::server::ServerInfo;
use std::env::consts::{DLL_EXTENSION, DLL_PREFIX};
use std::ffi::{OsStr, OsString};
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
        Self::start_with(false)
    }

    /// Start a private daemon, with preprocessor cache ("direct") mode on or off.
    fn start_with(preprocessor_cache_mode: bool) -> Self {
        let root = tempfile::Builder::new()
            .prefix("sc-")
            .tempdir_in(runtime_directory())
            .unwrap();
        assert!(
            root.path().join("socket").as_os_str().as_bytes().len() < 104,
            "set XDG_RUNTIME_DIR to a writable short path for the private Unix socket"
        );
        let config = harness::sccache_client_cfg(root.path(), preprocessor_cache_mode);
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

/// Two checkouts of the same sources at different paths. Canonical, as the client's working
/// directory will be.
struct Checkouts {
    a: PathBuf,
    b: PathBuf,
}

impl Checkouts {
    fn new(root: &Path, files: &[(&str, &str)]) -> Self {
        let root = fs::canonicalize(root).unwrap();
        let [a, b] = ["checkout-a", "checkout-b"].map(|name| root.join(name));
        for checkout in [&a, &b] {
            for (path, contents) in files {
                let path = checkout.join(path);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, contents).unwrap();
            }
        }
        Self { a, b }
    }
}

impl Daemon {
    /// Compile `<name>/src/lib.rs` the way cargo compiles a workspace member, opted into cwd
    /// normalization as every cowshed workspace is: from the checkout root, with a relative
    /// source path and an absolute manifest and output directory. Returns the output directory.
    fn cargo_compile(
        &self,
        rustc: &Path,
        checkout: &Path,
        name: &str,
        extra: &[&OsStr],
    ) -> PathBuf {
        self.cargo_compile_env(rustc, checkout, name, extra, &[])
    }

    /// `cargo_compile` with more client environment.
    fn cargo_compile_env(
        &self,
        rustc: &Path,
        checkout: &Path,
        name: &str,
        extra: &[&OsStr],
        env: &[(&str, &OsStr)],
    ) -> PathBuf {
        let out = checkout.join("target");
        fs::create_dir_all(&out).unwrap();
        run(self
            .command()
            .current_dir(checkout)
            .env("SCCACHE_BASEDIR_CWD", "1")
            .env("CARGO_MANIFEST_DIR", checkout.join(name))
            .envs(env.iter().copied())
            .arg(rustc)
            .args([
                "--crate-name",
                name,
                "--edition=2021",
                "--crate-type",
                "lib",
                "--emit=dep-info,metadata,link",
            ])
            .arg(format!("{name}/src/lib.rs"))
            .arg("--out-dir")
            .arg(&out)
            .args(extra));
        out
    }
}

fn names(file: &Path, path: &Path) -> bool {
    let path = path.as_os_str().as_bytes();
    fs::read(file)
        .unwrap()
        .windows(path.len())
        .any(|window| window == path)
}

/// rustc records its working directory in rmeta and in debuginfo. A cwd-normalized key is
/// shared by every checkout, so its entry must not carry the first checkout's path to the
/// next: unremapped, checkout B is handed an rmeta and line tables that name checkout A.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn shared_entry_names_no_checkout() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(
        daemon.root.path(),
        &[(
            "plain/src/lib.rs",
            "pub fn located() -> u32 { line!() }\npub fn made<T: Default>() -> T { T::default() }\n",
        )],
    );
    let line_tables = [OsStr::new("-C"), OsStr::new("debuginfo=line-tables-only")];
    daemon.cargo_compile(&rustc, &checkouts.a, "plain", &line_tables);
    daemon.wait_for_writes(1);
    let out = daemon.cargo_compile(&rustc, &checkouts.b, "plain", &line_tables);
    let stats = daemon.stats().stats;
    assert_eq!(
        (stats.cache_misses.all(), stats.cache_hits.all()),
        (1, 1),
        "checkout B must reuse checkout A's entry"
    );
    for artifact in ["libplain.rlib", "libplain.rmeta"] {
        assert!(
            !names(&out.join(artifact), &checkouts.a),
            "checkout B's {artifact} names checkout A"
        );
    }
}

/// A proc macro can read a checkout path that no key input reveals: here through
/// std::env::var("CARGO_MANIFEST_DIR"), which dep-info does not record, from one dylib that
/// both checkouts load, as every registry proc macro is. The output names the checkout it was
/// compiled in, so only that checkout may reuse it.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn path_reading_proc_macro_output_stays_with_its_checkout() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let root = fs::canonicalize(daemon.root.path()).unwrap();
    let registry = root.join("registry");
    fs::create_dir(&registry).unwrap();
    fs::write(
        registry.join("pm.rs"),
        "extern crate proc_macro;\n\
         #[proc_macro]\n\
         pub fn manifest_dir(_: proc_macro::TokenStream) -> proc_macro::TokenStream {\n\
         \x20   format!(\"{:?}\", std::env::var(\"CARGO_MANIFEST_DIR\").unwrap()).parse().unwrap()\n\
         }\n",
    )
    .unwrap();
    let dylib = registry.join(format!("{DLL_PREFIX}pm.{DLL_EXTENSION}"));
    run(Command::new(&rustc)
        .current_dir(&registry)
        .args([
            "--crate-name",
            "pm",
            "--edition=2021",
            "--crate-type",
            "proc-macro",
            "pm.rs",
            "-o",
        ])
        .arg(&dylib));
    let checkouts = Checkouts::new(
        &root,
        &[(
            "baked/src/lib.rs",
            "pub fn dir() -> &'static str { pm::manifest_dir!() }\n",
        )],
    );
    let mut pm = OsString::from("pm=");
    pm.push(&dylib);
    let externs = [OsStr::new("--extern"), pm.as_os_str()];

    let a = daemon
        .cargo_compile(&rustc, &checkouts.a, "baked", &externs)
        .join("libbaked.rlib");
    assert!(names(&a, &checkouts.a));
    daemon.wait_for_writes(1);
    let b = daemon
        .cargo_compile(&rustc, &checkouts.b, "baked", &externs)
        .join("libbaked.rlib");
    assert!(
        !names(&b, &checkouts.a),
        "checkout B was served checkout A's manifest directory"
    );
    assert!(names(&b, &checkouts.b));
    daemon.wait_for_writes(2);
    // Reuse at the same path survives.
    fs::remove_file(&a).unwrap();
    daemon.cargo_compile(&rustc, &checkouts.a, "baked", &externs);
    assert!(names(&a, &checkouts.a));
    let stats = daemon.stats().stats;
    assert_eq!((stats.cache_misses.all(), stats.cache_hits.all()), (2, 1));
}

/// A request that names its checkout root in SCCACHE_BASEDIR while its cwd is the checkout
/// root, as cargo's is, keys exactly as SCCACHE_BASEDIR_CWD=1 alone: setting it for a whole
/// build reuses every Rust entry that build already stored.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn request_basedir_at_the_cwd_keys_as_the_cwd_alone() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(
        daemon.root.path(),
        &[("plain/src/lib.rs", "pub fn value() -> u32 { 17 }\n")],
    );
    daemon.cargo_compile(&rustc, &checkouts.a, "plain", &[]);
    daemon.wait_for_writes(1);
    daemon.cargo_compile_env(
        &rustc,
        &checkouts.b,
        "plain",
        &[],
        &[("SCCACHE_BASEDIR", checkouts.b.as_os_str())],
    );
    let stats = daemon.stats().stats;
    assert_eq!((stats.cache_misses.all(), stats.cache_hits.all()), (1, 1));
}

/// The dep-info of crate `name` in `out`, as rustc wrote it or a hit restored it.
fn dep_info_of(out: &Path, name: &str) -> String {
    fs::read_to_string(out.join(format!("{name}.d"))).unwrap()
}

/// A crate that includes a file generated below its checkout through an env-dep, as Bun's
/// crates include `${BUN_CODEGEN_DIR}/*.rs`: the env-dep value and the dep-info name the
/// checkout, the output does not. Every checkout shares the entry, and each is handed a
/// dep-info that names its own files, as cargo reads it.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn generated_include_shares_and_restores_its_dep_info() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(
        daemon.root.path(),
        &[
            (
                "gen/src/lib.rs",
                "include!(concat!(env!(\"GEN_DIR\"), \"/gen.rs\"));\n",
            ),
            ("build/codegen/gen.rs", "pub fn value() -> u32 { 41 }\n"),
        ],
    );
    let compile = |checkout: &Path| {
        let gen_dir = checkout.join("build/codegen");
        daemon.cargo_compile_env(
            &rustc,
            checkout,
            "gen",
            &[],
            &[("GEN_DIR", gen_dir.as_os_str())],
        )
    };
    compile(&checkouts.a);
    daemon.wait_for_writes(1);
    let out = compile(&checkouts.b);
    let stats = daemon.stats().stats;
    assert_eq!(
        (stats.cache_misses.all(), stats.cache_hits.all()),
        (1, 1),
        "checkout B must reuse checkout A's entry"
    );
    let dep_info = dep_info_of(&out, "gen");
    let b_gen = checkouts.b.join("build/codegen");
    assert!(
        !dep_info.contains(checkouts.a.to_str().unwrap()),
        "{dep_info}"
    );
    assert!(
        dep_info.contains(&format!("{}/gen.rs", b_gen.display())),
        "{dep_info}"
    );
    assert!(
        dep_info.contains(&format!("# env-dep:GEN_DIR={}\n", b_gen.display())),
        "{dep_info}"
    );
    assert!(!dep_info.contains('\0'), "{dep_info:?}");
    for artifact in ["libgen.rlib", "libgen.rmeta"] {
        assert!(
            !names(&out.join(artifact), &checkouts.a),
            "checkout B's {artifact} names checkout A"
        );
    }
}

/// cargo compiles a registry crate from its package directory, which every checkout shares,
/// with an absolute source path; its build script's OUT_DIR, and the linker cargo is handed,
/// lie below the checkout, which the build names in SCCACHE_BASEDIR. The dep-info names both
/// bases, and each checkout is handed its own.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn registry_crate_shares_across_checkouts() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let root = fs::canonicalize(daemon.root.path()).unwrap();
    let package = root.join("registry/dep-1.0.0");
    fs::create_dir_all(package.join("src")).unwrap();
    fs::write(
        package.join("src/lib.rs"),
        "include!(concat!(env!(\"OUT_DIR\"), \"/out.rs\"));\n",
    )
    .unwrap();
    let checkouts = Checkouts::new(
        &root,
        &[(
            "target/build/dep/out/out.rs",
            "pub fn value() -> u32 { 43 }\n",
        )],
    );
    let compile = |checkout: &Path| {
        let out = checkout.join("target/deps");
        fs::create_dir_all(&out).unwrap();
        run(daemon
            .command()
            .current_dir(&package)
            .env("SCCACHE_BASEDIR_CWD", "1")
            .env("SCCACHE_BASEDIR", checkout)
            .env("CARGO_MANIFEST_DIR", &package)
            .env("CARGO_TARGET_TEST_LINKER", checkout.join("wrap/cc"))
            .env("OUT_DIR", checkout.join("target/build/dep/out"))
            .arg(&rustc)
            .args([
                "--crate-name",
                "dep",
                "--edition=2021",
                "--crate-type",
                "lib",
                "--emit=dep-info,metadata,link",
            ])
            .arg(package.join("src/lib.rs"))
            .arg("--out-dir")
            .arg(&out));
        out
    };
    compile(&checkouts.a);
    daemon.wait_for_writes(1);
    let out = compile(&checkouts.b);
    let stats = daemon.stats().stats;
    assert_eq!(
        (stats.cache_misses.all(), stats.cache_hits.all()),
        (1, 1),
        "checkout B must reuse checkout A's entry"
    );
    let dep_info = dep_info_of(&out, "dep");
    assert!(
        !dep_info.contains(checkouts.a.to_str().unwrap()),
        "{dep_info}"
    );
    for named in [
        package.join("src/lib.rs"),
        checkouts.b.join("target/build/dep/out/out.rs"),
    ] {
        assert!(dep_info.contains(named.to_str().unwrap()), "{dep_info}");
    }
    for artifact in ["libdep.rlib", "libdep.rmeta"] {
        assert!(
            !names(&out.join(artifact), &checkouts.a),
            "checkout B's {artifact} names checkout A"
        );
    }
}

/// sccache caches no proc-macro crate, so each checkout links its own macro dylib, and every
/// crate the macro expands into hashes that dylib as an `--extern`. The client links it
/// remapped from the request bases (on macOS also with an @rpath install name and no debug
/// map), so both checkouts link the same bytes and the macro's dependent hits across them.
#[test]
#[ignore = "requires a real rustc and launches a private daemon"]
fn proc_macro_dependent_shares_across_checkouts() {
    let rustc = real_rustc();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(
        daemon.root.path(),
        &[
            (
                "pm/src/lib.rs",
                "extern crate proc_macro;\n\
                 #[proc_macro_derive(Answer)]\n\
                 pub fn answer(_: proc_macro::TokenStream) -> proc_macro::TokenStream {\n\
                 \x20   \"impl Point { pub fn answer() -> u32 { 42 } }\".parse().unwrap()\n\
                 }\n",
            ),
            (
                "user/src/lib.rs",
                "#[derive(pm::Answer)]\npub struct Point;\n",
            ),
        ],
    );
    // As cargo compiles a proc macro, with debuginfo on so the objects feed a debug map.
    let compile = |checkout: &Path| {
        let out = checkout.join("target");
        fs::create_dir_all(&out).unwrap();
        run(daemon
            .command()
            .current_dir(checkout)
            .env("SCCACHE_BASEDIR_CWD", "1")
            .env("CARGO_MANIFEST_DIR", checkout.join("pm"))
            .arg(&rustc)
            .args([
                "--crate-name",
                "pm",
                "--edition=2021",
                "--crate-type",
                "proc-macro",
                "--emit=dep-info,link",
                "-C",
                "prefer-dynamic",
                "-C",
                "debuginfo=2",
                "-C",
                "extra-filename=-h",
                "pm/src/lib.rs",
                "--extern",
                "proc_macro",
                "--out-dir",
            ])
            .arg(&out));
        let dylib = out.join(format!("{DLL_PREFIX}pm-h.{DLL_EXTENSION}"));
        let mut pm = OsString::from("pm=");
        pm.push(&dylib);
        daemon.cargo_compile(&rustc, checkout, "user", &[OsStr::new("--extern"), &pm]);
        dylib
    };
    let a = compile(&checkouts.a);
    daemon.wait_for_writes(1);
    let b = compile(&checkouts.b);
    assert!(
        !names(&b, &checkouts.b),
        "checkout B's macro names checkout B"
    );
    assert!(
        fs::read(&a).unwrap() == fs::read(&b).unwrap(),
        "the macro dylib differs between checkouts"
    );
    let stats = daemon.stats().stats;
    assert_eq!(
        (stats.cache_misses.all(), stats.cache_hits.all()),
        (1, 1),
        "checkout B's macro dependent must reuse checkout A's entry"
    );
}

fn real_clang() -> PathBuf {
    fs::canonicalize(
        std::env::var_os("SCCACHE_TEST_CLANG")
            .map(PathBuf::from)
            .unwrap_or_else(|| which::which("clang").unwrap()),
    )
    .unwrap()
}

/// A source whose header bakes its own path into the object through `__FILE__`.
const C_CHECKOUT: &[(&str, &str)] = &[
    (
        "include/value.h",
        "static inline const char *value_file(void) { return __FILE__; }\n",
    ),
    (
        "src/value.c",
        "#include \"value.h\"\nconst char *value(void) { return value_file(); }\n",
    ),
];

impl Daemon {
    /// Compile `src/value.c` the way ninja compiles a cmake or Bun target: from a build
    /// directory below the checkout root, which the client names in SCCACHE_BASEDIR, with the
    /// include directory absolute and a make depfile. Returns the build directory.
    fn c_compile(&self, clang: &Path, checkout: &Path, extra: &[OsString]) -> PathBuf {
        let build = checkout.join("build/release");
        fs::create_dir_all(&build).unwrap();
        let mut include = OsString::from("-I");
        include.push(checkout.join("include"));
        run(self
            .command()
            .current_dir(&build)
            .env("SCCACHE_BASEDIR", checkout)
            .arg(clang)
            .args(["-g", "-MMD", "-MF", "value.o.d", "-c", "../../src/value.c"])
            .args(["-o", "value.o"])
            .arg(include)
            .args(extra));
        build
    }
}

/// With the checkout mapped out of debug info and `__FILE__`, a compile from another
/// checkout's build directory hits, and neither its object nor its depfile names a checkout.
#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn c_request_basedir_shares_objects_across_checkouts() {
    let clang = real_clang();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(daemon.root.path(), C_CHECKOUT);
    let prefix_map = |checkout: &Path| {
        let mut map = OsString::from("-ffile-prefix-map=");
        map.push(checkout);
        map.push("=.");
        vec![map]
    };
    let a = daemon.c_compile(&clang, &checkouts.a, &prefix_map(&checkouts.a));
    daemon.wait_for_writes(1);
    let b = daemon.c_compile(&clang, &checkouts.b, &prefix_map(&checkouts.b));
    let stats = daemon.stats().stats;
    assert_eq!(
        (stats.cache_misses.all(), stats.cache_hits.all()),
        (1, 1),
        "checkout B must reuse checkout A's entry"
    );
    for build in [&a, &b] {
        for checkout in [&checkouts.a, &checkouts.b] {
            assert!(
                !names(&build.join("value.o"), checkout),
                "{} names {}",
                build.join("value.o").display(),
                checkout.display()
            );
        }
    }
    // The depfile is the one this checkout's preprocessing wrote, never the entry's.
    assert!(names(&b.join("value.o.d"), &checkouts.b));
    assert!(!names(&b.join("value.o.d"), &checkouts.a));
}

/// Without a prefix map the object names its checkout, so only that checkout reuses it.
#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn c_object_naming_its_checkout_stays_with_it() {
    let clang = real_clang();
    let daemon = Daemon::start();
    let checkouts = Checkouts::new(daemon.root.path(), C_CHECKOUT);
    let a = daemon.c_compile(&clang, &checkouts.a, &[]);
    assert!(names(&a.join("value.o"), &checkouts.a));
    daemon.wait_for_writes(1);
    let b = daemon.c_compile(&clang, &checkouts.b, &[]);
    assert!(
        !names(&b.join("value.o"), &checkouts.a),
        "checkout B was served checkout A's object"
    );
    assert!(names(&b.join("value.o"), &checkouts.b));
    daemon.wait_for_writes(2);
    fs::remove_file(a.join("value.o")).unwrap();
    daemon.c_compile(&clang, &checkouts.a, &[]);
    assert!(names(&a.join("value.o"), &checkouts.a));
    let stats = daemon.stats().stats;
    assert_eq!((stats.cache_misses.all(), stats.cache_hits.all()), (2, 1));
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Write `contents` with an mtime a minute old: direct mode refuses to record a file modified
/// during the compile, and these tests need it to record every one.
fn write_settled(path: &Path, contents: &[u8]) {
    fs::write(path, contents).unwrap();
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(60))
        .unwrap();
}

impl Daemon {
    /// Compile `args` (a source and its flags) with `clang` through sccache in the daemon root,
    /// and return the bytes of the object it produced.
    fn object(&self, clang: &Path, args: &[&str]) -> Vec<u8> {
        let out = self.root.path().join("out.o");
        if out.exists() {
            fs::remove_file(&out).unwrap();
        }
        run(self
            .command()
            .arg(clang)
            .args(args)
            .args(["-c", "-o", "out.o"]));
        fs::read(out).unwrap()
    }

    fn hits_and_misses(&self) -> (u64, u64) {
        let stats = self.stats().stats;
        (stats.cache_hits.all(), stats.cache_misses.all())
    }
}

/// The assembler, not the preprocessor, opens a `.incbin` file, so the preprocessed text names
/// it and nothing more. Bun's builtin-module blob is found this way, through an `-I` directory
/// below the build directory. The key holds the bytes of the file the assembler reads: the
/// first match of the cwd, then each `-I`, then each `-Wa,-I`.
#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn incbin_keys_the_file_the_assembler_reads() {
    let clang = real_clang();
    let daemon = Daemon::start();
    let root = daemon.root.path();
    fs::create_dir(root.join("gen")).unwrap();
    fs::create_dir(root.join("later")).unwrap();
    fs::write(
        root.join("blob.S"),
        ".data\n.globl blob\nblob:\n.incbin \"blob.bin\", 0, 8\n.incbin \"blob.bin\", 8\n",
    )
    .unwrap();
    fs::write(root.join("gen/blob.bin"), "first-blob-bytes").unwrap();
    fs::write(root.join("later/blob.bin"), "shadowed-one").unwrap();
    let args = ["blob.S", "-Wa,-Ilater", "-Igen"];

    assert!(contains(&daemon.object(&clang, &args), b"first-blob-bytes"));
    daemon.wait_for_writes(1);
    // The assembler stops at gen/, so the copy it never reaches is no input.
    fs::write(root.join("later/blob.bin"), "shadowed-two").unwrap();
    assert!(contains(&daemon.object(&clang, &args), b"first-blob-bytes"));
    assert_eq!(daemon.hits_and_misses(), (1, 1));

    fs::write(root.join("gen/blob.bin"), "other-blob-bytes").unwrap();
    assert!(
        contains(&daemon.object(&clang, &args), b"other-blob-bytes"),
        "a changed .incbin file was served the object of its old bytes"
    );
    daemon.wait_for_writes(2);
    // A copy in the cwd is found before any -I directory.
    fs::write(root.join("blob.bin"), "local-blob-bytes").unwrap();
    assert!(contains(&daemon.object(&clang, &args), b"local-blob-bytes"));
    daemon.wait_for_writes(3);
    fs::remove_file(root.join("blob.bin")).unwrap();
    fs::write(root.join("gen/blob.bin"), "first-blob-bytes").unwrap();
    assert!(contains(&daemon.object(&clang, &args), b"first-blob-bytes"));
    assert_eq!(daemon.hits_and_misses(), (2, 3));
}

/// A `.incbin` whose file the hasher cannot name exactly is compiled every time, never
/// looked up or stored: an operand built by an assembler macro, and one in C inline assembly,
/// which the C compiler hands its own assembler with its own search path.
#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn incbin_the_hasher_cannot_resolve_is_not_cached() {
    let clang = real_clang();
    let daemon = Daemon::start();
    let root = daemon.root.path();
    fs::write(
        root.join("macro.S"),
        ".macro blob_of name\n.incbin \"\\name\"\n.endm\n.data\nblob_of blob.bin\n",
    )
    .unwrap();
    fs::write(
        root.join("inline.c"),
        "__asm__(\".data\\n.incbin \\\"blob.bin\\\"\\n.text\\n\");\n",
    )
    .unwrap();
    for source in ["macro.S", "inline.c"] {
        fs::write(root.join("blob.bin"), "first-blob-bytes").unwrap();
        assert!(contains(
            &daemon.object(&clang, &[source]),
            b"first-blob-bytes"
        ));
        fs::write(root.join("blob.bin"), "other-blob-bytes").unwrap();
        assert!(
            contains(&daemon.object(&clang, &[source]), b"other-blob-bytes"),
            "{source}: a changed .incbin file was served the object of its old bytes"
        );
    }
    let stats = daemon.stats().stats;
    assert_eq!((stats.cache_hits.all(), stats.cache_misses.all()), (0, 0));
    assert_eq!(stats.non_cacheable_compilations, 4);
    assert_eq!(stats.cache_writes, 0);
}

/// Direct mode replays a result recorded against the source and every header the
/// preprocessor's line markers name. `#embed` and `__has_embed` read a file no line marker
/// names, so a translation unit using either must be preprocessed every time; the
/// preprocessed text then carries the embedded bytes, and the key with it.
fn assert_direct_mode_sees(
    files: &[(&str, &str)],
    changed: (&str, &str),
    before: &str,
    after: &str,
) {
    let clang = real_clang();
    let daemon = Daemon::start_with(true);
    let root = daemon.root.path();
    for (name, contents) in files {
        write_settled(&root.join(name), contents.as_bytes());
    }
    let args = ["-std=c23", "unit.c"];
    assert!(contains(&daemon.object(&clang, &args), before.as_bytes()));
    daemon.wait_for_writes(1);
    write_settled(&root.join(changed.0), changed.1.as_bytes());
    assert!(
        contains(&daemon.object(&clang, &args), after.as_bytes()),
        "direct mode served the object of a resource that changed: {}",
        daemon.log()
    );
}

#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn embed_in_the_source_defeats_direct_mode() {
    assert_direct_mode_sees(
        &[
            ("unit.h", "typedef int unit_t;\n"),
            (
                "unit.c",
                "#include \"unit.h\"\nconst unsigned char data[] = {\n#embed \"data.bin\"\n};\n",
            ),
            ("data.bin", "first-embedded-bytes"),
        ],
        ("data.bin", "other-embedded-bytes"),
        "first-embedded-bytes",
        "other-embedded-bytes",
    );
}

#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn embed_in_a_header_defeats_direct_mode() {
    assert_direct_mode_sees(
        &[
            (
                "unit.h",
                "const unsigned char data[] = {\n#embed \"data.bin\"\n};\n",
            ),
            ("unit.c", "#include \"unit.h\"\n"),
            ("data.bin", "first-embedded-bytes"),
        ],
        ("data.bin", "other-embedded-bytes"),
        "first-embedded-bytes",
        "other-embedded-bytes",
    );
}

#[test]
#[ignore = "requires real clang and launches a private daemon"]
fn has_embed_defeats_direct_mode() {
    assert_direct_mode_sees(
        &[
            (
                "unit.h",
                "#if __has_embed(\"optional.bin\")\nconst char flag[] = \"has-optional\";\n\
                 #else\nconst char flag[] = \"lacks-optional\";\n#endif\n",
            ),
            ("unit.c", "#include \"unit.h\"\n"),
        ],
        ("optional.bin", "x"),
        "lacks-optional",
        "has-optional",
    );
}
