//! The port-0 preload every job's Node and Bun load (04_sandbox.md, "A sandboxed job binds its listeners in its
//! block"; 11_shell.md, "Supervisor").
//!
//! On macOS a job connects only to its workspace's port block, so a listener on an OS-picked port binds but nothing in
//! the workspace can reach it. The preload, published into the private environment beside the trust bundle, moves a
//! Node or Bun process's port-0 listens, and a child's `--inspect…=<host>:0`, onto a free service port of the block. Its
//! path reaches the job as one loader option appended to the caller's own `NODE_OPTIONS` and `BUN_OPTIONS`. Where the
//! job environment names no block (Linux, whose workspaces own a private loopback) the preload does nothing.

use std::ffi::CStr;
use std::path::{Path, PathBuf};

/// The preload's bytes, published as [`PRELOAD_NAME`] at the root of the private environment.
pub const PRELOAD: &[u8] = include_bytes!("port_zero.cjs");
/// The preload's file name at the root of the private environment.
pub const PRELOAD_NAME: &CStr = c"port-zero.cjs";

/// A preload path neither runtime can be handed: both split their options variable on whitespace, and Bun keeps a
/// quote as part of the path rather than reading it as quoting.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "{} cannot be named in NODE_OPTIONS or BUN_OPTIONS, which split on whitespace and do not unquote alike",
    .0.display()
)]
pub struct UnsafeLoaderPath(pub PathBuf);

/// The option each runtime loads `preload` with: Node's `--require=<path>` in `NODE_OPTIONS` and Bun's
/// `--preload=<path>` in `BUN_OPTIONS`. Bun takes only the `=` spelling there: it prepends `BUN_OPTIONS` to argv, and a
/// bare `--preload <path>` makes the subcommand that follows (`bun install`, `bun test`) a script name.
pub fn loader_options(preload: &Path) -> Result<[(&'static str, String); 2], UnsafeLoaderPath> {
    let path = preload
        .to_str()
        .filter(|path| {
            !path.contains(|character: char| {
                character.is_whitespace() || matches!(character, '"' | '\'' | '\\')
            })
        })
        .ok_or_else(|| UnsafeLoaderPath(preload.to_owned()))?;
    Ok([
        ("NODE_OPTIONS", format!("--require={path}")),
        ("BUN_OPTIONS", format!("--preload={path}")),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::fork_lock::Run as _;
    use std::process::Command;

    const SCENARIOS: &str = include_str!("../tests/fixtures/port_zero_scenarios.cjs");

    #[test]
    fn each_runtime_loads_the_preload_by_its_own_one_word_option() {
        assert_eq!(
            loader_options(Path::new("/w/.cowshed/port-zero.cjs")),
            Ok([
                (
                    "NODE_OPTIONS",
                    "--require=/w/.cowshed/port-zero.cjs".to_owned()
                ),
                (
                    "BUN_OPTIONS",
                    "--preload=/w/.cowshed/port-zero.cjs".to_owned()
                ),
            ])
        );
        for path in [
            "/w s/p.cjs",
            "/w\t/p.cjs",
            "/w\"/p.cjs",
            "/w'/p.cjs",
            "/w\\/p.cjs",
        ] {
            assert_eq!(
                loader_options(Path::new(path)),
                Err(UnsafeLoaderPath(PathBuf::from(path)))
            );
        }
        let non_utf8 =
            <std::ffi::OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(b"/w\xff/p.cjs");
        assert!(loader_options(Path::new(non_utf8)).is_err());
    }

    /// The block the scenarios' listeners must land in: the enclosing workspace's, whose sandbox refuses a connect
    /// anywhere else, or else the first fixture block.
    fn block() -> (u16, u16) {
        let read = |name: &str| {
            std::env::var(name).ok().map(|value| {
                value
                    .parse::<u16>()
                    .unwrap_or_else(|error| panic!("{name}={value}: {error}"))
            })
        };
        match (read("COWSHED_PORT_BASE"), read("COWSHED_PORT_BLOCK_SIZE")) {
            (Some(base), Some(size)) => (base, size),
            (None, None) => (
                *crate::metadata::PortAllocation::Fixture.range().start(),
                64,
            ),
            half => panic!("a port block exported half: {half:?}"),
        }
    }

    /// `runtime` run on `script` with the preload loaded the way a job loads it, under `block`.
    fn run(
        runtime: &str,
        preload: &Path,
        script: &Path,
        block: Option<(&str, &str)>,
    ) -> std::process::Output {
        let mut command = Command::new(runtime);
        command
            .arg(script)
            .env_remove("COWSHED_PORT_BASE")
            .env_remove("COWSHED_PORT_BLOCK_SIZE")
            .env("PORT_ZERO_NODE", "node")
            .env("PORT_ZERO_BUN", "bun");
        for (name, option) in loader_options(preload).expect("a fixture path is a loader path") {
            command.env(name, option);
        }
        if let Some((base, size)) = block {
            command
                .env("COWSHED_PORT_BASE", base)
                .env("COWSHED_PORT_BLOCK_SIZE", size);
        }
        command
            .output_locked()
            .unwrap_or_else(|error| panic!("run {runtime}: {error}"))
    }

    fn fixture() -> (crate::temp_root::TempRoot, PathBuf) {
        let root = crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "port-zero");
        let preload = root.join("port-zero.cjs");
        std::fs::write(&preload, PRELOAD).expect("write the preload");
        (root, preload)
    }

    /// The listeners that broke Stryker in a workspace — core's LoggingServer (`listen(cb)`), the Bun runner's sync
    /// server (`listen(0, host)`) and its `bun test --inspect=127.0.0.1:0` — and a probe-then-reuse helper and
    /// `Bun.serve({ port: 0 })`, each connected to from the process that bound it; an inspector after `=`-joined
    /// options, and one after a bare flag that stays unmapped and is named on stderr; a crowded and an exhausted block for
    /// listeners and inspectors alike, in an eight-port block the fixture alone holds rather than the shared job block; and
    /// a child's arguments, or an option's operand, that are not its runtime's inspector option, unchanged.
    #[test]
    fn node_and_bun_put_every_port_zero_listener_on_a_service_port_of_the_block() {
        let (root, preload) = fixture();
        let script = root.join("scenarios.cjs");
        std::fs::write(&script, SCENARIOS).expect("write the scenarios");
        let (base, size) = block();
        let (base, size) = (base.to_string(), size.to_string());
        for (runtime, scenarios) in [("node", 16), ("bun", 17)] {
            let output = run(runtime, &preload, &script, Some((&base, &size)));
            let stdout = String::from_utf8_lossy(&output.stdout);
            let report = format!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
            assert!(output.status.success(), "{runtime}: {report}");
            assert_eq!(
                stdout
                    .lines()
                    .filter(|line| line.ends_with(" PASS"))
                    .count(),
                scenarios,
                "{runtime}: {report}"
            );
        }
    }

    /// A block exported half, unaligned or of a size that is no power of two is a broken environment: the preload
    /// refuses it by name rather than guess (04_sandbox.md).
    #[test]
    fn a_broken_block_environment_stops_the_process_by_name() {
        let (root, preload) = fixture();
        let script = root.join("empty.cjs");
        std::fs::write(&script, "").expect("write an empty script");
        for runtime in ["node", "bun"] {
            for block in [
                ("40960", ""),
                ("", "64"),
                ("40961", "64"),
                ("40960", "48"),
                ("x", "64"),
            ] {
                let output = run(runtime, &preload, &script, Some(block));
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(!output.status.success(), "{runtime} {block:?}");
                assert!(
                    stderr.contains("is not a port block"),
                    "{runtime} {block:?}: {stderr}"
                );
            }
        }
    }

    /// With no block in the environment the preload changes nothing: port 0 is the kernel's ephemeral choice, which on
    /// macOS lies above every workspace block.
    #[cfg(target_os = "macos")]
    #[test]
    fn without_a_block_a_port_zero_listener_keeps_the_kernels_port() {
        let (root, preload) = fixture();
        let script = root.join("ephemeral.cjs");
        std::fs::write(
            &script,
            "const s = require('node:net').createServer();\n\
             s.listen(0, '127.0.0.1', () => { process.stdout.write(String(s.address().port) + '\\n'); s.close(); });\n",
        )
        .expect("write the listener");
        for runtime in ["node", "bun"] {
            let output = run(runtime, &preload, &script, None);
            assert!(output.status.success(), "{runtime}: {output:?}");
            let port: u16 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .unwrap_or_else(|error| panic!("{runtime}: {error}: {output:?}"));
            assert!(port > crate::metadata::MACOS_PORT_MAX, "{runtime}: {port}");
        }
    }
}
