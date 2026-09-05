//! The devenv evaluation runs as a sandboxed child of the workspace (ee311749), and devenv
//! resolves its runtime directory as `$XDG_RUNTIME_DIR/devenv-<hash>`, falling back to `/tmp`
//! when the variable is unset; it ignores `TMPDIR` on purpose. The executed-child Seatbelt
//! profile denies `/tmp`, so a sandboxed evaluation that inherits no `XDG_RUNTIME_DIR` fails
//! before it evaluates anything, and with it every verb that prepares a workspace environment.
//!
//! This test runs the REAL spawn path — `sandbox-exec`, the executed-child profile, the
//! environment `sandboxed_command` builds — against a stand-in `devenv` placed on the one PATH
//! entry the sandbox resolves first (`<mount>/.cowshed/bin`). The stand-in reports the runtime
//! base it was handed and whether it could create a directory there and under `/tmp`, in the
//! same `print-dev-env --json` envelope the supervisor parses.

#![cfg(target_os = "macos")]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use cowshed_core::metadata::PortBlock;
use cowshed_core::runtime::supervisor::{
    SpawnSink, SystemSpawnSink, sandbox_runtime_dir, sandbox_runtime_link,
};
use cowshed_core::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_gateway_types::WorkspaceToken;

/// `sun_path` on macOS is 104 bytes; devenv keeps its default runtime base short for exactly
/// this reason (`resolve_runtime_dir`, devenv-core `paths.rs`). A base that leaves no room for
/// the `devenv-<7 hex>` component plus a socket name is a base devenv cannot use.
const SUN_PATH_BYTES: usize = 104;
const LONGEST_RUNTIME_SUFFIX: &str = "/devenv-1234567/x.sock";
/// `/Users/<user>/Dev/.cowshed/<owner>/<repo>/<workspace>` at the lengths this host actually
/// has (`/Users/danny/Dev/.cowshed/axe-scale/minigraf/minigraf-query-deps` is 66 bytes).
const PRODUCTION_MOUNT_BYTES: usize = 66;

/// devenv's own `print-dev-env --json` shape, filled in by the stand-in from what the sandbox
/// gave it. Values are booleans-as-strings because everything crosses as an exported variable.
const FAKE_DEVENV: &str = r#"#!/bin/sh
runtime_ok=false
tmp_denied=false
tmpdir_denied=false
if [ -n "$XDG_RUNTIME_DIR" ] && mkdir -m 700 "$XDG_RUNTIME_DIR/devenv-1234567" 2>/dev/null; then
  runtime_ok=true
fi
if mkdir -p "/private/tmp/devenv-1234567-$$" 2>/dev/null; then
  rmdir "/private/tmp/devenv-1234567-$$"
else
  tmp_denied=true
fi
if [ -n "$TMPDIR" ] && mkdir -p "$TMPDIR/devenv-1234567" 2>/dev/null; then
  rmdir "$TMPDIR/devenv-1234567"
else
  tmpdir_denied=true
fi
printf '{"variables":{"XDG_RUNTIME_DIR":{"type":"exported","value":"%s"},"COWSHED_RUNTIME_OK":{"type":"exported","value":"%s"},"COWSHED_TMP_DENIED":{"type":"exported","value":"%s"},"COWSHED_TMPDIR_DENIED":{"type":"exported","value":"%s"},"TMPDIR":{"type":"exported","value":"%s"}}}' "$XDG_RUNTIME_DIR" "$runtime_ok" "$tmp_denied" "$tmpdir_denied" "$TMPDIR"
"#;

fn scratch(label: &str) -> PathBuf {
    let alias = std::env::temp_dir().join(format!(
        "cowshed-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&alias).expect("scratch root");
    // Seatbelt matches resolved paths; `/var/folders` is a symlink into `/private/var`.
    std::fs::canonicalize(&alias).expect("canonical scratch root")
}

fn workspace_with_fake_devenv(root: &Path) -> SandboxConfig {
    let mount = root.join("workspace");
    let private = mount.join(".cowshed");
    std::fs::create_dir_all(private.join("bin")).expect("private bin");
    std::fs::write(
        mount.join(WORKSPACE_TOKEN_PATH),
        WorkspaceToken::from_bytes([7; 32]).encode(),
    )
    .expect("workspace token");
    let fake = private.join("bin/devenv");
    std::fs::write(&fake, FAKE_DEVENV).expect("fake devenv");
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let home = root.join("home");
    let exec_temp_dir = root.join("tmp");
    std::fs::create_dir_all(&home).expect("home");
    std::fs::create_dir_all(&exec_temp_dir).expect("exec temp dir");
    SandboxConfig {
        home,
        mount_root: root.to_path_buf(),
        workspace_mount: mount,
        exec_temp_dir,
        port_block: PortBlock::new(40_960, 16).expect("port block"),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
    }
}

fn exported(printed: &serde_json::Value, name: &str) -> String {
    let variable = &printed["variables"][name];
    assert_eq!(
        variable["type"], "exported",
        "{name} must reach the evaluation as an exported variable: {printed}"
    );
    variable["value"]
        .as_str()
        .unwrap_or_else(|| panic!("{name} carries a string value: {printed}"))
        .to_owned()
}

#[tokio::test]
async fn the_sandboxed_devenv_evaluation_owns_a_runtime_directory_the_profile_lets_it_write() {
    let root = scratch("devenv-runtime");
    let sandbox = workspace_with_fake_devenv(&root);
    let devenv_dir = sandbox.workspace_mount.clone();

    let output = SystemSpawnSink
        .print_devenv_env(&devenv_dir, &sandbox)
        .await
        .expect("the evaluation spawns through the sandbox");
    assert!(
        output.succeeded(),
        "the evaluation exits 0 inside the sandbox; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the stand-in printed the devenv envelope");

    // The regression condition: the profile still denies `/tmp`, so the only runtime base the
    // evaluation can use is the one the sandbox hands it.
    assert_eq!(
        exported(&printed, "COWSHED_TMP_DENIED"),
        "true",
        "the executed-child profile must keep denying /tmp"
    );

    // `TMPDIR` is the exec temp dir and it IS writable: its grant follows every deny that
    // covers it, so `mktemp` in a child works. It is still not the runtime base - devenv
    // ignores TMPDIR for that by design, and the socket path length budget is the workspace
    // runtime dir's reason to exist - so the evaluation must be handed the runtime dir
    // explicitly, which the assertion below checks.
    assert_eq!(
        exported(&printed, "COWSHED_TMPDIR_DENIED"),
        "false",
        "TMPDIR ({}) is the sandbox's own scratch and must be writable",
        exported(&printed, "TMPDIR")
    );

    // The child sees the short `/tmp/cs-<port>` link - the `sun_path` budget - and it resolves
    // onto the shed's own runtime directory, which is what the profile grants.
    let runtime_link = PathBuf::from(exported(&printed, "XDG_RUNTIME_DIR"));
    assert_eq!(runtime_link, sandbox_runtime_link(&sandbox));
    let runtime_base =
        std::fs::read_link(&runtime_link).expect("the runtime link exists on the host");
    assert_eq!(
        runtime_base,
        sandbox_runtime_dir(&sandbox),
        "the link resolves to the workspace's own runtime directory"
    );
    assert!(
        runtime_base.is_absolute() && runtime_base.is_dir(),
        "the runtime base must exist before the child runs: {}",
        runtime_base.display()
    );
    assert!(
        runtime_base.starts_with(&sandbox.workspace_mount),
        "a shed carries its own runtime base inside its mount, never the host's /tmp: {}",
        runtime_base.display()
    );
    // Write-allowed by the profile, proven by the child rather than by reading the profile: the
    // stand-in created devenv's own `devenv-<hash>`-shaped subdirectory there, mode 0700, exactly
    // as `create_runtime_dir` does.
    assert_eq!(
        exported(&printed, "COWSHED_RUNTIME_OK"),
        "true",
        "the child must be able to create its runtime directory under {}",
        runtime_base.display()
    );
    let probe = runtime_base.join("devenv-1234567");
    assert!(
        probe.is_dir(),
        "the probe directory lands on the host side too"
    );
    assert_eq!(
        std::fs::metadata(&probe)
            .expect("probe metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    // Short enough for the unix-domain sockets devenv puts under it. The scratch mount here is
    // arbitrarily long, so the budget is checked on what the runtime base ADDS to the mount: a
    // production mount (`~/Dev/.cowshed/<owner>/<repo>/<workspace>`) is about 65 bytes, and the
    // suffix plus devenv's own `devenv-<7 hex>/<socket>` must fit in what remains.
    let added = runtime_base
        .strip_prefix(&sandbox.workspace_mount)
        .expect("inside the mount")
        .as_os_str()
        .len()
        + 1;
    let longest = PRODUCTION_MOUNT_BYTES + added + LONGEST_RUNTIME_SUFFIX.len();
    assert!(
        longest < SUN_PATH_BYTES,
        "runtime base adds {added} bytes to the mount; a {PRODUCTION_MOUNT_BYTES}-byte mount then leaves a {longest}-byte socket path, over the {SUN_PATH_BYTES}-byte sun_path limit"
    );

    std::fs::remove_dir_all(&root).ok();
}
