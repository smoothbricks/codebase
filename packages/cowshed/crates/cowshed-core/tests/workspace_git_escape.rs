#![cfg(target_os = "macos")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_core::fork_lock::Run as _;
use cowshed_core::git::{git_command_at, sandboxed_git_command_at};
use cowshed_core::metadata::PortBlock;
use cowshed_core::sandbox::{
    RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, seatbelt_profile,
};

fn git(root: &Path, args: &[&str]) {
    let output = git_command_at(root)
        .args(args)
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output_locked()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn sandbox(workspace: &Path, temp: &Path) -> SandboxConfig {
    let home = temp.join("home");
    let execution = temp.join("execution");
    fs::create_dir_all(&home).expect("home");
    fs::create_dir_all(&execution).expect("execution");
    SandboxConfig {
        home,
        mount_root: temp.join("mounts"),
        workspace_mount: workspace.to_owned(),
        shed_links: Vec::new(),
        exec_temp_dir: execution,
        port_block: PortBlock::new(40_960, 16).expect("port block"),
        retained_port_blocks: Vec::new(),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        git_worktree_repository: None,
        build_volume_mount: None,
        capabilities: Default::default(),
    }
}

fn job(config: &SandboxConfig, script: &str) -> std::process::Output {
    let profile = seatbelt_profile(config, SandboxProfileRole::ExecutedChild).expect("profile");
    Command::new("/usr/bin/sandbox-exec")
        .args(["-p", &profile, "--", "/bin/sh", "-c", script])
        .current_dir(&config.workspace_mount)
        .env("HOME", config.workspace_mount.join(".cowshed/home"))
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output_locked()
        .expect("sandboxed job")
}

#[test]
fn workspace_hooks_cannot_run_as_controller_on_rebase() {
    let temp = std::env::temp_dir().join(format!("cowshed-git-escape-{}", uuid::Uuid::new_v4()));
    let workspace = temp.join("workspace");
    let marker = temp.join("outside-workspace-marker");
    fs::create_dir_all(&workspace).expect("workspace directory");
    let workspace = fs::canonicalize(workspace).expect("canonical workspace");
    let config = sandbox(&workspace, &temp);
    let result = std::panic::catch_unwind(|| {
        git(&workspace, &["init", "-q", "-b", "main"]);
        fs::write(workspace.join("base"), "base\n").expect("base");
        git(&workspace, &["add", "base"]);
        git(&workspace, &["commit", "-qm", "base"]);
        git(&workspace, &["branch", "target"]);
        git(&workspace, &["switch", "-q", "target"]);
        fs::write(workspace.join("upstream"), "upstream\n").expect("target change");
        git(&workspace, &["add", "upstream"]);
        git(&workspace, &["commit", "-qm", "target change"]);
        git(&workspace, &["switch", "-q", "main"]);
        fs::write(workspace.join("change"), "change\n").expect("change");
        git(&workspace, &["add", "change"]);
        git(&workspace, &["commit", "-qm", "change"]);
        let hooks = workspace.join(".git/hooks");
        let install = job(
            &config,
            &format!(
                "for hook in pre-rebase reference-transaction; do printf '#!/bin/sh\nprintf escaped >> \"{}\"\n' > \".git/hooks/$hook\"; chmod +x \".git/hooks/$hook\"; done",
                marker.display(),
            ),
        );
        assert!(
            install.status.success(),
            "job could not write hook: {}",
            String::from_utf8_lossy(&install.stderr)
        );
        for name in ["pre-rebase", "reference-transaction"] {
            assert!(
                hooks.join(name).is_file(),
                "job wrote {name} inside the checkout"
            );
        }
        let rebase = sandboxed_git_command_at(&workspace)
            .expect("controller Git sandbox")
            .args(["rebase", "target"])
            .env("GIT_AUTHOR_NAME", "fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
            .env("GIT_COMMITTER_NAME", "fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
            .output_locked()
            .expect("controller rebase");
        assert!(
            rebase.status.success(),
            "rebase: {}",
            String::from_utf8_lossy(&rebase.stderr)
        );
        assert!(
            !marker.exists(),
            "workspace hook ran on the controller's host"
        );
    });
    fs::remove_dir_all(temp).expect("remove fixture");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn denied_workspace_paths_resist_write_rename_link_and_symlink() {
    let temp = std::env::temp_dir().join(format!("cowshed-git-deny-{}", uuid::Uuid::new_v4()));
    let workspace = temp.join("workspace");
    fs::create_dir_all(workspace.join(".git/hooks")).expect("hooks directory");
    let workspace = fs::canonicalize(workspace).expect("canonical workspace");
    git(&workspace, &["init", "-q", "-b", "main"]);
    let mut config = sandbox(&workspace, &temp);
    config.grants.deny_write = vec![
        PathBuf::from(".git/hooks"),
        PathBuf::from(".git/config"),
        PathBuf::from(".policy"),
    ];
    let result = std::panic::catch_unwind(|| {
        let original_config = fs::read(workspace.join(".git/config")).expect("existing config");
        for script in [
            "printf exploit > .git/hooks/pre-rebase",
            "printf exploit > .git/config",
            "mkdir .policy",
            "mv .git moved-git",
            "ln .git/config linked-config",
            "ln -s .git/config config-alias; printf exploit > config-alias",
            "mkdir clean; mv clean .git/hooks",
        ] {
            let output = job(&config, script);
            assert!(!output.status.success(), "denied job succeeded: {script}");
        }
        let output = job(&config, "printf fine > ordinary");
        assert!(
            output.status.success(),
            "ordinary workspace write failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let commit = job(
            &config,
            "/var/select/developer_dir/usr/bin/git -c core.hooksPath=/dev/null add ordinary && /var/select/developer_dir/usr/bin/git -c core.hooksPath=/dev/null -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm allowed",
        );
        assert!(
            commit.status.success(),
            "ordinary Git commit failed: {}",
            String::from_utf8_lossy(&commit.stderr)
        );
        assert_eq!(
            fs::read(workspace.join(".git/config")).expect("config"),
            original_config
        );
        assert!(!workspace.join(".git/hooks/pre-rebase").exists());
    });
    fs::remove_dir_all(temp).expect("remove fixture");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn repository_filter_cannot_write_to_controller_home() {
    let temp = std::env::temp_dir().join(format!("cowshed-filter-{}", uuid::Uuid::new_v4()));
    let checkout = temp.join("checkout");
    fs::create_dir_all(&checkout).expect("checkout");
    let checkout = fs::canonicalize(checkout).expect("canonical checkout");
    let marker = temp.join("outside-filter-marker");
    let result = std::panic::catch_unwind(|| {
        git(&checkout, &["init", "-q", "-b", "main"]);
        fs::write(checkout.join(".gitattributes"), "data filter=escape\n").expect("attributes");
        fs::write(checkout.join("data"), "data\n").expect("data");
        git(&checkout, &["add", ".gitattributes", "data"]);
        git(&checkout, &["commit", "-qm", "tracked data"]);
        git(
            &checkout,
            &[
                "config",
                "filter.escape.smudge",
                &format!("sh -c 'echo escaped >> \"{}\"; cat'", marker.display()),
            ],
        );
        fs::remove_file(checkout.join("data")).expect("force checkout");
        git(&checkout, &["checkout", "--", "data"]);
        assert!(
            marker.exists(),
            "host write from configured filter must reproduce"
        );
        fs::remove_file(&marker).expect("reset marker");
        fs::remove_file(checkout.join("data")).expect("force sandboxed checkout");
        let output = sandboxed_git_command_at(&checkout)
            .expect("sandboxed Git")
            .args(["checkout", "--", "data"])
            .output_locked()
            .expect("sandboxed checkout");
        assert!(
            output.status.success(),
            "checkout: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !marker.exists(),
            "filter escaped the controller Git sandbox"
        );
    });
    fs::remove_dir_all(temp).expect("remove fixture");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn controller_git_refuses_a_symlinked_workspace_identity() {
    let temp = std::env::temp_dir().join(format!("cowshed-git-identity-{}", uuid::Uuid::new_v4()));
    let workspace = temp.join("workspace");
    fs::create_dir_all(&workspace).expect("workspace directory");
    let workspace = fs::canonicalize(workspace).expect("canonical workspace");
    let result = std::panic::catch_unwind(|| {
        git(&workspace, &["init", "-q", "-b", "main"]);
        let identity = workspace.join(".cowshed/git-identity.inc");
        fs::create_dir_all(identity.parent().expect("identity directory"))
            .expect("identity directory");
        let foreign = temp.join("foreign-identity.inc");
        fs::write(&foreign, b"[user]\n\tname = foreign\n").expect("foreign identity");
        std::os::unix::fs::symlink(&foreign, &identity).expect("symlinked identity");
        let error = match sandboxed_git_command_at(&workspace) {
            Ok(_) => panic!("controller accepted a symlinked Git identity"),
            Err(error) => error,
        };
        assert_eq!(error.code.as_str(), "integrity");
    });
    fs::remove_dir_all(temp).expect("remove fixture");
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
