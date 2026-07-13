use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use crate::sandbox::{SandboxConfig, SandboxError, seatbelt_profile};

pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecRequest {
    pub argv: Vec<OsString>,
    /// Absolute, or relative to the workspace mount.
    pub cwd: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnPlan {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
}

#[derive(Debug)]
pub struct ExecOutcome {
    /// The exact status returned by the sandboxed child, including signal state.
    pub status: ExitStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrapperStage {
    ValidateProfile,
    Spawn,
    Wait,
}

#[derive(Debug)]
pub enum ExecError {
    InvalidRequest {
        message: String,
    },
    SandboxDenied {
        message: String,
    },
    WrapperFailure {
        stage: WrapperStage,
        message: String,
        source: Option<io::Error>,
    },
}

impl fmt::Display for ExecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message }
            | Self::SandboxDenied { message }
            | Self::WrapperFailure { message, .. } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ExecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WrapperFailure {
                source: Some(source),
                ..
            } => Some(source),
            _ => None,
        }
    }
}

/// Converts authoritative controller inputs into an argv-only sandbox-exec launch.
/// No child-provided text or environment participates in this plan.
pub fn plan_exec(request: &ExecRequest, sandbox: &SandboxConfig) -> Result<SpawnPlan, ExecError> {
    validate_argv(&request.argv)?;
    let cwd = contained_cwd(&sandbox.workspace_mount, &request.cwd)?;
    let profile = seatbelt_profile(sandbox).map_err(map_sandbox_error)?;

    let mut args = Vec::with_capacity(request.argv.len() + 3);
    args.push(OsString::from("-p"));
    args.push(OsString::from(profile));
    args.push(OsString::from("--"));
    args.extend(request.argv.iter().cloned());

    Ok(SpawnPlan {
        program: PathBuf::from(SANDBOX_EXEC),
        args,
        cwd,
    })
}

pub trait SpawnRunner {
    fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure>;
}

#[derive(Debug)]
pub struct SpawnFailure {
    pub stage: WrapperStage,
    pub source: io::Error,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSpawnRunner;

impl SpawnRunner for SystemSpawnRunner {
    fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
        let mut command = Command::new(&plan.program);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        let mut child = command.spawn().map_err(|source| SpawnFailure {
            stage: WrapperStage::Spawn,
            source,
        })?;
        child.wait().map_err(|source| SpawnFailure {
            stage: WrapperStage::Wait,
            source,
        })
    }
}

pub fn execute_with<R: SpawnRunner>(
    request: &ExecRequest,
    sandbox: &SandboxConfig,
    runner: &R,
) -> Result<ExecOutcome, ExecError> {
    let plan = plan_exec(request, sandbox)?;
    let status = runner
        .run(&plan)
        .map_err(|failure| ExecError::WrapperFailure {
            stage: failure.stage,
            message: format!(
                "sandbox wrapper failed during {:?}: {}",
                failure.stage, failure.source
            ),
            source: Some(failure.source),
        })?;
    Ok(ExecOutcome { status })
}

pub fn execute(request: &ExecRequest, sandbox: &SandboxConfig) -> Result<ExecOutcome, ExecError> {
    execute_with(request, sandbox, &SystemSpawnRunner)
}

fn validate_argv(argv: &[OsString]) -> Result<(), ExecError> {
    if argv.is_empty() {
        return Err(ExecError::InvalidRequest {
            message: "exec requires a non-empty argv".into(),
        });
    }
    if argv[0].is_empty() {
        return Err(ExecError::InvalidRequest {
            message: "exec argv[0] must not be empty".into(),
        });
    }
    if argv.iter().any(|argument| contains_nul(argument)) {
        return Err(ExecError::InvalidRequest {
            message: "exec argv must not contain NUL".into(),
        });
    }
    Ok(())
}

fn contains_nul(value: &OsStr) -> bool {
    value.as_encoded_bytes().contains(&0)
}

fn contained_cwd(workspace_mount: &Path, requested: &Path) -> Result<PathBuf, ExecError> {
    if !workspace_mount.is_absolute() {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not absolute: {}",
                workspace_mount.display()
            ),
            source: None,
        });
    }
    if has_traversal(workspace_mount) {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not canonical: {}",
                workspace_mount.display()
            ),
            source: None,
        });
    }
    if has_traversal(requested) {
        return Err(ExecError::SandboxDenied {
            message: format!("cwd traversal is not allowed: {}", requested.display()),
        });
    }

    let workspace =
        std::fs::canonicalize(workspace_mount).map_err(|source| ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "could not resolve workspace mount {}: {source}",
                workspace_mount.display()
            ),
            source: Some(source),
        })?;
    if workspace != workspace_mount {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not canonical: {} resolves to {}",
                workspace_mount.display(),
                workspace.display()
            ),
            source: None,
        });
    }

    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workspace.join(requested)
    };
    let cwd = std::fs::canonicalize(&candidate).map_err(|source| ExecError::InvalidRequest {
        message: format!("could not resolve cwd {}: {source}", candidate.display()),
    })?;
    if !cwd.starts_with(&workspace) {
        return Err(ExecError::SandboxDenied {
            message: format!(
                "cwd {} is outside workspace {}",
                cwd.display(),
                workspace.display()
            ),
        });
    }
    Ok(cwd)
}

fn has_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
}

fn map_sandbox_error(error: SandboxError) -> ExecError {
    match error {
        SandboxError::GrantIntersectsDeny { .. } => ExecError::SandboxDenied {
            message: error.to_string(),
        },
        SandboxError::InvalidPortBlock { .. } | SandboxError::InvalidPath { .. } => {
            ExecError::WrapperFailure {
                stage: WrapperStage::ValidateProfile,
                message: error.to_string(),
                source: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::os::unix::process::ExitStatusExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::sandbox::{PortBlock, RunSandboxMode, SandboxGrants};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestTree {
        root: PathBuf,
        workspace: PathBuf,
        cwd: PathBuf,
    }

    impl TestTree {
        fn new() -> Self {
            let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let root_alias = std::env::temp_dir().join(format!(
                "cowshed-exec-test-{}-{sequence}",
                std::process::id()
            ));
            let cwd_alias = root_alias.join("workspace/nested");
            fs::create_dir_all(&cwd_alias).unwrap();
            let root = fs::canonicalize(&root_alias).unwrap();
            let workspace = root.join("workspace");
            let cwd = workspace.join("nested");
            Self {
                root,
                workspace,
                cwd,
            }
        }

        fn sandbox(&self) -> SandboxConfig {
            SandboxConfig {
                home: PathBuf::from("/Users/tester"),
                workspace_mount: self.workspace.clone(),
                exec_temp_dir: self.root.join("tmp"),
                port_block: PortBlock::new(40_960, 16).unwrap(),
                mode: RunSandboxMode::ReadWrite,
                grants: SandboxGrants::default(),
                allowed_unix_sockets: vec![],
                additional_denies: vec![],
            }
        }
    }

    impl Drop for TestTree {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[derive(Debug)]
    struct FakeRunner {
        status: ExitStatus,
        plans: RefCell<Vec<SpawnPlan>>,
    }

    impl SpawnRunner for FakeRunner {
        fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
            self.plans.borrow_mut().push(plan.clone());
            Ok(self.status)
        }
    }

    #[test]
    fn argv_is_passed_as_distinct_values_without_a_shell() {
        let tree = TestTree::new();
        let request = ExecRequest {
            argv: vec![
                OsString::from("printf"),
                OsString::from("%s"),
                OsString::from("$(touch /tmp/never); a b"),
            ],
            cwd: PathBuf::from("nested"),
        };
        let plan = plan_exec(&request, &tree.sandbox()).unwrap();
        assert_eq!(plan.program, Path::new(SANDBOX_EXEC));
        assert_eq!(plan.args[0], "-p");
        assert_eq!(plan.args[2], "--");
        assert_eq!(&plan.args[3..], request.argv.as_slice());
        assert_eq!(plan.cwd, tree.cwd);
    }

    #[test]
    fn cwd_traversal_and_symlink_escape_are_authoritative_denials() {
        let tree = TestTree::new();
        let traversal = ExecRequest {
            argv: vec![OsString::from("true")],
            cwd: PathBuf::from("../outside"),
        };
        assert!(matches!(
            plan_exec(&traversal, &tree.sandbox()),
            Err(ExecError::SandboxDenied { .. })
        ));

        let outside = tree.root.join("outside");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, tree.workspace.join("escape")).unwrap();
        let symlink = ExecRequest {
            argv: vec![OsString::from("true")],
            cwd: PathBuf::from("escape"),
        };
        assert!(matches!(
            plan_exec(&symlink, &tree.sandbox()),
            Err(ExecError::SandboxDenied { .. })
        ));
    }

    #[test]
    fn child_status_passes_through_unchanged() {
        let tree = TestTree::new();
        let status = ExitStatus::from_raw(37 << 8);
        let runner = FakeRunner {
            status,
            plans: RefCell::new(vec![]),
        };
        let request = ExecRequest {
            argv: vec![OsString::from("false")],
            cwd: tree.cwd.clone(),
        };
        let outcome = execute_with(&request, &tree.sandbox(), &runner).unwrap();
        assert_eq!(outcome.status, status);
        assert_eq!(runner.plans.borrow().len(), 1);
    }

    #[test]
    fn denied_grant_never_reaches_spawn() {
        let tree = TestTree::new();
        let runner = FakeRunner {
            status: ExitStatus::from_raw(0),
            plans: RefCell::new(vec![]),
        };
        let mut sandbox = tree.sandbox();
        sandbox
            .grants
            .read
            .push(PathBuf::from("/Users/tester/.ssh"));
        let request = ExecRequest {
            argv: vec![OsString::from("true")],
            cwd: tree.cwd.clone(),
        };
        assert!(matches!(
            execute_with(&request, &sandbox, &runner),
            Err(ExecError::SandboxDenied { .. })
        ));
        assert!(runner.plans.borrow().is_empty());
    }

    #[test]
    fn wrapper_io_failure_is_not_a_child_exit() {
        struct FailingRunner;
        impl SpawnRunner for FailingRunner {
            fn run(&self, _plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
                Err(SpawnFailure {
                    stage: WrapperStage::Spawn,
                    source: io::Error::new(io::ErrorKind::NotFound, "missing wrapper"),
                })
            }
        }

        let tree = TestTree::new();
        let request = ExecRequest {
            argv: vec![OsString::from("true")],
            cwd: tree.cwd.clone(),
        };
        assert!(matches!(
            execute_with(&request, &tree.sandbox(), &FailingRunner),
            Err(ExecError::WrapperFailure {
                stage: WrapperStage::Spawn,
                ..
            })
        ));
    }
}
