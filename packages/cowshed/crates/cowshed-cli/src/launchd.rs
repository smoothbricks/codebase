//! Pure launchd service definitions and filesystem mutation plans.
//!
//! This module deliberately does not execute `launchctl`, provision storage, or
//! perform filesystem I/O. Callers can validate an immutable service definition,
//! render its deterministic plist, and then execute the returned mutation plan
//! using their own platform boundary.

use std::error::Error;
use std::fmt;
use std::path::{Component, Path, PathBuf};

pub const GATEWAY_LABEL: &str = "dev.cowshed.gateway";
pub const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
pub const PRIVATE_PLIST_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceLifecycle {
    /// Start at login and restart whenever the service exits.
    KeepAlive,
    /// Run once when the agent is loaded.
    RunAtLoad,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchAgentSpec {
    label: String,
    executable: PathBuf,
    arguments: Vec<String>,
    lifecycle: ServiceLifecycle,
    plist_path: PathBuf,
    standard_error_path: PathBuf,
}

impl LaunchAgentSpec {
    pub fn new_user(
        home: &Path,
        label: impl Into<String>,
        executable: &Path,
        arguments: Vec<String>,
        lifecycle: ServiceLifecycle,
    ) -> Result<Self, LaunchdError> {
        validate_canonical_absolute_path("home", home)?;
        validate_canonical_absolute_path("executable", executable)?;

        let label = label.into();
        validate_label(&label)?;
        validate_arguments(&arguments)?;

        let plist_path = home
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist"));
        let standard_error_path = home
            .join(".cowshed")
            .join("telemetry")
            .join("daemon-stderr.log");

        Ok(Self {
            label,
            executable: executable.to_path_buf(),
            arguments,
            lifecycle,
            plist_path,
            standard_error_path,
        })
    }

    pub fn gateway(home: &Path, executable: &Path) -> Result<Self, LaunchdError> {
        Self::new_user(
            home,
            GATEWAY_LABEL,
            executable,
            vec!["gateway".into(), "run".into()],
            ServiceLifecycle::KeepAlive,
        )
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn lifecycle(&self) -> ServiceLifecycle {
        self.lifecycle
    }

    pub fn plist_path(&self) -> &Path {
        &self.plist_path
    }

    pub fn launch_agents_directory(&self) -> &Path {
        self.plist_path
            .parent()
            .expect("validated plist paths always have a parent")
    }

    pub fn standard_error_path(&self) -> &Path {
        &self.standard_error_path
    }

    pub fn program_arguments(&self) -> impl Iterator<Item = &str> {
        std::iter::once(
            self.executable
                .to_str()
                .expect("validated executable paths are UTF-8"),
        )
        .chain(self.arguments.iter().map(String::as_str))
    }

    pub fn plist_bytes(&self) -> Vec<u8> {
        let mut plist = String::from(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
            "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
            "<plist version=\"1.0\">\n",
            "<dict>\n",
            "  <key>Label</key>\n",
            "  ",
        ));
        push_xml_string(&mut plist, &self.label);
        plist.push_str("  <key>ProgramArguments</key>\n  <array>\n");
        for argument in self.program_arguments() {
            plist.push_str("    ");
            push_xml_string(&mut plist, argument);
        }
        plist.push_str("  </array>\n  <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n");
        match self.lifecycle {
            ServiceLifecycle::KeepAlive => plist.push_str("  <true/>\n"),
            ServiceLifecycle::RunAtLoad => plist.push_str("  <false/>\n"),
        }
        plist.push_str(
            "  <key>ProcessType</key>\n  <string>Background</string>\n  <key>StandardErrorPath</key>\n  ",
        );
        push_xml_string(
            &mut plist,
            self.standard_error_path
                .to_str()
                .expect("validated home paths are UTF-8"),
        );
        plist.push_str("</dict>\n</plist>\n");
        plist.into_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExistingPlist<'a> {
    pub bytes: &'a [u8],
    pub mode: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InstallState<'a> {
    /// `None` means `~/Library/LaunchAgents` does not exist.
    pub launch_agents_directory_mode: Option<u32>,
    pub plist: Option<ExistingPlist<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallPlan {
    operations: Vec<Mutation>,
}

impl InstallPlan {
    pub fn operations(&self) -> &[Mutation] {
        &self.operations
    }

    pub fn is_noop(&self) -> bool {
        self.operations.is_empty()
    }
}

/// An ordered, filesystem-only mutation plan.
///
/// `CreateExclusiveTemporaryFile` produces the temporary file consumed by the
/// immediately following temporary-file operations. The executor must choose a
/// unique suffix, open with exclusive creation and no symlink following, and
/// clean up that file if a later operation fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    EnsureDirectory {
        path: PathBuf,
        mode: u32,
    },
    SetPermissions {
        path: PathBuf,
        mode: u32,
    },
    CreateExclusiveTemporaryFile {
        directory: PathBuf,
        name_prefix: String,
        bytes: Vec<u8>,
        mode: u32,
    },
    SyncTemporaryFile,
    RenameTemporaryFile {
        destination: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    SyncDirectory {
        path: PathBuf,
    },
}

pub fn plan_install(spec: &LaunchAgentSpec, state: InstallState<'_>) -> InstallPlan {
    let directory = spec.launch_agents_directory().to_path_buf();
    let mut operations = Vec::new();

    match state.launch_agents_directory_mode {
        None => operations.push(Mutation::EnsureDirectory {
            path: directory.clone(),
            mode: PRIVATE_DIRECTORY_MODE,
        }),
        Some(mode) if mode != PRIVATE_DIRECTORY_MODE => {
            operations.push(Mutation::SetPermissions {
                path: directory.clone(),
                mode: PRIVATE_DIRECTORY_MODE,
            });
        }
        Some(_) => {}
    }

    let desired = spec.plist_bytes();
    let plist_is_current = state
        .plist
        .is_some_and(|plist| plist.mode == PRIVATE_PLIST_MODE && plist.bytes == desired);

    if !plist_is_current {
        operations.push(Mutation::CreateExclusiveTemporaryFile {
            directory: directory.clone(),
            name_prefix: format!(".{}.plist.", spec.label()),
            bytes: desired,
            mode: PRIVATE_PLIST_MODE,
        });
        operations.push(Mutation::SyncTemporaryFile);
        operations.push(Mutation::RenameTemporaryFile {
            destination: spec.plist_path().to_path_buf(),
        });
    }

    if !operations.is_empty() {
        operations.push(Mutation::SyncDirectory { path: directory });
    }

    InstallPlan { operations }
}

pub fn plan_remove(spec: &LaunchAgentSpec, installed: bool) -> InstallPlan {
    let operations = if installed {
        vec![
            Mutation::RemoveFile {
                path: spec.plist_path().to_path_buf(),
            },
            Mutation::SyncDirectory {
                path: spec.launch_agents_directory().to_path_buf(),
            },
        ]
    } else {
        Vec::new()
    };

    InstallPlan { operations }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchdError {
    InvalidPath {
        field: &'static str,
        reason: &'static str,
    },
    InvalidLabel,
    InvalidArgument {
        index: usize,
        reason: &'static str,
    },
    PrivilegedProvisioning,
}

impl fmt::Display for LaunchdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath { field, reason } => {
                write!(formatter, "invalid {field} path: {reason}")
            }
            Self::InvalidLabel => formatter.write_str("invalid launchd label"),
            Self::InvalidArgument { index, reason } => {
                write!(formatter, "invalid service argument {index}: {reason}")
            }
            Self::PrivilegedProvisioning => formatter
                .write_str("launchd services may not invoke foreground storage provisioning"),
        }
    }
}

impl Error for LaunchdError {}

fn validate_canonical_absolute_path(field: &'static str, path: &Path) -> Result<(), LaunchdError> {
    let value = path.to_str().ok_or(LaunchdError::InvalidPath {
        field,
        reason: "must be UTF-8",
    })?;
    if value.is_empty() || !path.is_absolute() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be absolute",
        });
    }
    if path.parent().is_none() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must not be the filesystem root",
        });
    }
    if value.chars().any(is_unsafe_xml_control) {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "contains a control character",
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be lexically normalized",
        });
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.as_os_str() != path.as_os_str() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must use its canonical lexical spelling",
        });
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), LaunchdError> {
    let valid = !label.is_empty()
        && !label.starts_with('.')
        && !label.ends_with('.')
        && label.split('.').all(|component| !component.is_empty())
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(LaunchdError::InvalidLabel)
    }
}

fn validate_arguments(arguments: &[String]) -> Result<(), LaunchdError> {
    if arguments.is_empty() {
        return Err(LaunchdError::InvalidArgument {
            index: 0,
            reason: "at least one service argument is required",
        });
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "adopt")
    {
        return Err(LaunchdError::PrivilegedProvisioning);
    }
    for (index, argument) in arguments.iter().enumerate() {
        if argument.is_empty() {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "must not be empty",
            });
        }
        if argument.chars().any(is_unsafe_xml_control) {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "contains a control character",
            });
        }
    }
    Ok(())
}

fn is_unsafe_xml_control(character: char) -> bool {
    matches!(character, '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{7f}')
}

fn push_xml_string(output: &mut String, value: &str) {
    output.push_str("<string>");
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '\'' => output.push_str("&apos;"),
            '"' => output.push_str("&quot;"),
            _ => output.push(character),
        }
    }
    output.push_str("</string>\n");
}
