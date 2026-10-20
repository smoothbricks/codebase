//! Whether a job's argv command can start, judged before its spawn and only on proven absence
//! (11_shell.md, "Job control").
//!
//! A failed `execvp` names the program it was given, whatever was missing. macOS's
//! `sandbox-exec` prints `execvp() of 'tooling/codegraph' failed: No such file or directory`
//! both when that script is absent and when it exists and is executable but its interpreter is
//! not there, because the kernel reports `ENOENT` for either. So before a command is spawned
//! the supervisor follows the same links the kernel will: the program (relative to the job's
//! cwd, or looked up on the job's PATH), then the interpreter its `#!` line names, then the name
//! an `env` interpreter looks up on that PATH. The first one that does not exist refuses the
//! spawn, naming it.
//!
//! Only absence is judged. A path the supervisor cannot inspect (a permission refusal, an
//! unreadable script) is the kernel's to decide inside the sandbox; this is not a second
//! sandbox. A name looked up on a PATH the supervisor does not know, because shell activation
//! sets it inside the sandbox, is not judged either. Nothing here rewrites the command: the kernel
//! still runs the interpreter, so `$0` and `BASH_SOURCE` stay the script's.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{self, Read as _};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

/// The interpreter line the kernel reads: macOS reads 512 bytes of it (xnu `IMG_SHSIZE`),
/// Linux 256 (`BINPRM_BUF_SIZE`). A line longer than what was read is not judged.
const INTERPRETER_LINE_BYTES: usize = 512;

/// The first missing link of a command that cannot start.
#[derive(Debug, Eq, PartialEq)]
pub enum Unstartable {
    /// The command names a path, and nothing exists there.
    Missing { program: PathBuf },
    /// The command is a bare name, and no directory of the job's PATH holds it.
    NotOnPath { name: OsString, path: OsString },
    /// The program exists, and the interpreter its `#!` line names does not.
    Interpreter {
        program: PathBuf,
        interpreter: PathBuf,
    },
    /// The program's `#!` line runs `env` with a name no directory of the job's PATH holds.
    EnvTarget {
        program: PathBuf,
        name: OsString,
        path: OsString,
    },
}

impl fmt::Display for Unstartable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { program } => write!(
                f,
                "{} does not exist, so the command did not start",
                program.display()
            ),
            Self::NotOnPath { name, path } => write!(
                f,
                "no directory of the job's PATH ({}) holds {}, so the command did not start",
                path.display(),
                name.display()
            ),
            Self::Interpreter {
                program,
                interpreter,
            } => write!(
                f,
                "{} exists, but its interpreter {} does not, so the command did not start",
                program.display(),
                interpreter.display()
            ),
            Self::EnvTarget {
                program,
                name,
                path,
            } => write!(
                f,
                "{} exists and runs `env {}`, but no directory of the job's PATH ({}) holds {}, \
                 so the command did not start",
                program.display(),
                name.display(),
                path.display(),
                name.display()
            ),
        }
    }
}

/// What a lookup of one path proved.
enum Found {
    Present(PathBuf),
    Absent,
    /// The supervisor cannot tell; the kernel decides inside the sandbox.
    Unknown,
}

fn inspect(path: PathBuf) -> Found {
    match fs::metadata(&path) {
        Ok(_) => Found::Present(path),
        Err(error) if absent(&error) => Found::Absent,
        Err(_) => Found::Unknown,
    }
}

/// `ENOENT`, or `ENOTDIR` for a path through a file: what `execve` reports as nothing there.
fn absent(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

/// `name` looked up on `path` as `execvp` does: each directory in order, an empty one meaning
/// the current directory.
fn on_path(name: &OsStr, path: &OsStr, cwd: &Path) -> Found {
    for directory in std::env::split_paths(path) {
        let directory = if directory.as_os_str().is_empty() {
            cwd.to_path_buf()
        } else {
            cwd.join(directory)
        };
        match inspect(directory.join(name)) {
            Found::Absent => {}
            found => return found,
        }
    }
    Found::Absent
}

/// The interpreter and its one optional argument from `program`'s `#!` line, as the kernel splits
/// it: the interpreter up to the first blank, the rest with surrounding blanks trimmed. `None`
/// when the program has no such line, or it cannot be read whole.
fn interpreter_line(program: &Path) -> Option<(PathBuf, Option<OsString>)> {
    let mut head = Vec::with_capacity(INTERPRETER_LINE_BYTES);
    fs::File::open(program)
        .ok()?
        .take(INTERPRETER_LINE_BYTES as u64)
        .read_to_end(&mut head)
        .ok()?;
    let line = head.strip_prefix(b"#!")?;
    let line = &line[..line.iter().position(|byte| *byte == b'\n')?];
    let blank = |byte: &u8| *byte == b' ' || *byte == b'\t';
    let line = &line[line.iter().position(|byte| !blank(byte))?..];
    let end = line.iter().position(blank).unwrap_or(line.len());
    let (interpreter, rest) = line.split_at(end);
    let rest = match (
        rest.iter().position(|byte| !blank(byte)),
        rest.iter().rposition(|byte| !blank(byte)),
    ) {
        (Some(first), Some(last)) => Some(OsStr::from_bytes(&rest[first..=last]).to_owned()),
        _ => None,
    };
    Some((PathBuf::from(OsStr::from_bytes(interpreter)), rest))
}

/// The first missing link of `argv0` started in `cwd`. `path` is the job's PATH when the
/// supervisor knows it, `None` when shell activation sets it inside the sandbox.
pub fn unstartable(argv0: &OsStr, cwd: &Path, path: Option<&OsStr>) -> Option<Unstartable> {
    let program = if argv0.as_bytes().contains(&b'/') {
        match inspect(cwd.join(argv0)) {
            Found::Present(program) => program,
            Found::Absent => {
                return Some(Unstartable::Missing {
                    program: cwd.join(argv0),
                });
            }
            Found::Unknown => return None,
        }
    } else {
        match on_path(argv0, path?, cwd) {
            Found::Present(program) => program,
            Found::Absent => {
                return Some(Unstartable::NotOnPath {
                    name: argv0.to_owned(),
                    path: path?.to_owned(),
                });
            }
            Found::Unknown => return None,
        }
    };
    let (interpreter, argument) = interpreter_line(&program)?;
    let interpreter = cwd.join(interpreter);
    match inspect(interpreter.clone()) {
        Found::Present(_) => {}
        Found::Absent => {
            return Some(Unstartable::Interpreter {
                program,
                interpreter,
            });
        }
        Found::Unknown => return None,
    }
    if interpreter.file_name() != Some(OsStr::new("env")) {
        return None;
    }
    // `env NAME`: options (`-S`, `-i`) and a NAME that is a path are not judged.
    let argument = argument?;
    let name = argument
        .as_bytes()
        .split(|byte| *byte == b' ' || *byte == b'\t')
        .next()
        .filter(|name| !name.is_empty() && name[0] != b'-' && !name.contains(&b'/'))?;
    let name = OsStr::from_bytes(name);
    let path = path?;
    match on_path(name, path, cwd) {
        Found::Absent => Some(Unstartable::EnvTarget {
            program,
            name: name.to_owned(),
            path: path.to_owned(),
        }),
        Found::Present(_) | Found::Unknown => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn script(directory: &Path, name: &str, text: &str, mode: u32) -> PathBuf {
        let path = directory.join(name);
        fs::write(&path, text).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    fn scratch(test: &str) -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            &format!("cowshed-command-start-{test}"),
        )
    }

    /// The case that cost the fleet an hour: `sandbox-exec` named the script for a missing
    /// interpreter. Here the interpreter is named.
    #[test]
    fn an_existing_script_whose_interpreter_is_absent_names_the_interpreter() {
        let root = scratch("interpreter");
        let program = script(
            &root,
            "run",
            "#!/nonexistent/cowshed-shell -e\nexit 0\n",
            0o755,
        );
        assert_eq!(
            unstartable(OsStr::new("./run"), &root, None),
            Some(Unstartable::Interpreter {
                program,
                interpreter: PathBuf::from("/nonexistent/cowshed-shell"),
            })
        );
    }

    #[test]
    fn an_absent_path_names_the_program() {
        let root = scratch("missing");
        assert_eq!(
            unstartable(OsStr::new("tooling/codegraph"), &root, None),
            Some(Unstartable::Missing {
                program: root.join("tooling/codegraph"),
            })
        );
    }

    #[test]
    fn an_env_name_is_judged_only_on_a_known_path() {
        let root = scratch("env-name");
        let bin = root.join("bin");
        fs::create_dir(&bin).unwrap();
        let program = script(&root, "run", "#!/usr/bin/env cowshed-no-such-tool\n", 0o755);
        let path = bin.clone().into_os_string();
        assert_eq!(
            unstartable(OsStr::new("./run"), &root, Some(&path)),
            Some(Unstartable::EnvTarget {
                program,
                name: OsString::from("cowshed-no-such-tool"),
                path: path.clone(),
            })
        );
        // Shell activation sets PATH inside the sandbox: unknown, so not judged.
        assert_eq!(unstartable(OsStr::new("./run"), &root, None), None);
        // The name on that PATH: the command starts.
        script(&bin, "cowshed-no-such-tool", "#!/bin/sh\n", 0o755);
        assert_eq!(unstartable(OsStr::new("./run"), &root, Some(&path)), None);
    }

    #[test]
    fn a_bare_name_is_looked_up_on_the_path_the_job_gets() {
        let root = scratch("bare-name");
        let path = OsString::from("/nonexistent-a:/nonexistent-b");
        assert_eq!(
            unstartable(OsStr::new("cowshed-no-such-tool"), &root, Some(&path)),
            Some(Unstartable::NotOnPath {
                name: OsString::from("cowshed-no-such-tool"),
                path: path.clone(),
            })
        );
        assert_eq!(
            unstartable(OsStr::new("cowshed-no-such-tool"), &root, None),
            None
        );
        assert_eq!(
            unstartable(OsStr::new("sh"), &root, Some(OsStr::new("/bin"))),
            None
        );
    }

    /// A script the supervisor cannot read is the kernel's to judge inside the sandbox, and an
    /// `env` with options is not parsed: neither refuses.
    #[test]
    fn what_cannot_be_inspected_is_not_judged() {
        let root = scratch("uninspectable");
        script(&root, "sealed", "#!/nonexistent/cowshed-shell\n", 0o111);
        assert_eq!(unstartable(OsStr::new("./sealed"), &root, None), None);
        script(
            &root,
            "split",
            "#!/usr/bin/env -S cowshed-no-such-tool --flag\n",
            0o755,
        );
        assert_eq!(
            unstartable(OsStr::new("./split"), &root, Some(OsStr::new("/bin"))),
            None
        );
    }

    #[test]
    fn a_binary_and_a_script_whose_interpreter_exists_start() {
        let root = scratch("starts");
        script(&root, "run", "#!/bin/sh\nexit 0\n", 0o755);
        assert_eq!(unstartable(OsStr::new("./run"), &root, None), None);
        assert_eq!(unstartable(OsStr::new("/bin/sh"), &root, None), None);
    }
}
