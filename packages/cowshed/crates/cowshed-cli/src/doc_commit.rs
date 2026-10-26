//! `cowshed doc-commit (-m <msg> | -F <file>) (--patch <file> | --icm <program>)`
//! (02_workspaces.md "`cowshed doc-commit`"): documentation reaches main without a shed, a gate
//! or a land's wait, and main's tree is never dirty outside the ref turn. The edit is the input:
//! in one hold of the repository's ref turn it is applied, its paths judged by
//! [`cowshed_core::declared_land::admit`], checked and committed, or put back.
//!
//! Host-only like `land-turn`: no daemon and no gateway. Main is the checkout the caller names,
//! and a doc edit needs only main's tree and its ref turn; resolving main through the gateway held
//! every doc commit for as long as the gateway hung (20 to 60+ minutes on 2026-10-22).

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io::{Read as _, Write};
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use cowshed_core::api::dto::{DocCommitReport, GitOid};
use cowshed_core::declared_land::{self as declared, DocDeclaration, DocRefusal, StatusEntry};
use cowshed_core::{CowshedError, Result};

use crate::args::{DocCommitArgs, DocEdit, DocMessage};
use crate::output::Output;
use crate::runtime::output_error;

/// Run one doc commit and write its report in the caller's format; a refusal is the caller's to
/// report. `project` is `--project`, main's checkout; else the working directory's.
pub(crate) async fn dispatch<W, E>(
    args: &DocCommitArgs,
    project: Option<&Path>,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    W: Write + Send,
    E: Write + Send,
{
    let report = commit(args, project, output).await?;
    if json {
        output.success(report).map_err(output_error)?;
    } else {
        output
            .bare_line(report.commit.as_str().as_bytes())
            .map_err(output_error)?;
    }
    Ok(0)
}

async fn commit<W, E>(
    args: &DocCommitArgs,
    project: Option<&Path>,
    output: &mut Output<W, E>,
) -> Result<DocCommitReport>
where
    W: Write + Send,
    E: Write + Send,
{
    let start = match project {
        Some(directory) => directory.to_owned(),
        None => std::env::current_dir().map_err(|error| {
            CowshedError::environment_missing(
                format!("could not determine the current directory: {error}"),
                "use --project <main checkout>",
            )
        })?,
    };
    let main = main_checkout(&start)?;
    let config = cowshed_core::storage::bootstrap::main_cowshed_config(&main)?;
    let (Some(land), Some(doc)) = (config.land(), config.doc()) else {
        return Err(CowshedError::environment_missing(
            format!(
                "{} declares no doc lane: cowshed doc-commit needs [land] and [doc] in main's .cowshed.toml",
                main.join(".cowshed.toml").display()
            ),
            "declare [land] turn and [doc] paths (06_cli.md \"Configuration\"), or commit in a shed and land it",
        ));
    };
    // Both inputs are read before the turn is asked for: the hold never waits on a caller.
    let message = read_input(match &args.message {
        DocMessage::Text(text) => Input::Text(text),
        DocMessage::File(path) => Input::File(path),
    })?;
    let edit = match &args.edit {
        DocEdit::Patch(path) => Edit::Patch(read_input(Input::File(path))?),
        DocEdit::Icm(path) => Edit::Icm(read_input(Input::File(path))?),
    };
    let environment = crate::declared_land::pure_environment()?;
    let icm = match &edit {
        Edit::Icm(_) => Some(icm_program()?),
        Edit::Patch(_) => None,
    };
    let state = Scratch::new()?;
    std::fs::write(state.path("message"), &message)
        .map_err(|error| state.failed("write the commit message", &error))?;
    let root =
        cowshed_core::land_turn::land_turn_root(&cowshed_core::host_dirs::support_directory(
            &cowshed_core::gateway_sessions::canonical_home()?,
        ));
    let turn = cowshed_core::runtime::ref_turn::RefTurn::acquire_in(
        &root,
        land.ref_turn(),
        cowshed_core::runtime::ref_turn::RefWriter::DocCommit,
    )
    .await?;
    let hold = Hold {
        main: &main,
        environment: &environment,
        descriptor: turn.descriptor(),
        state: &state,
    };
    let held = hold.run(&edit, icm.as_deref(), doc, output);
    match &held {
        Ok(report) => turn.end(format!("committed {}", report.commit.as_str())),
        Err(error) => turn.end(format!("refused: {}", error.message)),
    }
    held
}

/// A file argument or stdin (`-`).
enum Input<'a> {
    Text(&'a str),
    File(&'a Path),
}

fn read_input(input: Input<'_>) -> Result<Vec<u8>> {
    match input {
        Input::Text(text) => {
            let mut bytes = text.as_bytes().to_vec();
            if !bytes.ends_with(b"\n") {
                bytes.push(b'\n');
            }
            Ok(bytes)
        }
        Input::File(path) if path == Path::new("-") => {
            let mut bytes = Vec::new();
            std::io::stdin()
                .read_to_end(&mut bytes)
                .map_err(|error| CowshedError::internal(format!("cannot read stdin: {error}")))?;
            Ok(bytes)
        }
        Input::File(path) => std::fs::read(path).map_err(|error| {
            CowshedError::not_found(
                format!("cannot read {}: {error}", path.display()),
                "name a readable file, or - for stdin",
            )
        }),
    }
}

enum Edit {
    Patch(Vec<u8>),
    Icm(Vec<u8>),
}

/// The `icm` the caller would run, on the caller's `PATH`: an ICM program is that tool's input.
fn icm_program() -> Result<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .map(|directory| directory.join("icm"))
        .find(|candidate| {
            use std::os::unix::fs::PermissionsExt as _;
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
        .ok_or_else(|| {
            CowshedError::environment_missing(
                "cowshed doc-commit --icm runs the icm on your PATH, and there is none",
                "put icm on PATH, or pass the edit as a patch with --patch",
            )
        })
}

/// Main's checkout: the git top level at `start`, when cowshed's in-checkout marker gives it no
/// role other than main. A doc edit in a shed is refused: commit in the shed and land it.
fn main_checkout(start: &Path) -> Result<PathBuf> {
    let top = git(start, &["rev-parse", "--show-toplevel"], &[], None).map_err(|error| {
        CowshedError::usage(
            format!(
                "{} is not in a checkout: {}",
                start.display(),
                error.message
            ),
            "run cowshed doc-commit in main's checkout, or name it with --project",
        )
    })?;
    let main = PathBuf::from(String::from_utf8_lossy(&top).trim());
    let marker = main.join(cowshed_core::storage::WORKSPACE_MARKER_PATH);
    match cowshed_core::metadata::WorkspaceMarker::read_from(&marker) {
        Err(cowshed_core::metadata::MetadataError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(main)
        }
        Err(error) => Err(CowshedError::integrity(
            format!(
                "cannot read the cowshed marker {}: {error}",
                marker.display()
            ),
            "refusing to guess whether this checkout is main",
        )),
        Ok(read) if read.role == cowshed_core::metadata::WorkspaceRole::Main => Ok(main),
        Ok(read) => Err(CowshedError::conflict(
            format!(
                "{} is cowshed workspace {}, not main: a doc commit edits main",
                main.display(),
                read.workspace
            ),
            "commit in the workspace yourself and land it with cowshed land",
        )),
    }
}

/// A private directory for the hold's scratch index and inputs, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let base = std::env::temp_dir().join(format!(
            "cowshed-doc-commit-{}-{}",
            std::process::id(),
            declared::unix_ms_now()
        ));
        std::fs::create_dir(&base).map_err(|error| {
            CowshedError::internal(format!("cannot create {}: {error}", base.display()))
        })?;
        Ok(Self(base))
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn failed(&self, what: &str, error: &std::io::Error) -> CowshedError {
        CowshedError::internal(format!("cannot {what} in {}: {error}", self.0.display()))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            cowshed_os::eprintln!(
                "cowshed: cannot remove the doc commit's scratch directory {}: {error}",
                self.0.display()
            );
        }
    }
}

/// Main as the hold found it before the edit: its head, its status, and the bytes of every path
/// it held uncommitted (kept only for an ICM edit, whose paths are known only once it ran).
struct Basis {
    head: GitOid,
    before: Vec<StatusEntry>,
    kept: Vec<Kept>,
}

/// A path main held uncommitted before the edit: its directory entry and its index entries.
struct Kept {
    path: String,
    entry: Entry,
    index: Vec<u8>,
}

/// A held path's directory entry as the edit found it: never the file a symlink names, so putting
/// it back restores the link itself, and a held deletion is put back absent.
#[derive(Debug, Eq, PartialEq)]
enum Entry {
    Absent,
    File {
        bytes: Vec<u8>,
        mode: u32,
    },
    Link(PathBuf),
    /// A directory (a submodule's checkout): no doc edit writes one, and none is put back.
    Directory,
}

impl Entry {
    fn read(path: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::PermissionsExt as _;
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Self::Absent),
            Err(error) => return Err(error),
        };
        let kind = metadata.file_type();
        if kind.is_symlink() {
            Ok(Self::Link(std::fs::read_link(path)?))
        } else if kind.is_dir() {
            Ok(Self::Directory)
        } else {
            Ok(Self::File {
                bytes: std::fs::read(path)?,
                mode: metadata.permissions().mode(),
            })
        }
    }

    /// Make `path` this entry again, replacing whatever file or link the edit left there.
    fn restore(&self, path: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let unlink = || match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => Err(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!("the edit left a directory at {}", path.display()),
            )),
            Ok(_) => std::fs::remove_file(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        };
        match self {
            Self::Absent => unlink(),
            Self::File { bytes, mode } => {
                unlink()?;
                std::fs::write(path, bytes)?;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(*mode))
            }
            Self::Link(target) => {
                unlink()?;
                std::os::unix::fs::symlink(target, path)
            }
            Self::Directory => Err(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!(
                    "{} was a directory, which a doc commit cannot put back",
                    path.display()
                ),
            )),
        }
    }
}

/// Everything the ref-turn hold runs with.
struct Hold<'a> {
    main: &'a Path,
    environment: &'a [(OsString, OsString)],
    descriptor: RawFd,
    state: &'a Scratch,
}

impl Hold<'_> {
    fn run<W: Write + Send, E: Write + Send>(
        &self,
        edit: &Edit,
        icm: Option<&Path>,
        doc: &DocDeclaration,
        output: &mut Output<W, E>,
    ) -> Result<DocCommitReport> {
        let head = self.head()?;
        let before = self.status()?;
        let held_before = declared::paths(&before);
        match edit {
            // A patch is judged by git's account before main's tree is touched.
            Edit::Patch(patch) => {
                let paths = declared::admit(self.patch_paths(patch)?, &held_before, doc)
                    .map_err(|refusal| refused(self.main, &refusal))?;
                self.apply_and_commit(
                    &Basis {
                        head,
                        before,
                        kept: Vec::new(),
                    },
                    &paths,
                    edit,
                    icm,
                    doc,
                    output,
                )
            }
            // A program is judged by what it wrote; every held path's bytes are kept first.
            Edit::Icm(_) => {
                let kept = self.keep(&held_before)?;
                self.apply_and_commit(&Basis { head, before, kept }, &[], edit, icm, doc, output)
            }
        }
    }

    fn apply_and_commit<W: Write + Send, E: Write + Send>(
        &self,
        basis: &Basis,
        judged: &[String],
        edit: &Edit,
        icm: Option<&Path>,
        doc: &DocDeclaration,
        output: &mut Output<W, E>,
    ) -> Result<DocCommitReport> {
        let Basis { head, before, kept } = basis;
        let mut touched: BTreeSet<String> = judged.iter().cloned().collect();
        let committed = (|| {
            let paths = match (edit, icm) {
                (Edit::Patch(patch), _) => {
                    self.git(&["apply", "-"], Some(patch))?;
                    judged.to_vec()
                }
                (Edit::Icm(program), Some(icm)) => {
                    std::fs::write(self.state.path("program.ts"), program)
                        .map_err(|error| self.state.failed("write the ICM program", &error))?;
                    let ran = self.declared(
                        &[
                            icm.as_os_str().to_owned(),
                            OsString::from("run"),
                            self.state.path("program.ts").into_os_string(),
                            OsString::from("--root"),
                            self.main.as_os_str().to_owned(),
                            OsString::from("--json"),
                        ],
                        None,
                    );
                    let written = self.written(before, kept)?;
                    touched.extend(written.iter().cloned());
                    let (status, stdout) = ran?;
                    if !status.success() {
                        return Err(CowshedError::conflict(
                            format!(
                                "icm {} for the doc commit: {}",
                                crate::declared_land::describe(status),
                                String::from_utf8_lossy(&stdout).trim()
                            ),
                            "repair the program by its digest, then run cowshed doc-commit again",
                        ));
                    }
                    let held: BTreeSet<String> =
                        kept.iter().map(|kept| kept.path.clone()).collect();
                    declared::admit(written, &held, doc)
                        .map_err(|refusal| refused(self.main, &refusal))?
                }
                (Edit::Icm(_), None) => {
                    return Err(CowshedError::internal(
                        "an ICM edit reached its hold without icm",
                    ));
                }
            };
            if let Some(check) = &doc.check {
                let mut argv: Vec<OsString> = check.iter().map(OsString::from).collect();
                argv.extend(paths.iter().map(OsString::from));
                let (status, stdout) = self.declared(&argv, None)?;
                let after_check = self.written(before, kept)?;
                touched.extend(after_check.iter().cloned());
                if !status.success() {
                    return Err(CowshedError::conflict(
                        format!(
                            "the declared doc check `{}` {}: {}",
                            check.join(" "),
                            crate::declared_land::describe(status),
                            String::from_utf8_lossy(&stdout).trim()
                        ),
                        "fix the edit so the doc check passes, then run cowshed doc-commit again",
                    ));
                }
                let strays: Vec<String> = after_check
                    .iter()
                    .filter(|path| !paths.contains(path))
                    .cloned()
                    .collect();
                if !strays.is_empty() {
                    return Err(CowshedError::conflict(
                        format!(
                            "the declared doc check `{}` wrote paths outside the edit: {}",
                            check.join(" "),
                            strays.join(", ")
                        ),
                        "a doc check may rewrite only the paths it is given",
                    ));
                }
            }
            let mut add: Vec<&str> = vec!["--literal-pathspecs", "add", "-A", "--"];
            add.extend(paths.iter().map(String::as_str));
            self.git(&add, None)?;
            let message = self.state.path("message");
            let mut argv: Vec<OsString> = ["git", "--literal-pathspecs", "commit", "--only", "-F"]
                .map(OsString::from)
                .to_vec();
            argv.push(message.into_os_string());
            argv.push(OsString::from("--"));
            argv.extend(paths.iter().map(OsString::from));
            let (status, stdout) = self.declared(&argv, None)?;
            if !status.success() {
                return Err(CowshedError::conflict(
                    format!(
                        "git commit {}: {}",
                        crate::declared_land::describe(status),
                        String::from_utf8_lossy(&stdout).trim()
                    ),
                    "the repository's commit hooks refused the edit; fix it and run cowshed doc-commit again",
                ));
            }
            Ok(paths)
        })();
        match committed {
            Ok(paths) => Ok(DocCommitReport {
                commit: self.head()?,
                paths,
            }),
            Err(error) => {
                let now = self.head()?;
                if &now != head {
                    output
                        .warning(&format!(
                            "the edit committed as {} before the doc commit stopped; nothing to put back",
                            now.as_str()
                        ))
                        .map_err(output_error)?;
                    return Err(error);
                }
                self.put_back(&touched, kept, output)?;
                Err(error)
            }
        }
    }

    /// The paths a patch writes, by git's own account: applied to a scratch index of HEAD, never
    /// to main, with both sides of a rename. A patch that does not apply is refused here.
    fn patch_paths(&self, patch: &[u8]) -> Result<BTreeSet<String>> {
        let index = self.state.path("index");
        let scratch = [(OsString::from("GIT_INDEX_FILE"), index.into_os_string())];
        self.git_with(&["read-tree", "HEAD"], None, &scratch)?;
        self.git_with(&["apply", "--cached", "-"], Some(patch), &scratch)
            .map_err(|error| {
                CowshedError::conflict(
                    format!("the patch does not apply to main's HEAD: {}", error.message),
                    "make the patch against main's current HEAD, then run cowshed doc-commit again",
                )
            })?;
        let names = self.git_with(
            &[
                "diff",
                "--cached",
                "--no-renames",
                "--name-only",
                "-z",
                "HEAD",
            ],
            None,
            &scratch,
        )?;
        names
            .split(|&byte| byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| {
                String::from_utf8(name.to_vec()).map_err(|_| {
                    CowshedError::conflict(
                        "the patch writes a path that is not UTF-8, which a doc commit cannot judge",
                        "commit it in a shed and land it",
                    )
                })
            })
            .collect()
    }

    /// The directory entry and the index entries (`git ls-files -s -z`) of each path main held
    /// uncommitted, kept so an edit that writes one is seen and that path put back exactly.
    fn keep(&self, held: &BTreeSet<String>) -> Result<Vec<Kept>> {
        held.iter()
            .map(|path| {
                let file = self.main.join(path);
                let entry = Entry::read(&file).map_err(|error| {
                    CowshedError::internal(format!(
                        "cannot keep a copy of {} before the edit: {error}",
                        file.display()
                    ))
                })?;
                let index = self.git(
                    &["--literal-pathspecs", "ls-files", "-s", "-z", "--", path],
                    None,
                )?;
                Ok(Kept {
                    path: path.clone(),
                    entry,
                    index,
                })
            })
            .collect()
    }

    /// What main's tree differs in from the read before the edit.
    fn written(&self, before: &[StatusEntry], kept: &[Kept]) -> Result<BTreeSet<String>> {
        let after = self.status()?;
        let rewritten = kept
            .iter()
            .filter(|kept| {
                Entry::read(&self.main.join(&kept.path)).ok().as_ref() != Some(&kept.entry)
            })
            .map(|kept| kept.path.clone())
            .collect::<Vec<_>>();
        Ok(declared::written(before, &after, rewritten))
    }

    /// Put `paths` back as HEAD has them, and each kept path back as it was; every path is
    /// attempted and each one that fails is said.
    fn put_back<W: Write + Send, E: Write + Send>(
        &self,
        paths: &BTreeSet<String>,
        kept: &[Kept],
        output: &mut Output<W, E>,
    ) -> Result<()> {
        let mut failed = Vec::new();
        for path in paths {
            if let Some(kept) = kept.iter().find(|kept| &kept.path == path) {
                // The index first, then the entry: `--index-info` takes the kept stages back
                // exactly, a conflict's included, after the path's current ones are removed.
                let restored = self
                    .git(
                        &[
                            "--literal-pathspecs",
                            "update-index",
                            "--force-remove",
                            "--",
                            path,
                        ],
                        None,
                    )
                    .and_then(|_| {
                        if kept.index.is_empty() {
                            Ok(Vec::new())
                        } else {
                            self.git(&["update-index", "-z", "--index-info"], Some(&kept.index))
                        }
                    })
                    .is_ok()
                    && kept.entry.restore(&self.main.join(path)).is_ok();
                if !restored {
                    failed.push(path.clone());
                }
                continue;
            }
            let tracked = self
                .git(
                    &[
                        "--literal-pathspecs",
                        "ls-tree",
                        "--name-only",
                        "HEAD",
                        "--",
                        path,
                    ],
                    None,
                )
                .map(|listed| !listed.is_empty());
            let tracked = match tracked {
                Ok(tracked) => tracked,
                // Not knowing whether HEAD has it, neither restore nor removal is safe: said.
                Err(_) => {
                    failed.push(path.clone());
                    continue;
                }
            };
            let restored = if tracked {
                self.git(
                    &[
                        "--literal-pathspecs",
                        "restore",
                        "--source=HEAD",
                        "--staged",
                        "--worktree",
                        "--",
                        path,
                    ],
                    None,
                )
                .is_ok()
            } else {
                self.git(
                    &[
                        "--literal-pathspecs",
                        "rm",
                        "-q",
                        "--cached",
                        "--ignore-unmatch",
                        "--",
                        path,
                    ],
                    None,
                )
                .is_ok()
                    && match std::fs::remove_file(self.main.join(path)) {
                        Ok(()) => true,
                        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
                    }
            };
            if !restored {
                failed.push(path.clone());
            }
        }
        if !failed.is_empty() {
            return Err(CowshedError::conflict(
                format!(
                    "the edit did not commit, and main's tree still differs from HEAD at: {}",
                    failed.join(", ")
                ),
                "put those paths back by hand: git -C <main> restore --source=HEAD --staged --worktree -- <path>",
            ));
        }
        let shown: Vec<&str> = paths.iter().map(String::as_str).collect();
        output
            .guidance(&format!(
                "doc-commit: the edit did not commit; main's tree is back as it was at {}",
                if shown.is_empty() {
                    "every path".to_owned()
                } else {
                    shown.join(" ")
                }
            ))
            .map_err(output_error)
    }

    fn head(&self) -> Result<GitOid> {
        crate::declared_land::main_head(self.main)
    }

    fn status(&self) -> Result<Vec<StatusEntry>> {
        let bytes = self.git(
            &[
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--no-renames",
            ],
            None,
        )?;
        declared::parse_status(&bytes).map_err(CowshedError::internal)
    }

    fn git(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<Vec<u8>> {
        self.git_with(args, stdin, &[])
    }

    /// Plain git in main, in the pure environment, holding the turn's descriptor.
    fn git_with(
        &self,
        args: &[&str],
        stdin: Option<&[u8]>,
        extra: &[(OsString, OsString)],
    ) -> Result<Vec<u8>> {
        git(
            self.main,
            args,
            self.environment,
            Some(Held {
                descriptor: self.descriptor,
                stdin,
                extra,
            }),
        )
    }

    /// A command in main's pure environment (`direnv exec` when main has an `.envrc`), holding
    /// the turn's descriptor; answers its status and its stdout and stderr together.
    fn declared(
        &self,
        argv: &[OsString],
        stdin: Option<&[u8]>,
    ) -> Result<(std::process::ExitStatus, Vec<u8>)> {
        let mut command =
            crate::declared_land::declared_command(self.main, argv, self.environment)?;
        cowshed_core::held_lock::hand_to(&mut command, vec![self.descriptor]);
        run(&mut command, stdin, true)
    }
}

/// What a git run under the ref turn carries: the turn's descriptor, its stdin and the variables
/// it adds to the pure environment.
struct Held<'a> {
    descriptor: RawFd,
    stdin: Option<&'a [u8]>,
    extra: &'a [(OsString, OsString)],
}
/// Run `git -C <dir> <args>`: in `environment` (the caller's when empty) and, when `held` is
/// given, holding its descriptor with its stdin and extra variables. A nonzero exit is an error
/// naming git's stderr.
fn git(
    directory: &Path,
    args: &[&str],
    environment: &[(OsString, OsString)],
    held: Option<Held<'_>>,
) -> Result<Vec<u8>> {
    let mut command = std::process::Command::new("git");
    command.arg("-C").arg(directory).args(args);
    if !environment.is_empty() {
        command
            .env_clear()
            .envs(environment.iter().map(|(name, value)| (name, value)));
        let policy = cowshed_core::held_lock::held_command_environment(None)
            .map_err(CowshedError::internal)?;
        command.envs(policy.iter().map(|(name, value)| (&**name, &**value)));
    }
    let stdin = match held {
        Some(Held {
            descriptor,
            stdin,
            extra,
        }) => {
            cowshed_core::held_lock::hand_to(&mut command, vec![descriptor]);
            command.envs(extra.iter().map(|(name, value)| (name, value)));
            stdin
        }
        None => None,
    };
    let (status, stdout, stderr) = run_split(&mut command, stdin)?;
    if status.success() {
        return Ok(stdout);
    }
    Err(CowshedError::conflict(
        format!(
            "git {} {}: {}",
            args.join(" "),
            crate::declared_land::describe(status),
            String::from_utf8_lossy(&stderr).trim()
        ),
        "see git's message",
    ))
}

fn run(
    command: &mut std::process::Command,
    stdin: Option<&[u8]>,
    merge: bool,
) -> Result<(std::process::ExitStatus, Vec<u8>)> {
    let (status, mut stdout, stderr) = run_split(command, stdin)?;
    if merge {
        stdout.extend_from_slice(&stderr);
    }
    Ok((status, stdout))
}

fn run_split(
    command: &mut std::process::Command,
    stdin: Option<&[u8]>,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>)> {
    let program = command.get_program().to_string_lossy().into_owned();
    command
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot run {program}: {error}"),
            "install it, or put it on the bootstrap PATH",
        )
    })?;
    if let Some(bytes) = stdin
        && let Some(mut pipe) = child.stdin.take()
    {
        pipe.write_all(bytes).map_err(|error| {
            CowshedError::internal(format!("cannot write to {program}: {error}"))
        })?;
    }
    // WHY unbounded: the command is this doc commit's own work under its ref turn; it ends with the
    // command, which holds the turn's descriptor.
    #[allow(clippy::disallowed_methods)]
    let finished = child
        .wait_with_output()
        .map_err(|error| CowshedError::internal(format!("cannot wait for {program}: {error}")))?;
    Ok((finished.status, finished.stdout, finished.stderr))
}

fn refused(main: &Path, refusal: &DocRefusal) -> CowshedError {
    match refusal {
        DocRefusal::Empty => CowshedError::conflict(
            "the edit writes nothing, so there is nothing to commit",
            "pass an edit that changes documentation",
        ),
        DocRefusal::NotDoc(paths) => CowshedError::conflict(
            format!(
                "the edit writes paths that are not documentation by {}'s [doc] paths: {}",
                main.join(".cowshed.toml").display(),
                paths.join(", ")
            ),
            "commit code in a shed and land it with cowshed land <ws>",
        ),
        DocRefusal::HeldUncommitted(paths) => CowshedError::conflict(
            format!(
                "main's tree holds uncommitted edits at {}: committing them would commit someone else's edit",
                paths.join(", ")
            ),
            "wait for that edit's owner to commit or put it back, then run cowshed doc-commit again",
        ),
    }
}
