//! `cowshed doc-commit (-m <msg> | -F <file>) (--patch <file> | --icm <program>)`
//! (02_workspaces.md "`cowshed doc-commit`"): documentation reaches main without a shed, a gate
//! or a land's wait, and main's tree is never dirty outside the ref turn. The edit is the input:
//! in one hold of the repository's ref turn it is applied, its paths judged by
//! [`cowshed_core::declared_land::admit`], checked and committed, or put back.
//!
//! Host-only like `land-turn`: no daemon, and of the gateway only the ref turn, which holds every
//! command the hold runs. Main is the checkout the caller names, and a doc edit needs only main's
//! tree and its ref turn; resolving main through the gateway held every doc commit for as long as
//! the gateway hung (20 to 60+ minutes on 2026-10-22).

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::{Read as _, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use cowshed_core::api::dto::{DocCommitReport, GitOid};
use cowshed_core::declared_land::{self as declared, DocDeclaration, DocRefusal, StatusEntry};
use cowshed_core::runtime::ref_turn::{RefTurn, RefWriter};
use cowshed_core::storage::bootstrap::CanonicalRoots;
use cowshed_core::{CowshedError, Result};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

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
    let main = main_checkout(&start).await?;
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
    let socket = args
        .gateway_socket
        .clone()
        .unwrap_or_else(|| CanonicalRoots::global().control_socket());
    let mut turn = RefTurn::acquire_in(&socket, land.ref_turn(), RefWriter::DocCommit).await?;
    let mut hold = Hold {
        main: &main,
        environment: &environment,
        turn: &mut turn,
        state: &state,
    };
    let held = hold.run(&edit, icm.as_deref(), doc, output).await;
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
async fn main_checkout(start: &Path) -> Result<PathBuf> {
    let top = git(start, &["rev-parse", "--show-toplevel"], &[], None)
        .await
        .map_err(|error| {
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
    kept: BTreeMap<String, Kept>,
}

/// A path main held uncommitted before the edit: its directory entry and its index entries.
struct Kept {
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

/// The paths a rollback puts back, split by how each is restored: a held path from its kept
/// entry and index stages, another from HEAD when HEAD has it, and a path HEAD does not have
/// by removing it.
#[derive(Debug, Eq, PartialEq)]
struct Rollback {
    held: Vec<String>,
    tracked: Vec<String>,
    absent: Vec<String>,
}

/// Group one ls-files -s -z read by held path, each record byte for byte as git printed it,
/// terminator included, so the kept mode, hash and every conflict stage replay through
/// --index-info unchanged. The path starts after the first tab: a path may itself hold tabs,
/// newlines or glob spellings. Only held paths are matched, by their bytes, so an unrelated
/// record git lists -- its path not UTF-8, or holding no tab this reader knows -- is passed
/// over, never a new refusal, and no hash spelling is guessed at.
fn index_records_by_path(listed: &[u8], held: &BTreeSet<String>) -> BTreeMap<String, Vec<u8>> {
    let by_bytes: BTreeMap<&[u8], &String> =
        held.iter().map(|path| (path.as_bytes(), path)).collect();
    let mut grouped: BTreeMap<String, Vec<u8>> =
        held.iter().map(|path| (path.clone(), Vec::new())).collect();
    for record in listed.split(|&byte| byte == 0) {
        let Some(tab) = record.iter().position(|&byte| byte == b'\t') else {
            continue;
        };
        let Some(path) = by_bytes.get(&record[tab + 1..]) else {
            continue;
        };
        if let Some(records) = grouped.get_mut(*path) {
            records.extend_from_slice(record);
            records.push(0);
        }
    }
    grouped
}

/// Split the paths a failed hold wrote by how each is put back, against one ls-tree -z read
/// of HEAD: both sides of a rename land on their own side -- the side HEAD has tracked, the
/// side it lacks absent -- and names HEAD lists that are not UTF-8 are matched by bytes and
/// otherwise passed over.
fn partition_rollback(
    paths: &BTreeSet<String>,
    kept: &BTreeMap<String, Kept>,
    head: &[u8],
) -> Rollback {
    let in_head: BTreeSet<&[u8]> = head
        .split(|&byte| byte == 0)
        .filter(|name| !name.is_empty())
        .collect();
    let mut rollback = Rollback {
        held: Vec::new(),
        tracked: Vec::new(),
        absent: Vec::new(),
    };
    for path in paths {
        if kept.contains_key(path) {
            rollback.held.push(path.clone());
        } else if in_head.contains(path.as_bytes()) {
            rollback.tracked.push(path.clone());
        } else {
            rollback.absent.push(path.clone());
        }
    }
    rollback
}

/// Paths as one NUL-terminated stdin list, for the git reads and writes that take theirs so:
/// no path travels on a command line, whatever it spells or however many there are.
fn nul_paths(paths: &[String]) -> Vec<u8> {
    let mut listed = Vec::new();
    for path in paths {
        listed.extend_from_slice(path.as_bytes());
        listed.push(0);
    }
    listed
}

/// Everything the ref-turn hold runs with: every command it runs is bound to the turn.
struct Hold<'a> {
    main: &'a Path,
    environment: &'a [(OsString, OsString)],
    turn: &'a mut RefTurn,
    state: &'a Scratch,
}

impl Hold<'_> {
    async fn run<W: Write + Send, E: Write + Send>(
        &mut self,
        edit: &Edit,
        icm: Option<&Path>,
        doc: &DocDeclaration,
        output: &mut Output<W, E>,
    ) -> Result<DocCommitReport> {
        let head = self.head()?;
        let before = self.status().await?;
        let held_before = declared::paths(&before);
        match edit {
            // A patch is judged by git's account before main's tree is touched.
            Edit::Patch(patch) => {
                let paths = declared::admit(self.patch_paths(patch).await?, &held_before, doc)
                    .map_err(|refusal| refused(self.main, &refusal))?;
                self.apply_and_commit(
                    &Basis {
                        head,
                        before,
                        kept: BTreeMap::new(),
                    },
                    &paths,
                    edit,
                    icm,
                    doc,
                    output,
                )
                .await
            }
            // A program is judged by what it wrote; every held path's bytes are kept first.
            Edit::Icm(_) => {
                let kept = self.keep(&held_before).await?;
                self.apply_and_commit(&Basis { head, before, kept }, &[], edit, icm, doc, output)
                    .await
            }
        }
    }

    async fn apply_and_commit<W: Write + Send, E: Write + Send>(
        &mut self,
        basis: &Basis,
        judged: &[String],
        edit: &Edit,
        icm: Option<&Path>,
        doc: &DocDeclaration,
        output: &mut Output<W, E>,
    ) -> Result<DocCommitReport> {
        let Basis { head, before, kept } = basis;
        let mut touched: BTreeSet<String> = judged.iter().cloned().collect();
        let committed: Result<Vec<String>> = async {
            let paths = match (edit, icm) {
                (Edit::Patch(patch), _) => {
                    self.git(&["apply", "-"], Some(patch)).await?;
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
                        )
                        .await;
                    let written = self.written(before, kept).await?;
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
                    let held: BTreeSet<String> = kept.keys().cloned().collect();
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
                let (status, stdout) = self.declared(&argv, None).await?;
                let after_check = self.written(before, kept).await?;
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
            self.git(&add, None).await?;
            let message = self.state.path("message");
            let mut argv: Vec<OsString> = ["git", "--literal-pathspecs", "commit", "--only", "-F"]
                .map(OsString::from)
                .to_vec();
            argv.push(message.into_os_string());
            argv.push(OsString::from("--"));
            argv.extend(paths.iter().map(OsString::from));
            let (status, stdout) = self.declared(&argv, None).await?;
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
        }
        .await;
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
                self.put_back(&touched, kept, output).await?;
                Err(error)
            }
        }
    }

    /// The paths a patch writes, by git's own account: applied to a scratch index of HEAD, never
    /// to main, with both sides of a rename. A patch that does not apply is refused here.
    async fn patch_paths(&mut self, patch: &[u8]) -> Result<BTreeSet<String>> {
        let index = self.state.path("index");
        let scratch = [(OsString::from("GIT_INDEX_FILE"), index.into_os_string())];
        self.git_with(&["read-tree", "HEAD"], None, &scratch)
            .await?;
        self.git_with(&["apply", "--cached", "-"], Some(patch), &scratch)
            .await
            .map_err(|error| {
                CowshedError::conflict(
                    format!("the patch does not apply to main's HEAD: {}", error.message),
                    "make the patch against main's current HEAD, then run cowshed doc-commit again",
                )
            })?;
        let names = self
            .git_with(
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
            )
            .await?;
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

    /// The directory entry and the index entries (git ls-files -s -z) of each path main held
    /// uncommitted, kept so an edit that writes one is seen and that path put back exactly.
    /// The whole index is read once and grouped in memory, however many paths are held; none
    /// held, it is not read at all.
    async fn keep(&mut self, held: &BTreeSet<String>) -> Result<BTreeMap<String, Kept>> {
        if held.is_empty() {
            return Ok(BTreeMap::new());
        }
        let listed = self.git(&["ls-files", "-s", "-z"], None).await?;
        let mut index = index_records_by_path(&listed, held);
        let mut kept = BTreeMap::new();
        for path in held {
            let file = self.main.join(path);
            let entry = Entry::read(&file).map_err(|error| {
                CowshedError::internal(format!(
                    "cannot keep a copy of {} before the edit: {error}",
                    file.display()
                ))
            })?;
            kept.insert(
                path.clone(),
                Kept {
                    entry,
                    index: index.remove(path).unwrap_or_default(),
                },
            );
        }
        Ok(kept)
    }

    /// What main's tree differs in from the read before the edit.
    async fn written(
        &mut self,
        before: &[StatusEntry],
        kept: &BTreeMap<String, Kept>,
    ) -> Result<BTreeSet<String>> {
        let after = self.status().await?;
        let rewritten = kept
            .iter()
            .filter(|(path, held)| {
                Entry::read(&self.main.join(path)).ok().as_ref() != Some(&held.entry)
            })
            .map(|(path, _held)| path.clone())
            .collect::<Vec<_>>();
        Ok(declared::written(before, &after, rewritten))
    }

    /// Put paths back as HEAD has them, and each kept path back as it was. Each partition of
    /// the rollback is one git batch however many paths it holds -- held index entries
    /// removed and replayed, HEAD's paths restored, paths HEAD lacks removed from the
    /// index -- with paths on NUL stdin, never a command line, and no batch runs empty.
    /// A batch answers one exit for its whole partition, never which of its paths git
    /// applied, so a failed batch names its whole attempted partition and skips only that
    /// partition's dependent step (a held entry after its index, an unlink after its index
    /// removal); every independent partition still runs. Filesystem entries and unlinks
    /// stay per path, each failure named on its own.
    async fn put_back<W: Write + Send, E: Write + Send>(
        &mut self,
        paths: &BTreeSet<String>,
        kept: &BTreeMap<String, Kept>,
        output: &mut Output<W, E>,
    ) -> Result<()> {
        let mut failed = Vec::new();
        // HEAD is classified once, before anything is put back, when a path no kept copy
        // covers needs it. A read that fails knows nothing of HEAD, so splitting against
        // an empty one lands every such path on the absent side: those are named and
        // neither restored nor removed, while the held paths are still put back.
        let unheld = paths.iter().any(|path| !kept.contains_key(path));
        let classified = if unheld {
            self.git(&["ls-tree", "-r", "-z", "--name-only", "HEAD"], None)
                .await
                .ok()
        } else {
            Some(Vec::new())
        };
        let mut rollback = partition_rollback(paths, kept, classified.as_deref().unwrap_or(&[]));
        if classified.is_none() {
            failed.append(&mut rollback.absent);
        }

        // The index first, then the entries: one removal of the held paths' current stages,
        // one replay of their kept stages exactly, a conflict's included, then each kept
        // entry back over its file.
        if !rollback.held.is_empty() {
            let removed = self
                .git(
                    &[
                        "--literal-pathspecs",
                        "update-index",
                        "--force-remove",
                        "-z",
                        "--stdin",
                    ],
                    Some(&nul_paths(&rollback.held)),
                )
                .await;
            if removed.is_err() {
                failed.extend(rollback.held.iter().cloned());
            } else {
                let mut replay = Vec::new();
                for path in &rollback.held {
                    if let Some(held) = kept.get(path) {
                        replay.extend_from_slice(&held.index);
                    }
                }
                let indexed = replay.is_empty()
                    || self
                        .git(&["update-index", "-z", "--index-info"], Some(&replay))
                        .await
                        .is_ok();
                for path in &rollback.held {
                    let Some(held) = kept.get(path) else {
                        continue;
                    };
                    // A failed replay names the paths it carried -- the ones a kept index
                    // was replayed for; a held path with none was already put back in the
                    // index by the removal, so its entry still goes back.
                    let restored = (indexed || held.index.is_empty())
                        && held.entry.restore(&self.main.join(path)).is_ok();
                    if !restored {
                        failed.push(path.clone());
                    }
                }
            }
        }

        if !rollback.tracked.is_empty() {
            let restored = self
                .git(
                    &[
                        "--literal-pathspecs",
                        "restore",
                        "--source=HEAD",
                        "--staged",
                        "--worktree",
                        "--pathspec-from-file=-",
                        "--pathspec-file-nul",
                    ],
                    Some(&nul_paths(&rollback.tracked)),
                )
                .await;
            if restored.is_err() {
                failed.extend(rollback.tracked.iter().cloned());
            }
        }

        if !rollback.absent.is_empty() {
            let removed = self
                .git(
                    &[
                        "--literal-pathspecs",
                        "rm",
                        "-q",
                        "--cached",
                        "--ignore-unmatch",
                        "--pathspec-from-file=-",
                        "--pathspec-file-nul",
                    ],
                    Some(&nul_paths(&rollback.absent)),
                )
                .await;
            if removed.is_err() {
                failed.extend(rollback.absent.iter().cloned());
            } else {
                for path in &rollback.absent {
                    match std::fs::remove_file(self.main.join(path)) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => failed.push(path.clone()),
                    }
                }
            }
        }

        if !failed.is_empty() {
            failed.sort();
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

    async fn status(&mut self) -> Result<Vec<StatusEntry>> {
        let bytes = self
            .git(
                &[
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=all",
                    "--no-renames",
                ],
                None,
            )
            .await?;
        declared::parse_status(&bytes).map_err(CowshedError::internal)
    }

    async fn git(&mut self, args: &[&str], stdin: Option<&[u8]>) -> Result<Vec<u8>> {
        self.git_with(args, stdin, &[]).await
    }

    /// Plain git in main, in the pure environment, bound to the ref turn.
    async fn git_with(
        &mut self,
        args: &[&str],
        stdin: Option<&[u8]>,
        extra: &[(OsString, OsString)],
    ) -> Result<Vec<u8>> {
        git(
            self.main,
            args,
            self.environment,
            Some(Held {
                turn: &mut *self.turn,
                stdin,
                extra,
            }),
        )
        .await
    }

    /// A command in main's pure environment (`direnv exec` when main has an `.envrc`), bound to
    /// the ref turn; answers its status and its stdout and stderr together.
    async fn declared(
        &mut self,
        argv: &[OsString],
        stdin: Option<&[u8]>,
    ) -> Result<(std::process::ExitStatus, Vec<u8>)> {
        let command = crate::declared_land::declared_command(self.main, argv, self.environment)?;
        run(command, stdin, true, &mut *self.turn).await
    }
}

/// What a git run under the ref turn carries: the turn it is bound to, its stdin and the variables
/// it adds to the pure environment.
struct Held<'a> {
    turn: &'a mut RefTurn,
    stdin: Option<&'a [u8]>,
    extra: &'a [(OsString, OsString)],
}
/// Run `git -C <dir> <args>`: in `environment` (the caller's when empty) and, when `held` is
/// given, bound to its turn with its stdin and extra variables. A nonzero exit is an error naming
/// git's stderr.
async fn git(
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
    let (stdin, turn) = match held {
        Some(Held { turn, stdin, extra }) => {
            command.envs(extra.iter().map(|(name, value)| (name, value)));
            (stdin, Some(turn))
        }
        None => (None, None),
    };
    let (status, stdout, stderr) = run_split(command, stdin, turn).await?;
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

async fn run(
    command: std::process::Command,
    stdin: Option<&[u8]>,
    merge: bool,
    turn: &mut RefTurn,
) -> Result<(std::process::ExitStatus, Vec<u8>)> {
    let (status, mut stdout, stderr) = run_split(command, stdin, Some(turn)).await?;
    if merge {
        stdout.extend_from_slice(&stderr);
    }
    Ok((status, stdout))
}

/// Run `command` and answer its status, stdout and stderr. Under the ref turn it binds itself to
/// the turn as it starts ([`RefTurn::bind_at_exec`]), so the turn is held until it exits, however
/// this process ends. One the gateway does not bind, one whose stdin cannot be written and one
/// still running when the turn is lost are sent SIGTERM, and the hold fails once it has ended, so
/// nothing runs in main beside the next writer and nothing is put back while it runs.
async fn run_split(
    mut command: std::process::Command,
    stdin: Option<&[u8]>,
    mut turn: Option<&mut RefTurn>,
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
    if let Some(turn) = turn.as_deref() {
        turn.bind_at_exec(&mut command);
    }
    let mut child = tokio::process::Command::from(command)
        .spawn()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot run {program}: {error}"),
                "install it, or put it on the bootstrap PATH",
            )
        })?;
    let pid = crate::land_turn::spawned_pid(&child);
    if let Some(turn) = turn.as_deref_mut()
        && let Err(error) = turn.bound(pid.unsigned_abs()).await
    {
        return Err(stopped(&mut child, pid, &program, &error.message).await);
    }
    let (Some(mut out), Some(mut err)) = (child.stdout.take(), child.stderr.take()) else {
        return Err(stopped(
            &mut child,
            pid,
            &program,
            "its output pipes were not opened",
        )
        .await);
    };
    let input = child.stdin.take();
    let outcome = {
        let work = async {
            let write = async {
                if let (Some(bytes), Some(mut pipe)) = (stdin, input) {
                    // Dropping the pipe once written closes the command's stdin.
                    pipe.write_all(bytes)
                        .await
                        .map_err(|error| format!("cannot write to {program}: {error}"))?;
                }
                Ok::<(), String>(())
            };
            let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
            let read = async {
                tokio::try_join!(out.read_to_end(&mut stdout), err.read_to_end(&mut stderr))
                    .map_err(|error| format!("cannot read the output of {program}: {error}"))
            };
            tokio::try_join!(write, read)?;
            // WHY unbounded: the command is this doc commit's own work, after its output has ended;
            // the ref turn's loss ends the wait, and the command is then stopped and reaped.
            #[allow(clippy::disallowed_methods)]
            let status = child
                .wait()
                .await
                .map_err(|error| format!("cannot wait for {program}: {error}"))?;
            Ok::<_, String>((status, stdout, stderr))
        };
        match turn {
            Some(turn) => tokio::select! {
                finished = work => finished,
                error = turn.lost() => Err(format!("the ref turn was lost: {error}")),
            },
            None => work.await,
        }
    };
    match outcome {
        Ok(finished) => Ok(finished),
        Err(reason) => Err(stopped(&mut child, pid, &program, &reason).await),
    }
}

/// Send SIGTERM to the command `pid` for `reason`, reap it within the reap bound, and answer the
/// error that says so: the reason, the signal and how the command ended; a signal that did not
/// reach it is said instead of the signal.
async fn stopped(
    child: &mut tokio::process::Child,
    pid: libc::pid_t,
    program: &str,
    reason: &str,
) -> CowshedError {
    let signalled = crate::land_turn::forward(pid, libc::SIGTERM, program);
    let ended = format!(
        " and {}",
        crate::land_turn::reap_stopped("doc-commit command", child, signalled.is_ok()).await
    );
    let signal = match signalled {
        Ok(()) => format!("{program} was sent SIGTERM"),
        Err(error) => format!("SIGTERM did not reach {program} ({})", error.message),
    };
    CowshedError::environment_missing(
        format!("{reason}; {signal}{ended}"),
        "check the host gateway (`cowshed gateway status`), then run cowshed doc-commit again",
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn held(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|path| (*path).to_owned()).collect()
    }

    fn kept(paths: &[&str]) -> BTreeMap<String, Kept> {
        paths
            .iter()
            .map(|path| {
                (
                    (*path).to_owned(),
                    Kept {
                        entry: Entry::Absent,
                        index: Vec::new(),
                    },
                )
            })
            .collect()
    }

    fn terminated(record: &[u8]) -> Vec<u8> {
        let mut bytes = record.to_vec();
        bytes.push(0);
        bytes
    }

    /// One whole-index read groups onto its held paths alone: every conflict stage in its
    /// printed order, modes and hashes spelled as git printed them (their widths differ here
    /// on purpose, since nothing parses them), paths holding a tab, a newline or glob
    /// spellings matched after the first tab, and records for other paths -- one not UTF-8,
    /// one holding no tab -- passed over rather than refused.
    #[test]
    fn index_records_group_losslessly_by_held_path() {
        let stage1 = format!("100644 {} 1\tconflict.md", "a".repeat(40)).into_bytes();
        let stage2 = format!("100644 {} 2\tconflict.md", "b".repeat(64)).into_bytes();
        let stage3 = b"100755 abc123 3\tconflict.md".to_vec();
        let tabbed = format!("100755 {} 0\twith\ttab.md", "c".repeat(40)).into_bytes();
        let globbed = format!("120000 {} 0\tglob*\nline?.md", "d".repeat(40)).into_bytes();
        let mut listed = Vec::new();
        for record in [&stage1, &stage2, &stage3, &tabbed, &globbed] {
            listed.extend_from_slice(&terminated(record));
        }
        listed.extend_from_slice(&terminated(b"100644 eeee 0\tother.md"));
        let mut foreign = b"100644 ffff 0\t".to_vec();
        foreign.extend_from_slice(&[0xff, 0xfe]);
        foreign.extend_from_slice(b".md");
        listed.extend_from_slice(&terminated(&foreign));
        listed.extend_from_slice(&terminated(b"not a staged record"));

        let paths = held(&[
            "conflict.md",
            "with\ttab.md",
            "glob*\nline?.md",
            "untracked.md",
        ]);
        let mut conflict = terminated(&stage1);
        conflict.extend_from_slice(&terminated(&stage2));
        conflict.extend_from_slice(&terminated(&stage3));
        let expected: BTreeMap<String, Vec<u8>> = [
            ("conflict.md".to_owned(), conflict),
            ("with\ttab.md".to_owned(), terminated(&tabbed)),
            ("glob*\nline?.md".to_owned(), terminated(&globbed)),
            ("untracked.md".to_owned(), Vec::new()),
        ]
        .into_iter()
        .collect();
        assert_eq!(index_records_by_path(&listed, &paths), expected);

        // Nothing held reads nothing onto any path, and an empty read keeps every held
        // path's empty index, which is its untracked spelling.
        assert_eq!(
            index_records_by_path(&listed, &BTreeSet::new()),
            BTreeMap::new()
        );
        assert_eq!(
            index_records_by_path(b"", &paths),
            paths
                .iter()
                .map(|path| (path.clone(), Vec::new()))
                .collect::<BTreeMap<_, _>>()
        );
    }

    /// The rollback split: held before HEAD, HEAD's own names matched by bytes (a glob
    /// spelling literal, a name that is not UTF-8 passed over), both sides of a rename on
    /// their own sides, and the empty splits.
    #[test]
    fn rollback_partitions_held_tracked_and_absent_paths() {
        let paths = held(&[
            "conflict.md",
            "with\ttab.md",
            "tracked.md",
            "new.md",
            "old-name.md",
            "renamed.md",
            "glob*\nline?.md",
        ]);
        let mut head = Vec::new();
        for name in [
            "tracked.md",
            "old-name.md",
            "glob*\nline?.md",
            "with\ttab.md",
        ] {
            head.extend_from_slice(&terminated(name.as_bytes()));
        }
        head.extend_from_slice(&terminated(&[0xff, b'.', b'm', b'd']));
        assert_eq!(
            partition_rollback(&paths, &kept(&["conflict.md", "with\ttab.md"]), &head),
            Rollback {
                held: vec!["conflict.md".to_owned(), "with\ttab.md".to_owned()],
                tracked: vec![
                    "glob*\nline?.md".to_owned(),
                    "old-name.md".to_owned(),
                    "tracked.md".to_owned(),
                ],
                absent: vec!["new.md".to_owned(), "renamed.md".to_owned()],
            }
        );
        assert_eq!(
            partition_rollback(&BTreeSet::new(), &kept(&[]), b""),
            Rollback {
                held: Vec::new(),
                tracked: Vec::new(),
                absent: Vec::new(),
            }
        );
        assert_eq!(
            partition_rollback(&paths, &kept(&[]), b""),
            Rollback {
                held: Vec::new(),
                tracked: Vec::new(),
                absent: paths.iter().cloned().collect(),
            }
        );
    }

    #[test]
    fn nul_paths_terminates_each_path() {
        assert_eq!(nul_paths(&[]), Vec::<u8>::new());
        assert_eq!(
            nul_paths(&["a.md".to_owned(), "b\tc.md".to_owned()]),
            b"a.md\0b\tc.md\0".to_vec()
        );
    }
}
