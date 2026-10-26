//! A repository's declared land and doc lane (02_workspaces.md "A declared land" and
//! "`cowshed doc-commit`"; 06_cli.md "Configuration"): what main's `.cowshed.toml` `[land]` and
//! `[doc]` declare, and the pure decisions the route and the doc commit take over what they
//! observe. Nothing here runs a process or reads a repository; the CLI's shell does both.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::api::dto::{GitOid, LandTurnName};
use crate::error::FenceRefusal;

/// One word of a declared gate: literal text, or the landing workspace's name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateWord {
    Text(String),
    /// `{workspace}`.
    Workspace,
}

/// One word of a declared after-land command: literal text, or main's head before or after the
/// land.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AfterWord {
    Text(String),
    /// `{before}`.
    Before,
    /// `{head}`.
    Head,
}

/// A declared command: a program named by literal text, then its words. Built only by
/// [`DeclaredCommand::parse`], so an empty command or one whose program is a slot cannot exist.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeclaredCommand<W> {
    program: String,
    words: Vec<W>,
}

/// Why a declared command was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandError {
    Empty,
    /// The program is a slot: what runs must be what main declares, never a value the land fills.
    ProgramIsSlot(String),
    /// A `{…}` word this command has no value for.
    UnknownSlot(String),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("a command needs at least its program"),
            Self::ProgramIsSlot(word) => write!(
                formatter,
                "the program must be literal text, not the slot {word}"
            ),
            Self::UnknownSlot(word) => {
                write!(formatter, "{word} is not a slot this command fills")
            }
        }
    }
}

/// The slots one kind of command fills.
pub trait Slots: Sized {
    fn text(text: String) -> Self;
    /// The slot `word` names (`{workspace}`), `None` when this kind has no such slot.
    fn slot(word: &str) -> Option<Self>;
}

impl Slots for GateWord {
    fn text(text: String) -> Self {
        Self::Text(text)
    }
    fn slot(word: &str) -> Option<Self> {
        (word == "{workspace}").then_some(Self::Workspace)
    }
}

impl Slots for AfterWord {
    fn text(text: String) -> Self {
        Self::Text(text)
    }
    fn slot(word: &str) -> Option<Self> {
        match word {
            "{before}" => Some(Self::Before),
            "{head}" => Some(Self::Head),
            _ => None,
        }
    }
}

/// A word spelled `{…}` is a slot; any other word is literal text.
fn is_slot_spelling(word: &str) -> bool {
    word.len() > 2 && word.starts_with('{') && word.ends_with('}')
}

impl<W: Slots> DeclaredCommand<W> {
    pub fn parse(words: Vec<String>) -> Result<Self, CommandError> {
        let mut words = words.into_iter();
        let program = words.next().ok_or(CommandError::Empty)?;
        if is_slot_spelling(&program) {
            return Err(CommandError::ProgramIsSlot(program));
        }
        let words = words
            .map(|word| {
                if is_slot_spelling(&word) {
                    W::slot(&word).ok_or(CommandError::UnknownSlot(word))
                } else {
                    Ok(W::text(word))
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { program, words })
    }

    pub fn program(&self) -> &str {
        &self.program
    }
}

impl DeclaredCommand<GateWord> {
    /// The gate's argv for the land of `workspace`.
    pub fn argv(&self, workspace: &str) -> Vec<OsString> {
        std::iter::once(OsString::from(&self.program))
            .chain(self.words.iter().map(|word| match word {
                GateWord::Text(text) => OsString::from(text),
                GateWord::Workspace => OsString::from(workspace),
            }))
            .collect()
    }
}

impl DeclaredCommand<AfterWord> {
    /// The command's argv for a land that moved main from `before` to `head`.
    pub fn argv(&self, before: &GitOid, head: &GitOid) -> Vec<String> {
        std::iter::once(self.program.clone())
            .chain(self.words.iter().map(|word| match word {
                AfterWord::Text(text) => text.clone(),
                AfterWord::Before => before.as_str().to_owned(),
                AfterWord::Head => head.as_str().to_owned(),
            }))
            .collect()
    }
}

pub type GateCommand = DeclaredCommand<GateWord>;
pub type AfterCommand = DeclaredCommand<AfterWord>;

/// A git remote name main is pushed to: what `git push <remote>` takes as its first operand and
/// never an option.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RemoteName(String);

impl RemoteName {
    pub fn parse(name: String) -> Result<Self, String> {
        if name.is_empty()
            || name.starts_with('-')
            || name
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(name);
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RemoteName {
    type Error = String;
    fn try_from(name: String) -> Result<Self, String> {
        Self::parse(name).map_err(|name| format!("{name:?} is not a git remote name"))
    }
}

impl From<RemoteName> for String {
    fn from(name: RemoteName) -> Self {
        name.0
    }
}

/// `[land]`: the repository lands into main by its declared route. Built only by
/// [`LandDeclaration::new`], so its ref turn is always its turn plus `.ref`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandDeclaration {
    turn: LandTurnName,
    ref_turn: LandTurnName,
    gate: GateCommand,
    push: Option<RemoteName>,
    after: Vec<AfterCommand>,
}

/// Why a `[land] turn` was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TurnError {
    /// Not a land-turn queue name, or one whose `.ref` queue would not be.
    Invalid(String),
    /// Another turn's ref turn: a land would share a queue with that repository's doc commits.
    IsRefTurn(String),
}

impl std::fmt::Display for TurnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(name) => write!(
                formatter,
                "{name:?} is not a land-turn queue whose ref turn is one too: 1 to 60 of A-Z a-z 0-9 . _ -, not starting with `.`"
            ),
            Self::IsRefTurn(name) => write!(
                formatter,
                "{name:?} ends in `.ref`, the suffix of a ref turn; name the repository's land turn"
            ),
        }
    }
}

impl LandDeclaration {
    pub fn new(
        turn: &str,
        gate: GateCommand,
        push: Option<RemoteName>,
        after: Vec<AfterCommand>,
    ) -> Result<Self, TurnError> {
        if turn.ends_with(".ref") {
            return Err(TurnError::IsRefTurn(turn.to_owned()));
        }
        let invalid = |_| TurnError::Invalid(turn.to_owned());
        Ok(Self {
            turn: LandTurnName::new(turn).map_err(invalid)?,
            ref_turn: LandTurnName::new(format!("{turn}.ref")).map_err(invalid)?,
            gate,
            push,
            after,
        })
    }

    /// The land-turn queue (05_gateway.md "Land turns").
    pub fn turn(&self) -> &LandTurnName {
        &self.turn
    }

    /// The ref turn: the queue a doc commit holds and a land holds only around its
    /// fast-forward (05_gateway.md "A ref turn").
    pub fn ref_turn(&self) -> &LandTurnName {
        &self.ref_turn
    }

    pub fn gate(&self) -> &GateCommand {
        &self.gate
    }

    /// The remote a landed main is pushed to; `None` pushes nothing.
    pub fn push(&self) -> Option<&RemoteName> {
        self.push.as_ref()
    }

    /// Run in order after the push, detached from the land.
    pub fn after(&self) -> &[AfterCommand] {
        &self.after
    }
}

/// One `[doc] paths` pattern: `*` matches any run of characters, `/` included, `?` one
/// character, every other character itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocPattern(String);

impl DocPattern {
    pub fn parse(pattern: String) -> Result<Self, String> {
        if pattern.is_empty() || pattern.starts_with('/') {
            return Err(pattern);
        }
        Ok(Self(pattern))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn matches(&self, path: &str) -> bool {
        wildmatch(self.0.as_bytes(), path.as_bytes())
    }
}

/// Iterative glob match with backtracking to the last `*`: linear in practice, no allocation.
/// `text` is UTF-8 and every step over it is one character, so `?` matches a character and a
/// literal never starts inside one.
fn wildmatch(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        match pattern.get(p) {
            Some(b'*') => {
                star = Some((p, t));
                p += 1;
            }
            Some(b'?') => {
                p += 1;
                t += char_width(text[t]);
            }
            Some(&byte) if byte == text[t] => {
                p += 1;
                t += 1;
            }
            _ => match star {
                Some((at, from)) => {
                    let next = from + char_width(text[from]);
                    p = at + 1;
                    t = next;
                    star = Some((at, next));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&byte| byte == b'*')
}

/// The length of the UTF-8 character `lead` starts.
fn char_width(lead: u8) -> usize {
    match lead {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

/// `[doc]`: which paths `cowshed doc-commit` may commit, and the check it runs on them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocDeclaration {
    pub paths: Vec<DocPattern>,
    /// Run with the admitted paths appended, after the edit and before the commit.
    pub check: Option<Vec<String>>,
}

impl DocDeclaration {
    pub fn admits(&self, path: &str) -> bool {
        self.paths.iter().any(|pattern| pattern.matches(path))
    }
}

/// What the route does once the gated land into main refused.
#[derive(Debug, Eq, PartialEq)]
pub enum AfterFastForward {
    /// Main is at the gated head: the land moved it, and the refusal is its retire's.
    Landed,
    /// Main moved since the rebase (a doc commit, say): rebase onto it and gate again.
    Regate {
        observed: Option<GitOid>,
    },
    Stop,
}

/// The route's decision over the fast-forward's refusal. `delivered` is what main says after it:
/// main holds the gated head, and the gated head is not the one the land rebased onto, so this land
/// moved main and its refusal came after the fast-forward, from the retire or what follows it. A
/// fence's refusal always comes before delivery: a target that moved since the rebase sends the land
/// back, and every other refusal is the land's answer.
pub fn after_refused_fast_forward(
    refusal: Option<&FenceRefusal>,
    delivered: bool,
) -> AfterFastForward {
    match refusal {
        Some(FenceRefusal::TargetMoved { observed }) => AfterFastForward::Regate {
            observed: observed.clone(),
        },
        Some(_) => AfterFastForward::Stop,
        None if delivered => AfterFastForward::Landed,
        None => AfterFastForward::Stop,
    }
}

/// What the route does when its rebase was refused: rebase again onto the newer head when main
/// moved before the rebase began, else stop.
pub fn rebase_again(refusal: Option<&FenceRefusal>) -> bool {
    matches!(refusal, Some(FenceRefusal::OntoMoved { .. }))
}

/// One path of `git status --porcelain=v1 -z --untracked-files=all`: its two status letters and
/// the path (for a rename or copy, both paths as two entries).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusEntry {
    pub code: [u8; 2],
    pub path: String,
}

/// Parse `git status --porcelain=v1 -z` output. A rename or copy carries its source path as the
/// next NUL-terminated field; both are entries, since an edit that renames writes both.
pub fn parse_status(bytes: &[u8]) -> Result<Vec<StatusEntry>, String> {
    let mut entries = Vec::new();
    let mut fields = bytes
        .split(|&byte| byte == 0)
        .filter(|field| !field.is_empty());
    while let Some(field) = fields.next() {
        let (code, path) = match field {
            [x, y, b' ', path @ ..] if !path.is_empty() => ([*x, *y], path),
            _ => {
                return Err(format!(
                    "git status printed an entry this parser does not know: {:?}",
                    String::from_utf8_lossy(field)
                ));
            }
        };
        let path = utf8_path(path)?;
        entries.push(StatusEntry { code, path });
        if matches!(code[0], b'R' | b'C') || matches!(code[1], b'R' | b'C') {
            let source = fields
                .next()
                .ok_or_else(|| "git status printed a rename without its source path".to_owned())?;
            entries.push(StatusEntry {
                code,
                path: utf8_path(source)?,
            });
        }
    }
    Ok(entries)
}

fn utf8_path(bytes: &[u8]) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        format!(
            "git status named a path that is not UTF-8, which a doc commit cannot judge: {:?}",
            String::from_utf8_lossy(error.as_bytes())
        )
    })
}

/// Why a doc commit refused its edit's paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DocRefusal {
    /// The edit wrote nothing.
    Empty,
    /// Paths `[doc] paths` does not admit.
    NotDoc(Vec<String>),
    /// Paths main's tree already held uncommitted before the edit.
    HeldUncommitted(Vec<String>),
}

/// Admit an edit's paths: every one documentation, none held uncommitted before the edit.
/// Answers the paths sorted and unique.
pub fn admit(
    edit: impl IntoIterator<Item = String>,
    held_before: &BTreeSet<String>,
    declaration: &DocDeclaration,
) -> Result<Vec<String>, DocRefusal> {
    let edit: BTreeSet<String> = edit.into_iter().collect();
    if edit.is_empty() {
        return Err(DocRefusal::Empty);
    }
    let not_doc: Vec<String> = edit
        .iter()
        .filter(|path| !declaration.admits(path))
        .cloned()
        .collect();
    if !not_doc.is_empty() {
        return Err(DocRefusal::NotDoc(not_doc));
    }
    let held: Vec<String> = edit.intersection(held_before).cloned().collect();
    if !held.is_empty() {
        return Err(DocRefusal::HeldUncommitted(held));
    }
    Ok(edit.into_iter().collect())
}

/// The paths a program wrote, by git's account: the entries whose status changed between the
/// reads before and after it, plus each path held uncommitted before whose bytes it changed
/// (`rewritten`, measured by the caller against the copy it kept).
pub fn written(
    before: &[StatusEntry],
    after: &[StatusEntry],
    rewritten: impl IntoIterator<Item = String>,
) -> BTreeSet<String> {
    let before: BTreeSet<(&[u8; 2], &str)> = before
        .iter()
        .map(|entry| (&entry.code, entry.path.as_str()))
        .collect();
    let after_set: BTreeSet<(&[u8; 2], &str)> = after
        .iter()
        .map(|entry| (&entry.code, entry.path.as_str()))
        .collect();
    before
        .symmetric_difference(&after_set)
        .map(|(_, path)| (*path).to_owned())
        .chain(rewritten)
        .collect()
}

/// The paths a status read names.
pub fn paths(entries: &[StatusEntry]) -> BTreeSet<String> {
    entries.iter().map(|entry| entry.path.clone()).collect()
}

/// `lands.log`, beside the turn's tickets: one JSON line per land event, appended, never
/// rewritten (02_workspaces.md "A declared land" step 5).
pub const LANDS_LOG: &str = "lands.log";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LandEvent {
    Landed,
    Unpushed,
    Pushed,
    PushFailed,
    AfterExited,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandRecord {
    pub at_unix_ms: u64,
    pub event: LandEvent,
    pub head: GitOid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<GitOid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl LandRecord {
    pub fn now(
        event: LandEvent,
        head: GitOid,
        before: Option<GitOid>,
        detail: Option<String>,
    ) -> Self {
        Self {
            at_unix_ms: unix_ms_now(),
            event,
            head,
            before,
            detail,
        }
    }
}

/// Milliseconds since the Unix epoch; 0 for a clock before it, which no record orders by.
pub fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Append `record` to `directory`'s `lands.log` as one line in one write.
pub fn record(directory: &Path, record: &LandRecord) -> io::Result<()> {
    let mut line = serde_json::to_vec(record).map_err(io::Error::other)?;
    line.push(b'\n');
    std::fs::create_dir_all(directory)?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join(LANDS_LOG))?
        .write_all(&line)
}

/// What a declared land hands its detached `__land-after` child: everything that child runs,
/// rendered, so it reads no configuration of its own.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterRequest {
    /// Main's checkout: where every command runs.
    pub main: PathBuf,
    /// The turn's directory, whose `lands.log` the outcomes append to and beside which each
    /// command's output is kept.
    pub turn: PathBuf,
    /// The branch the land moved, which the push pushes.
    pub branch: String,
    pub before: GitOid,
    pub head: GitOid,
    /// `None` when the land was told not to push or the repository declares no remote.
    pub push: Option<RemoteName>,
    /// The after commands, rendered.
    pub after: Vec<Vec<String>>,
    /// The pure environment every command runs in.
    pub environment: Vec<(String, String)>,
}

/// The caller variables a declared command keeps: identity, never the caller's project shell.
pub const IDENTITY: [&str; 6] = ["HOME", "USER", "LOGNAME", "TERM", "LANG", "SSH_AUTH_SOCK"];

/// The host bootstrap `PATH` for a declared command: the installed cowshed, the host's Nix
/// profile and program directories, the system directories; nothing of the caller's.
pub fn bootstrap_path(home: &Path) -> Vec<PathBuf> {
    let mut directories = vec![crate::host_dirs::installed_binary_directory(home)];
    for directory in crate::capabilities::host_program_directories(home)
        .into_iter()
        .chain(["/usr/sbin", "/sbin"].map(PathBuf::from))
    {
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    directories
}

/// The pure environment: the caller's identity variables `caller` answers, and `path`.
pub fn pure_environment(
    caller: impl Fn(&str) -> Option<OsString>,
    path: OsString,
) -> Vec<(OsString, OsString)> {
    IDENTITY
        .iter()
        .filter_map(|name| caller(name).map(|value| (OsString::from(name), value)))
        .chain(std::iter::once((OsString::from("PATH"), path)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(words: &[&str]) -> Result<GateCommand, CommandError> {
        GateCommand::parse(words.iter().map(|word| (*word).to_owned()).collect())
    }

    fn oid(byte: char) -> GitOid {
        GitOid::new(byte.to_string().repeat(40)).unwrap()
    }

    #[test]
    fn a_land_turn_names_its_ref_turn_and_is_never_one() {
        let gate = gate(&["true"]).unwrap();
        let declaration = LandDeclaration::new("acme", gate.clone(), None, Vec::new()).unwrap();
        assert_eq!(declaration.turn().as_str(), "acme");
        assert_eq!(declaration.ref_turn().as_str(), "acme.ref");
        assert_eq!(
            LandDeclaration::new("acme.ref", gate.clone(), None, Vec::new()),
            Err(TurnError::IsRefTurn("acme.ref".to_owned()))
        );
        assert_eq!(
            LandDeclaration::new(&"a".repeat(61), gate.clone(), None, Vec::new()),
            Err(TurnError::Invalid("a".repeat(61)))
        );
        assert!(LandDeclaration::new(".hidden", gate, None, Vec::new()).is_err());
    }

    #[test]
    fn a_gate_fills_only_its_workspace_slot() {
        let command = gate(&["sh", "tooling/land-host.sh", "--gate", "{workspace}"]).unwrap();
        assert_eq!(
            command.argv("topic"),
            ["sh", "tooling/land-host.sh", "--gate", "topic"].map(OsString::from)
        );
        assert_eq!(
            gate(&["sh", "{head}"]),
            Err(CommandError::UnknownSlot("{head}".to_owned()))
        );
        assert_eq!(
            gate(&["{workspace}"]),
            Err(CommandError::ProgramIsSlot("{workspace}".to_owned()))
        );
        assert_eq!(gate(&[]), Err(CommandError::Empty));
        // A brace inside a word is text, not a slot.
        assert!(gate(&["sh", "-c", "echo {}"]).is_ok());
    }

    #[test]
    fn an_after_command_fills_before_and_head() {
        let command = AfterCommand::parse(
            ["sh", "install.sh", "smoothbricks", "{before}", "{head}"]
                .map(str::to_owned)
                .to_vec(),
        )
        .unwrap();
        assert_eq!(
            command.argv(&oid('a'), &oid('b')),
            [
                "sh".to_owned(),
                "install.sh".to_owned(),
                "smoothbricks".to_owned(),
                "a".repeat(40),
                "b".repeat(40)
            ]
        );
        assert!(AfterCommand::parse(vec!["sh".to_owned(), "{workspace}".to_owned()]).is_err());
    }

    #[test]
    fn a_doc_pattern_star_crosses_directories() {
        let markdown = DocPattern::parse("*.md".to_owned()).unwrap();
        assert!(markdown.matches("README.md"));
        assert!(markdown.matches("specs/cowshed/02_workspaces.md"));
        assert!(!markdown.matches("src/lib.rs"));
        assert!(!markdown.matches("notes.md.bak"));
        let specs = DocPattern::parse("specs/*/?0_*.md".to_owned()).unwrap();
        assert!(specs.matches("specs/cowshed/00_overview.md"));
        assert!(!specs.matches("specs/cowshed/01_storage.md"));
        assert!(DocPattern::parse("/abs.md".to_owned()).is_err());
        let one = DocPattern::parse("?.md".to_owned()).unwrap();
        assert!(one.matches("é.md"), "? is one character, not one byte");
        assert!(!one.matches("éé.md"));
        assert!(
            DocPattern::parse("*é?.md".to_owned())
                .unwrap()
                .matches("xéé.md")
        );
    }

    #[test]
    fn only_a_moved_target_sends_the_land_back_to_its_rebase() {
        assert_eq!(
            after_refused_fast_forward(
                Some(&FenceRefusal::TargetMoved {
                    observed: Some(oid('c'))
                }),
                false
            ),
            AfterFastForward::Regate {
                observed: Some(oid('c'))
            }
        );
        // A fence refuses before delivery, whatever main holds.
        assert_eq!(
            after_refused_fast_forward(
                Some(&FenceRefusal::SourceMoved { observed: oid('d') }),
                true
            ),
            AfterFastForward::Stop
        );
        assert_eq!(
            after_refused_fast_forward(None, false),
            AfterFastForward::Stop
        );
        // An unfenced refusal with the gated head delivered came after the fast-forward.
        assert_eq!(
            after_refused_fast_forward(None, true),
            AfterFastForward::Landed
        );
        assert!(rebase_again(Some(&FenceRefusal::OntoMoved {
            observed: oid('e')
        })));
        assert!(!rebase_again(None));
    }

    #[test]
    fn status_entries_carry_both_sides_of_a_rename() {
        let entries = parse_status(b" M a.md\0R  new.md\0old.md\0?? dir/new file.md\0").unwrap();
        assert_eq!(
            paths(&entries),
            ["a.md", "dir/new file.md", "new.md", "old.md"]
                .map(str::to_owned)
                .into_iter()
                .collect()
        );
        assert!(parse_status(b"garbage\0").is_err());
        assert_eq!(parse_status(b"").unwrap(), []);
    }

    #[test]
    fn admission_refuses_code_and_paths_main_held_uncommitted() {
        let declaration = DocDeclaration {
            paths: vec![DocPattern::parse("*.md".to_owned()).unwrap()],
            check: None,
        };
        let held: BTreeSet<String> = ["held.md".to_owned()].into_iter().collect();
        assert_eq!(
            admit(["b.md".to_owned(), "a.md".to_owned()], &held, &declaration),
            Ok(vec!["a.md".to_owned(), "b.md".to_owned()])
        );
        assert_eq!(
            admit(
                ["a.md".to_owned(), "src/x.rs".to_owned()],
                &held,
                &declaration
            ),
            Err(DocRefusal::NotDoc(vec!["src/x.rs".to_owned()]))
        );
        assert_eq!(
            admit(["held.md".to_owned()], &held, &declaration),
            Err(DocRefusal::HeldUncommitted(vec!["held.md".to_owned()]))
        );
        assert_eq!(admit([], &held, &declaration), Err(DocRefusal::Empty));
    }

    #[test]
    fn a_program_wrote_the_paths_whose_status_changed_and_the_held_paths_it_rewrote() {
        let before = parse_status(b" M held.md\0?? scratch.md\0").unwrap();
        let after = parse_status(b" M held.md\0?? scratch.md\0 M doc.md\0?? new.md\0").unwrap();
        assert_eq!(
            written(&before, &after, ["held.md".to_owned()]),
            ["doc.md", "held.md", "new.md"]
                .map(str::to_owned)
                .into_iter()
                .collect()
        );
        // A path whose status code changed was written even though it was dirty before.
        let after = parse_status(b"MM held.md\0?? scratch.md\0").unwrap();
        assert!(written(&before, &after, []).contains("held.md"));
    }

    #[test]
    fn the_pure_environment_keeps_identity_and_the_bootstrap_path_only() {
        let caller = |name: &str| match name {
            "HOME" => Some(OsString::from("/home/u")),
            "SSH_AUTH_SOCK" => Some(OsString::from("/agent.sock")),
            _ => None,
        };
        assert_eq!(
            pure_environment(caller, OsString::from("/bin")),
            [
                ("HOME", "/home/u"),
                ("SSH_AUTH_SOCK", "/agent.sock"),
                ("PATH", "/bin")
            ]
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        );
        let path = bootstrap_path(Path::new("/home/u"));
        assert_eq!(
            path[0],
            crate::host_dirs::installed_binary_directory(Path::new("/home/u"))
        );
        assert!(path.contains(&PathBuf::from("/usr/bin")));
        assert!(path.contains(&PathBuf::from("/sbin")));
    }

    #[test]
    fn a_land_record_is_one_json_line_appended() {
        let directory = std::env::temp_dir().join(format!(
            "cowshed-lands-log-{}-{}",
            std::process::id(),
            unix_ms_now()
        ));
        record(
            &directory,
            &LandRecord {
                at_unix_ms: 1,
                event: LandEvent::Landed,
                head: oid('a'),
                before: Some(oid('b')),
                detail: None,
            },
        )
        .unwrap();
        record(
            &directory,
            &LandRecord {
                at_unix_ms: 2,
                event: LandEvent::PushFailed,
                head: oid('a'),
                before: None,
                detail: Some("exit 128".to_owned()),
            },
        )
        .unwrap();
        let log = std::fs::read_to_string(directory.join(LANDS_LOG)).unwrap();
        let lines: Vec<LandRecord> = log
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].event, LandEvent::PushFailed);
        assert!(log.contains("\"event\":\"push-failed\""));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
