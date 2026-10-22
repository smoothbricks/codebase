//! Every word the `cowshed` CLI parses has a typed N-API counterpart (07_api.md, "CLI and N-API
//! option parity").
//!
//! The CLI's grammar is read from its one clap definition, `cli_command()` in
//! `crates/cowshed-cli/src/args.rs`: every leaf verb and every argument it declares, positionals,
//! flags, values and the trailing argv alike. [`PARITY`] names each one's counterpart. A wire
//! counterpart is a field of a declared operation's request record, resolved through the same
//! declarations the projections are generated from; its facade counterpart is the public
//! TypeScript member that reaches it, emitted as `src/cli-parity.generated.ts`, which the
//! package's own typecheck compiles against the handwritten facade. A verb or argument with no
//! row, a row with no verb or argument, a field the request does not carry and an operation the
//! addon does not project are refused by name; a facade member the package does not export does
//! not compile.
//!
//! The clap reader accepts exactly the builder vocabulary `args.rs` uses and refuses any other
//! call by name, so a flag cannot reach the parser through a shape this reader skips; the N-API
//! crate's parity test compares what it reads with the tree clap itself builds.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use syn::{Expr, ExprArray, ExprCall, ExprMethodCall, Item, Lit};

use crate::ir::{Api, Fields, RustType, Shape, TypeArgument, last_segment};
use crate::operations::{Operation, Scope};
use crate::typescript::renamed;

/// The grammar word of the CLI a row names: one leaf verb, or one argument of it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CliWord {
    /// The verb path, `cache clear`; `*` for the options every verb accepts.
    pub command: String,
    /// The clap argument id; empty for the verb itself.
    pub argument: String,
    pub spelling: String,
}

/// How a CLI word reaches the same behavior from TypeScript.
#[derive(Clone, Copy, Debug)]
pub enum Counterpart {
    /// The word is these fields of these operations' requests (`""` for the request itself, a
    /// dotted wire path otherwise), reached through these public facade members.
    Field {
        wire: &'static [(&'static str, &'static str)],
        facade: &'static [Facade],
    },
    /// The specification defines the word as this composition of facade calls, which is what
    /// the CLI itself runs.
    Composed {
        recipe: &'static str,
        facade: &'static [Facade],
    },
    /// Pure terminal presentation, or a human step a library caller cannot provide.
    CliOnly { reason: &'static str },
    /// No typed counterpart exists yet; `owner` is the unit that adds it and deletes this row.
    KnownGap { owner: &'static str },
}

/// One public TypeScript member: `Interface.method` of `src/types.ts`, or an export of
/// `src/index.ts`; with `argument`, that parameter, and with `path`, the dotted property in it.
#[derive(Clone, Copy, Debug)]
pub struct Facade {
    pub member: &'static str,
    pub argument: Option<u8>,
    pub path: &'static str,
}

pub struct Row {
    pub command: &'static str,
    /// Empty for the verb itself.
    pub argument: &'static str,
    pub counterpart: Counterpart,
}

/// The host verbs have no N-API channel until the host operation table lands (tg#9537).
const HOST_TABLE: &str = "the remaining host rows and launchers (tg#9537): disk-command, land-turn, credential add, sccache on the installed build, and the lock/hold/subtree-root process holders";

const fn member(member: &'static str) -> Facade {
    Facade {
        member,
        argument: None,
        path: "",
    }
}

const fn argument(member: &'static str, argument: u8, path: &'static str) -> Facade {
    Facade {
        member,
        argument: Some(argument),
        path,
    }
}

const fn field(
    wire: &'static [(&'static str, &'static str)],
    facade: &'static [Facade],
) -> Counterpart {
    Counterpart::Field { wire, facade }
}

const fn row(command: &'static str, argument: &'static str, counterpart: Counterpart) -> Row {
    Row {
        command,
        argument,
        counterpart,
    }
}

const fn gap(command: &'static str, argument: &'static str) -> Row {
    row(
        command,
        argument,
        Counterpart::KnownGap { owner: HOST_TABLE },
    )
}

const SETUP_DIALOG: &str = "setup's administrator authorization dialog is a human step a library \
                            caller cannot provide (06_cli.md, rule 3)";

/// Every leaf verb and argument of the CLI, and its counterpart.
#[rustfmt::skip]
pub const PARITY: &[Row] = &[
    row("*", "json", Counterpart::CliOnly { reason: "stdout encoding: the API returns typed values, never an envelope" }),
    row("*", "quiet", Counterpart::CliOnly { reason: "stderr guidance verbosity: the API writes nothing to stderr" }),
    row("*", "project", field(&[("project.open", "path")], &[argument("connectCoordinator", 1, ""), argument("openProject", 1, "")])),

    row("adopt", "", field(&[("coordinator.adopt", "")], &[member("Coordinator.adopt")])),
    row("adopt", "path", field(&[("coordinator.adopt", "options.path")], &[argument("Coordinator.adopt", 0, "path")])),
    row("adopt", "capacity", field(&[("coordinator.adopt", "options.capacity")], &[argument("Coordinator.adopt", 0, "capacity")])),
    row("adopt", "repo-id", field(&[("coordinator.adopt", "options.repoId")], &[argument("Coordinator.adopt", 0, "repoId")])),
    row("adopt", "quarantine", field(&[("coordinator.adopt", "options.quarantine")], &[argument("Coordinator.adopt", 0, "quarantine")])),
    row("adopt", "re-adopt", field(&[("coordinator.adopt", "options.reAdopt")], &[argument("Coordinator.adopt", 0, "reAdopt")])),

    row("setup", "", Counterpart::CliOnly { reason: SETUP_DIALOG }),
    row("setup", "uninstall", Counterpart::CliOnly { reason: SETUP_DIALOG }),
    row("setup", "force", Counterpart::CliOnly { reason: SETUP_DIALOG }),
    row("setup", "sccache", Counterpart::CliOnly { reason: SETUP_DIALOG }),
    row("setup", "retire-caches-volume", Counterpart::CliOnly { reason: SETUP_DIALOG }),
    row("setup", "downgrade", Counterpart::CliOnly { reason: SETUP_DIALOG }),

    row("fork", "", field(&[("coordinator.fork", "")], &[member("Coordinator.fork")])),
    row("fork", "workspaces", field(&[("coordinator.fork", "workspace"), ("coordinator.fork", "source")], &[argument("Coordinator.fork", 0, ""), argument("Coordinator.fork", 1, "source")])),
    row("fork", "ref", field(&[("coordinator.fork", "options.ref")], &[argument("Coordinator.fork", 1, "ref")])),
    row("fork", "clean", field(&[("coordinator.fork", "options.clean")], &[argument("Coordinator.fork", 1, "clean")])),
    row("fork", "browse", field(&[("coordinator.fork", "options.browse")], &[argument("Coordinator.fork", 1, "browse")])),
    row("fork", "slot", field(&[("coordinator.fork", "options.slot")], &[argument("Coordinator.fork", 1, "slot")])),
    row("fork", "register", field(&[("coordinator.fork", "options.register")], &[argument("Coordinator.fork", 1, "register")])),
    row("fork", "git-worktree", field(&[("coordinator.fork", "options.gitWorktree")], &[argument("Coordinator.fork", 1, "gitWorktree")])),
    row("fork", "markdown-only", field(&[("coordinator.fork", "options.markdownOnly")], &[argument("Coordinator.fork", 1, "markdownOnly")])),

    row("mv", "", field(&[("coordinator.rename", ""), ("coordinator.moveCheckout", ""), ("coordinator.changeRepoId", "")], &[member("Coordinator.rename"), member("Coordinator.moveCheckout"), member("Coordinator.changeRepoId")])),
    row("mv", "src", field(&[("coordinator.rename", "source")], &[argument("Coordinator.rename", 0, "")])),
    row("mv", "dst", field(&[("coordinator.rename", "destination"), ("coordinator.moveCheckout", "destination")], &[argument("Coordinator.rename", 1, ""), argument("Coordinator.moveCheckout", 0, "")])),
    row("mv", "repo-id", field(&[("coordinator.changeRepoId", "newRepoId")], &[argument("Coordinator.changeRepoId", 0, "")])),

    row("checkpoint", "", field(&[("worker.checkpoint", "")], &[member("WorkspaceHandle.checkpoint")])),
    row("checkpoint", "workspace", field(&[("coordinator.worker", "workspace")], &[argument("Coordinator.worker", 0, "")])),
    row("checkpoint", "label", field(&[("worker.checkpoint", "options.label")], &[argument("WorkspaceHandle.checkpoint", 0, "label")])),
    row("checkpoint", "keep", field(&[("worker.checkpoint", "options.keep")], &[argument("WorkspaceHandle.checkpoint", 0, "keep")])),

    row("restore", "", field(&[("coordinator.restore", "")], &[member("Coordinator.restore")])),
    row("restore", "workspace", field(&[("coordinator.restore", "workspace")], &[argument("Coordinator.restore", 0, "")])),
    row("restore", "label", field(&[("coordinator.restore", "label")], &[argument("Coordinator.restore", 1, "")])),

    row("ls", "", field(&[("project.list", "")], &[member("Project.listWorkspaces")])),
    row("ls", "all", field(&[("host.listProjects", "")], &[member("Host.listProjects")])),
    row("ls", "landing", field(&[("project.list", "landing"), ("host.listProjects", "landing")], &[argument("Project.listWorkspaces", 0, "landing"), argument("Host.listProjects", 0, "landing")])),
    row("ls", "landed", field(&[("project.list", "landing"), ("host.listProjects", "landing")], &[argument("Project.listWorkspaces", 0, "landing"), argument("Host.listProjects", 0, "landing")])),

    row("path", "", field(&[("project.workspace", ""), ("workspace.attach", ""), ("workspace.info", "")], &[member("Project.path")])),
    row("path", "workspace", field(&[("project.workspace", "workspace")], &[argument("Project.path", 0, "")])),
    row("path", "slot", field(&[("project.workspaceInSlot", "slot")], &[argument("Project.workspaceInSlot", 0, "")])),
    row("path", "no-attach", Counterpart::Composed { recipe: "the controller is never asked to attach: Project.path skips workspace.attach on noAttach, as the CLI does", facade: &[argument("Project.path", 1, "noAttach")] }),

    row("build-state", "", field(&[("host.buildState", "project")], &[argument("Host.buildState", 0, "")])),

    row("exec", "", field(&[("worker.exec", ""), ("coordinator.exec", "")], &[member("WorkspaceHandle.exec"), member("Coordinator.exec")])),
    row("exec", "workspace", field(&[("coordinator.worker", "workspace")], &[argument("Coordinator.worker", 0, "")])),
    row("exec", "stdin", field(&[("worker.exec", "stdin")], &[argument("WorkspaceHandle.exec", 0, "stdin")])),
    row("exec", "stdin-file", field(&[("worker.exec", "stdin")], &[argument("WorkspaceHandle.exec", 0, "stdinWorkspacePath")])),
    row("exec", "stdin-base64", field(&[("worker.exec", "stdin")], &[argument("WorkspaceHandle.exec", 0, "stdin")])),
    row("exec", "ro", field(&[("worker.exec", "mode")], &[argument("WorkspaceHandle.exec", 0, "mode")])),
    row("exec", "cwd", field(&[("worker.exec", "cwd")], &[argument("WorkspaceHandle.exec", 0, "cwd")])),
    row("exec", "session", field(&[("worker.shell", "session"), ("worker.exec", "session")], &[argument("WorkspaceHandle.shell", 0, ""), member("Session.exec")])),
    row("exec", "timeout", Counterpart::Composed { recipe: "the caller's own deadline over JobHandle.wait(), then kill() (07_api.md, \"CLI and API exec parity\")", facade: &[member("JobHandle.wait"), member("JobHandle.kill")] }),
    row("exec", "background", Counterpart::Composed { recipe: "every API exec answers its JobHandle at admission (07_api.md, \"CLI and API exec parity\")", facade: &[member("WorkspaceHandle.exec")] }),
    row("exec", "stdout-copy", field(&[("worker.exec", "stdoutCopy")], &[argument("WorkspaceHandle.exec", 0, "stdoutCopy")])),
    row("exec", "stderr-copy", field(&[("worker.exec", "stderrCopy")], &[argument("WorkspaceHandle.exec", 0, "stderrCopy")])),
    row("exec", "replace-output", field(&[("worker.exec", "stdoutCopy.policy"), ("worker.exec", "stderrCopy.policy")], &[argument("WorkspaceHandle.exec", 0, "stdoutCopy.policy"), argument("WorkspaceHandle.exec", 0, "stderrCopy.policy")])),
    row("exec", "class", field(&[("worker.exec", "class"), ("coordinator.exec", "class")], &[argument("WorkspaceHandle.exec", 0, "class"), argument("Coordinator.exec", 1, "class")])),
    row("exec", "want", field(&[("worker.exec", "want")], &[argument("WorkspaceHandle.exec", 0, "want")])),
    row("exec", "host", field(&[("coordinator.exec", "scope")], &[argument("Coordinator.exec", 1, "scope")])),
    row("exec", "argv", field(&[("worker.exec", "argv")], &[argument("WorkspaceHandle.exec", 0, "argv")])),

    row("grant", "", field(&[("coordinator.grant", ""), ("workspace.grants", ""), ("coordinator.grantProject", ""), ("coordinator.projectGrants", "")], &[member("Coordinator.grant"), member("WorkspaceRef.grants"), member("Coordinator.grantProject"), member("Coordinator.projectGrants")])),
    row("grant", "workspace", field(&[("coordinator.grant", "workspace")], &[argument("Coordinator.grant", 0, "")])),
    row("grant", "project-wide", field(&[("coordinator.grantProject", ""), ("coordinator.projectGrants", "")], &[member("Coordinator.grantProject"), member("Coordinator.projectGrants")])),
    row("grant", "read", field(&[("coordinator.grant", "delta.read"), ("coordinator.grantProject", "delta.read")], &[argument("Coordinator.grant", 1, "read"), argument("Coordinator.grantProject", 0, "read")])),
    row("grant", "write", field(&[("coordinator.grant", "delta.write")], &[argument("Coordinator.grant", 1, "write")])),
    row("grant", "deny-write", field(&[("coordinator.grant", "delta.denyWrite"), ("coordinator.grantProject", "delta.denyWrite")], &[argument("Coordinator.grant", 1, "denyWrite"), argument("Coordinator.grantProject", 0, "denyWrite")])),
    row("grant", "deny", field(&[("coordinator.grant", "delta.deny"), ("coordinator.grantProject", "delta.deny")], &[argument("Coordinator.grant", 1, "deny"), argument("Coordinator.grantProject", 0, "deny")])),
    row("grant", "egress", field(&[("coordinator.grant", "delta.egress.host"), ("coordinator.grantProject", "delta.egress.host")], &[argument("Coordinator.grant", 1, "egress"), argument("Coordinator.grantProject", 0, "egress")])),
    row("grant", "opaque", field(&[("coordinator.grant", "delta.egress.mode"), ("coordinator.grantProject", "delta.egress.mode")], &[argument("Coordinator.grant", 1, "egress"), argument("Coordinator.grantProject", 0, "egress")])),
    row("grant", "ports", field(&[("coordinator.grant", "delta.servicePorts")], &[argument("Coordinator.grant", 1, "servicePorts")])),
    row("grant", "loopback-any", field(&[("coordinator.grant", "delta.loopbackAny")], &[argument("Coordinator.grant", 1, "loopbackAny")])),

    row("revoke", "", field(&[("coordinator.revoke", ""), ("coordinator.revokeProject", "")], &[member("Coordinator.revoke"), member("Coordinator.revokeProject")])),
    row("revoke", "workspace", field(&[("coordinator.revoke", "workspace")], &[argument("Coordinator.revoke", 0, "")])),
    row("revoke", "project-wide", field(&[("coordinator.revokeProject", "")], &[member("Coordinator.revokeProject")])),
    row("revoke", "read", field(&[("coordinator.revoke", "delta.read"), ("coordinator.revokeProject", "delta.read")], &[argument("Coordinator.revoke", 1, "read"), argument("Coordinator.revokeProject", 0, "read")])),
    row("revoke", "write", field(&[("coordinator.revoke", "delta.write")], &[argument("Coordinator.revoke", 1, "write")])),
    row("revoke", "deny-write", field(&[("coordinator.revoke", "delta.denyWrite"), ("coordinator.revokeProject", "delta.denyWrite")], &[argument("Coordinator.revoke", 1, "denyWrite"), argument("Coordinator.revokeProject", 0, "denyWrite")])),
    row("revoke", "deny", field(&[("coordinator.revoke", "delta.deny"), ("coordinator.revokeProject", "delta.deny")], &[argument("Coordinator.revoke", 1, "deny"), argument("Coordinator.revokeProject", 0, "deny")])),
    row("revoke", "egress", field(&[("coordinator.revoke", "delta.egress.host"), ("coordinator.revokeProject", "delta.egress.host")], &[argument("Coordinator.revoke", 1, "egress"), argument("Coordinator.revokeProject", 0, "egress")])),
    row("revoke", "loopback-any", field(&[("coordinator.revoke", "delta.loopbackAny")], &[argument("Coordinator.revoke", 1, "loopbackAny")])),
    row("revoke", "all", Counterpart::Composed { recipe: "read the grants and revoke every granted item at their revision (expectedRevision), as the CLI does", facade: &[member("WorkspaceRef.grants"), argument("Coordinator.revoke", 1, "expectedRevision"), member("Coordinator.projectGrants"), argument("Coordinator.revokeProject", 0, "expectedRevision")] }),

    row("rm", "", field(&[("coordinator.destroy", ""), ("coordinator.removeProject", "")], &[member("Coordinator.remove"), member("Coordinator.removeProject")])),
    row("rm", "workspace", field(&[("coordinator.destroy", "workspace")], &[argument("Coordinator.remove", 0, "")])),
    row("rm", "force", field(&[("coordinator.destroy", "options.force"), ("coordinator.removeProject", "options.force")], &[argument("Coordinator.remove", 1, "force"), argument("Coordinator.removeProject", 0, "force")])),
    row("rm", "restore", field(&[("coordinator.destroy", "options.restore")], &[argument("Coordinator.remove", 1, "restore")])),
    row("rm", "abandon", field(&[("coordinator.destroy", "options.abandon"), ("coordinator.removeProject", "options.abandon")], &[argument("Coordinator.remove", 1, "abandon"), argument("Coordinator.removeProject", 0, "abandon")])),
    row("rm", "purge", field(&[("coordinator.removeProject", "")], &[member("Coordinator.removeProject")])),

    row("forget", "", field(&[("host.forgetAbandoned", "")], &[member("Host.forgetAbandoned")])),
    row("forget", "repo", field(&[("host.forgetAbandoned", "repoId")], &[argument("Host.forgetAbandoned", 0, "")])),
    row("forget", "abandon", Counterpart::Composed { recipe: "the operation is the authorization: host.forgetAbandoned retires only on its own explicit name, as the parser refuses forget without --abandon", facade: &[member("Host.forgetAbandoned")] }),

    row("attach", "", field(&[("workspace.attach", "")], &[member("WorkspaceRef.attach")])),
    row("attach", "workspace", field(&[("project.workspace", "workspace")], &[argument("Project.workspace", 0, "")])),
    row("attach", "all", field(&[("host.attachAll", "")], &[member("Host.attachAll")])),
    row("attach", "browse", field(&[("workspace.attach", "options.browse"), ("host.attachAll", "browse")], &[argument("WorkspaceRef.attach", 0, "browse"), argument("Host.attachAll", 0, "browse")])),

    row("detach", "", field(&[("coordinator.detach", "")], &[member("Coordinator.detach")])),
    row("detach", "workspace", field(&[("coordinator.detach", "workspace")], &[argument("Coordinator.detach", 0, "")])),
    row("detach", "all", field(&[("host.detachAll", "")], &[member("Host.detachAll")])),

    row("mount", "", field(&[("host.mountMain", "")], &[member("Host.mountMain")])),
    row("mount", "target", Counterpart::Composed { recipe: "main is the only mount target, so the operation names it: host.mountMain", facade: &[member("Host.mountMain")] }),
    row("mount", "repo-id", field(&[("host.mountMain", "repoId")], &[argument("Host.mountMain", 0, "")])),

    row("resize", "", field(&[("coordinator.resize", "")], &[member("Coordinator.resize")])),
    row("resize", "workspace", field(&[("coordinator.resize", "workspace")], &[argument("Coordinator.resize", 0, "")])),
    row("resize", "size", field(&[("coordinator.resize", "capacity")], &[argument("Coordinator.resize", 1, "")])),
    row("resize", "build", field(&[("coordinator.resize", "volume")], &[argument("Coordinator.resize", 2, "")])),

    row("defrag", "", field(&[("coordinator.defragment", "")], &[member("Coordinator.defragment")])),
    row("defrag", "workspace", field(&[("coordinator.defragment", "workspace")], &[argument("Coordinator.defragment", 0, "")])),

    row("reseed", "", field(&[("coordinator.reseed", "")], &[member("Coordinator.reseed")])),
    row("reseed", "workspace", field(&[("coordinator.reseed", "workspace")], &[argument("Coordinator.reseed", 0, "")])),
    row("reseed", "from", field(&[("coordinator.reseed", "from")], &[argument("Coordinator.reseed", 1, "")])),
    row("reseed", "dry-run", field(&[("coordinator.reseed", "dryRun")], &[argument("Coordinator.reseed", 2, "")])),

    row("rekey", "", field(&[("coordinator.rekey", "")], &[member("Coordinator.rekey")])),
    row("rekey", "workspace", field(&[("coordinator.rekey", "workspace")], &[argument("Coordinator.rekey", 0, "")])),

    row("cache clear", "", field(&[("coordinator.clearNxState", ""), ("host.clearNxStore", "")], &[member("Coordinator.clearNxState"), member("Host.clearNxStore")])),
    row("cache clear", "workspace", field(&[("coordinator.clearNxState", "workspace")], &[argument("Coordinator.clearNxState", 0, "")])),

    row("gc", "", field(&[("coordinator.gc", "")], &[member("Coordinator.gc")])),
    row("gc", "dry-run", field(&[("coordinator.gc", "options.dryRun")], &[argument("Coordinator.gc", 0, "dryRun")])),
    row("gc", "abandoned", field(&[("coordinator.gc", "options.abandoned")], &[argument("Coordinator.gc", 0, "abandoned")])),
    row("gc", "supersede", field(&[("coordinator.gc", "options.supersede.workspace"), ("coordinator.gc", "options.supersede.head")], &[argument("Coordinator.gc", 0, "supersede.workspace"), argument("Coordinator.gc", 0, "supersede.head")])),
    row("gc", "reason", field(&[("coordinator.gc", "options.supersede.reason")], &[argument("Coordinator.gc", 0, "supersede.reason")])),

    row("push", "", field(&[("worker.push", "")], &[member("WorkspaceHandle.push")])),
    row("push", "workspace", field(&[("coordinator.worker", "workspace")], &[argument("Coordinator.worker", 0, "")])),
    row("push", "branch", field(&[("worker.push", "options.branch")], &[argument("WorkspaceHandle.push", 0, "branch")])),
    row("push", "expected-workspace-incarnation", field(&[("worker.push", "options.expectedWorkspaceIncarnation")], &[argument("WorkspaceHandle.push", 0, "expectedWorkspaceIncarnation")])),
    row("push", "expected-source-head", field(&[("worker.push", "options.expectedSourceHead")], &[argument("WorkspaceHandle.push", 0, "expectedSourceHead")])),
    row("push", "expected-destination-head", field(&[("worker.push", "options.expectedDestinationHead")], &[argument("WorkspaceHandle.push", 0, "expectedDestinationHead")])),

    row("rebase", "", field(&[("coordinator.rebase", "")], &[member("Coordinator.rebase")])),
    row("rebase", "workspace", field(&[("coordinator.rebase", "workspace")], &[argument("Coordinator.rebase", 0, "")])),
    row("rebase", "into", field(&[("coordinator.rebase", "into")], &[argument("Coordinator.rebase", 1, "into")])),
    row("rebase", "onto", field(&[("coordinator.rebase", "options.onto")], &[argument("Coordinator.rebase", 1, "onto")])),
    row("rebase", "expected-workspace-incarnation", field(&[("coordinator.rebase", "options.expectedWorkspaceIncarnation")], &[argument("Coordinator.rebase", 1, "expectedWorkspaceIncarnation")])),
    row("rebase", "expected-source-head", field(&[("coordinator.rebase", "options.expectedSourceHead")], &[argument("Coordinator.rebase", 1, "expectedSourceHead")])),
    row("rebase", "expected-onto-head", field(&[("coordinator.rebase", "options.expectedOntoHead")], &[argument("Coordinator.rebase", 1, "expectedOntoHead")])),
    row("rebase", "gate", field(&[("coordinator.rebase", "options.gate")], &[argument("Coordinator.rebase", 1, "gate")])),
    
    row("rebase", "ungated", field(&[("coordinator.rebase", "options.mode")], &[argument("Coordinator.rebase", 1, "mode")])),

    row("land", "", field(&[("coordinator.land", "")], &[member("Coordinator.land")])),
    row("land", "workspace", field(&[("coordinator.land", "workspace")], &[argument("Coordinator.land", 0, "")])),
    row("land", "into", field(&[("coordinator.land", "into")], &[argument("Coordinator.land", 1, "into")])),
    row("land", "target", field(&[("coordinator.land", "options.targetBranch")], &[argument("Coordinator.land", 1, "targetBranch")])),
    row("land", "check", field(&[("coordinator.land", "options.check")], &[argument("Coordinator.land", 1, "check")])),
    row("land", "no-retire", field(&[("coordinator.land", "options.retire")], &[argument("Coordinator.land", 1, "retire")])),
    row("land", "push-only", field(&[("coordinator.land", "options.pushOnly")], &[argument("Coordinator.land", 1, "pushOnly")])),
    row("land", "expected-workspace-incarnation", field(&[("coordinator.land", "options.expectedWorkspaceIncarnation")], &[argument("Coordinator.land", 1, "expectedWorkspaceIncarnation")])),
    row("land", "expected-source-head", field(&[("coordinator.land", "options.expectedSourceHead")], &[argument("Coordinator.land", 1, "expectedSourceHead")])),
    row("land", "expected-target-head", field(&[("coordinator.land", "options.expectedTargetHead")], &[argument("Coordinator.land", 1, "expectedTargetHead")])),
    row("land", "ref-lock", field(&[("coordinator.land", "options.refLock")], &[argument("Coordinator.land", 1, "refLock")])),
    
    row("land", "ungated", field(&[("coordinator.land", "options.mode")], &[argument("Coordinator.land", 1, "mode")])),

    row("doctor", "", field(&[("coordinator.doctor", "")], &[member("Coordinator.doctor")])),
    row("doctor", "repair", Counterpart::Composed { recipe: "doctor() then gc({}): the one sweep the CLI's repair runs (06_cli.md, \"Onboarding and repair\")", facade: &[member("Coordinator.doctor"), member("Coordinator.gc")] }),
    row("doctor", "clear-attach-budget", field(&[("host.clearAttachBudget", "")], &[member("Host.clearAttachBudget")])),

    gap("disk-command", ""),
    gap("disk-command", "tool"),
    gap("disk-command", "deadline-ms"),
    gap("disk-command", "argv"),

    gap("land-turn", ""),
    gap("land-turn", "repo"),
    gap("land-turn", "workspace"),
    gap("land-turn", "outside-lock"),
    gap("land-turn", "argv"),
    

    // Host process helpers for scripts: a held lock, a holder census, a held command and a
    // process-subtree root, each over the caller's own inherited descriptors.
    gap("lock", ""),
    gap("lock", "path"),
    gap("lock", "state-fd"),
    gap("lock", "owned-pid"),
    gap("lock", "argv"),
    row("lock-holders", "", field(&[("host.lockHolders", "")], &[member("Host.lockHolders")])),
    row("lock-holders", "path", field(&[("host.lockHolders", "path")], &[argument("Host.lockHolders", 0, "")])),
    gap("hold", ""),
    gap("hold", "control-fd"),
    gap("hold", "state-fd"),
    gap("hold", "argv"),
    row("tree-holders", "", field(&[("host.treeHolders", "")], &[member("Host.treeHolders")])),
    row("tree-holders", "path", field(&[("host.treeHolders", "path")], &[argument("Host.treeHolders", 0, "")])),
    gap("subtree-root", ""),
    gap("subtree-root", "handle-fd"),
    gap("subtree-root", "argv"),
    row("retire-descendants", "", field(&[("host.retireDescendants", "handle")], &[argument("Host.retireDescendants", 0, "")])),
    row("retire-descendants", "grace-ms", field(&[("host.retireDescendants", "graceMs")], &[argument("Host.retireDescendants", 1, "")])),

    row("gateway start", "", field(&[("host.gatewayStart", "")], &[member("Host.gatewayStart")])),
    row("gateway stop", "", field(&[("host.gatewayStop", "")], &[member("Host.gatewayStop")])),
    row("gateway stop", "purge", field(&[("host.gatewayStop", "purge")], &[argument("Host.gatewayStop", 0, "purge")])),
    row("gateway status", "", field(&[("host.gatewayStatus", "")], &[member("Host.gatewayStatus")])),
    row("gateway run", "", Counterpart::CliOnly { reason: "process entrypoint: the daemon launchd execs; a library call never becomes it and no host connection runs it (07_api.md, Host operations)" }),

    row("controller", "", Counterpart::Composed { recipe: "an embedding process starts `cowshed controller` on one end of a socketpair and passes the other to coordinatorEndpoint (07_api.md, \"cowshed-napi\")", facade: &[member("coordinatorEndpoint")] }),
    row("host", "", Counterpart::Composed { recipe: "an embedding process calls openHost, which starts `cowshed host` through the package launcher on a socketpair and connects to it (07_api.md, Host operations)", facade: &[member("openHost")] }),

    gap("credential add", ""),
    gap("credential add", "origin"),
    gap("credential add", "path-prefix"),
    gap("credential add", "method"),
    gap("credential add", "secret-env"),
    gap("credential add", "secret-command"),
    gap("credential add", "secret-stdin"),
    row("credential ls", "", field(&[("host.credentialStatus", "project")], &[argument("Host.credentialStatus", 0, "")])),
    row("credential status", "", field(&[("host.credentialStatus", "project")], &[argument("Host.credentialStatus", 0, "")])),
    row("credential rm", "", field(&[("host.credentialRemove", "project")], &[argument("Host.credentialRemove", 0, "")])),
    row("credential rm", "origin", field(&[("host.credentialRemove", "origin")], &[argument("Host.credentialRemove", 1, "")])),

    row("identity add", "", field(&[("host.identityAdd", "project")], &[argument("Host.identityAdd", 0, "")])),
    row("identity add", "remote", field(&[("host.identityAdd", "remote")], &[argument("Host.identityAdd", 1, "")])),

    gap("sccache start", ""),
    gap("sccache start", "capacity"),
    gap("sccache stop", ""),
    gap("sccache status", ""),

    row("skill install", "", field(&[("host.skillInstall", "")], &[member("Host.skillInstall")])),
    row("skill install", "harness", field(&[("host.skillInstall", "harnesses")], &[argument("Host.skillInstall", 0, "harnesses")])),
];

/// What the check found, for the generator's one summary line.
pub struct Checked {
    pub typescript: String,
    pub summary: String,
}

/// Compare the CLI's clap tree with [`PARITY`], resolve every wire field through the request
/// declarations, and emit the facade assertions.
pub fn check(args_source: &str, operations: &[Operation], api: &Api) -> Result<Checked, String> {
    check_rows(&read_cli(args_source)?, PARITY, operations, api)
}

/// [`check`] over an explicit table, so a test can refuse a deliberately broken one.
pub fn check_rows(
    cli: &[CliWord],
    rows: &[Row],
    operations: &[Operation],
    api: &Api,
) -> Result<Checked, String> {
    let mut problems = Vec::new();
    let words: BTreeMap<(&str, &str), &CliWord> = cli
        .iter()
        .map(|word| ((word.command.as_str(), word.argument.as_str()), word))
        .collect();
    let mut covered = BTreeSet::new();
    for row in rows {
        let key = (row.command, row.argument);
        if !covered.insert(key) {
            problems.push(format!("cli parity row `{}` is listed twice", spelled(row)));
        }
        if !words.contains_key(&key) {
            problems.push(format!(
                "cli parity row `{}` names nothing the CLI parses; delete it",
                spelled(row)
            ));
        }
        if let Counterpart::Field { wire, facade } = row.counterpart {
            if facade.is_empty() {
                problems.push(format!(
                    "cli parity row `{}` names no facade member",
                    spelled(row)
                ));
            }
            let label = words
                .get(&key)
                .map_or_else(|| spelled(row), |word| word.spelling.clone());
            for (method, path) in wire {
                if let Err(problem) = resolve(operations, api, method, path) {
                    problems.push(format!("cowshed {label}: {problem}"));
                }
            }
        }
    }
    for word in cli {
        if !covered.contains(&(word.command.as_str(), word.argument.as_str())) {
            problems.push(format!(
                "cowshed {} has no N-API counterpart: add the request field and facade member, then its PARITY row",
                word.spelling
            ));
        }
    }
    if !problems.is_empty() {
        return Err(format!(
            "CLI/N-API parity ({} problems):\n  {}",
            problems.len(),
            problems.join("\n  ")
        ));
    }
    let count = |predicate: fn(&Counterpart) -> bool| {
        rows.iter()
            .filter(|row| predicate(&row.counterpart))
            .count()
    };
    let summary = format!(
        "cli parity: {} verbs and arguments: {} wire fields, {} composed, {} cli-only, {} known gaps ({HOST_TABLE})",
        cli.len(),
        count(|counterpart| matches!(counterpart, Counterpart::Field { .. })),
        count(|counterpart| matches!(counterpart, Counterpart::Composed { .. })),
        count(|counterpart| matches!(counterpart, Counterpart::CliOnly { .. })),
        count(|counterpart| matches!(counterpart, Counterpart::KnownGap { .. })),
    );
    Ok(Checked {
        typescript: typescript(cli_spellings(cli), rows, &summary),
        summary,
    })
}

fn spelled(row: &Row) -> String {
    match (row.command, row.argument) {
        ("*", argument) => format!("--{argument}"),
        (command, "") => command.to_owned(),
        (command, argument) => format!("{command} {argument}"),
    }
}

fn cli_spellings(cli: &[CliWord]) -> BTreeMap<(&str, &str), &str> {
    cli.iter()
        .map(|word| {
            (
                (word.command.as_str(), word.argument.as_str()),
                word.spelling.as_str(),
            )
        })
        .collect()
}

/// Walk `path` from `method`'s request record to the field it names.
fn resolve(operations: &[Operation], api: &Api, method: &str, path: &str) -> Result<(), String> {
    let operation = operations
        .iter()
        .find(|operation| operation.method == method)
        .ok_or_else(|| format!("names {method}, which the operation table does not declare"))?;
    if operation.scope == Scope::Internal {
        return Err(format!(
            "names {method}, which is controller-internal and not projected onto the addon"
        ));
    }
    let mut ty = RustType::from_syn(operation.request.clone())?;
    let mut walked = String::new();
    for segment in path.split('.').filter(|segment| !segment.is_empty()) {
        ty = field_type(api, &ty, segment).ok_or_else(|| {
            format!(
                "names {method} `{path}`, but the request {} carries no `{segment}`",
                if walked.is_empty() {
                    "record".to_owned()
                } else {
                    format!("field `{walked}`")
                }
            )
        })?;
        if !walked.is_empty() {
            walked.push('.');
        }
        walked.push_str(segment);
    }
    Ok(())
}

/// The type of the wire field `name` of the record `ty` names, through the wrappers a field's
/// type passes its record through.
fn field_type(api: &Api, ty: &RustType, name: &str) -> Option<RustType> {
    let RustType::Named {
        name: type_name,
        arguments,
    } = ty
    else {
        return None;
    };
    let last = last_segment(type_name);
    if matches!(last, "Option" | "Vec" | "Box" | "Arc") {
        let [TypeArgument::Type { value }] = arguments.as_slice() else {
            return None;
        };
        return field_type(api, value, name);
    }
    let record = api.records.get(last)?;
    match &record.shape {
        Shape::Alias(target) => field_type(api, target, name),
        Shape::Struct(Fields::Named(fields)) => fields.iter().find_map(|field| {
            if field.serde.skip {
                return None;
            }
            if field.serde.flatten {
                return field_type(api, &field.ty, name);
            }
            let declared = field.name.as_deref()?;
            let wire = renamed(
                declared,
                field.serde.rename.as_deref(),
                record.serde.rename_all.as_deref(),
            )
            .ok()?;
            (wire == name).then(|| field.ty.clone())
        }),
        Shape::Struct(_) | Shape::Enum(_) => None,
    }
}

/// The facade assertions: one property per mapped word, whose type names the public members it
/// reaches. A member the package does not export does not compile.
fn typescript(spellings: BTreeMap<(&str, &str), &str>, rows: &[Row], summary: &str) -> String {
    let mut interfaces = BTreeSet::new();
    let mut functions = false;
    let mut body = String::new();
    for row in rows {
        let (facade, kind) = match row.counterpart {
            Counterpart::Field { facade, .. } => (facade, "wire"),
            Counterpart::Composed { facade, .. } => (facade, "composed"),
            Counterpart::CliOnly { .. } | Counterpart::KnownGap { .. } => continue,
        };
        let spelling = spellings
            .get(&(row.command, row.argument))
            .copied()
            .unwrap_or_default();
        let types: Vec<String> = facade
            .iter()
            .map(|facade| {
                let base = match facade.member.split_once('.') {
                    Some((interface, method)) => {
                        interfaces.insert(interface);
                        format!("{interface}['{method}']")
                    }
                    None => {
                        functions = true;
                        format!("typeof Cowshed.{}", facade.member)
                    }
                };
                let Some(index) = facade.argument else {
                    return base;
                };
                facade
                    .path
                    .split('.')
                    .filter(|segment| !segment.is_empty())
                    .fold(
                        format!("NonNullable<Parameters<{base}>[{index}]>"),
                        |inner, segment| format!("At<{inner}, '{segment}'>"),
                    )
            })
            .collect();
        writeln!(body, "  /** `cowshed {spelling}` ({kind}). */").unwrap();
        writeln!(body, "  readonly '{spelling}': [{}];", types.join(", ")).unwrap();
    }
    let mut output = format!(
        "// Generated by cowshed-api-gen from cowshed-cli's clap tree and its PARITY table. Do not edit.\n// {summary}\n\n"
    );
    if functions {
        output.push_str("import type * as Cowshed from './index.js';\n");
    }
    if !interfaces.is_empty() {
        writeln!(
            output,
            "import type {{ {} }} from './types.js';",
            interfaces.into_iter().collect::<Vec<_>>().join(", ")
        )
        .unwrap();
    }
    output.push_str(
        "\n/** One property of a property path; a property the type does not declare does not compile. */\n\
         type At<T, K extends keyof NonNullable<T>> = NonNullable<T>[K];\n\n\
         /** Every CLI verb and argument with a typed counterpart, as the public members that reach it. */\n\
         export interface CliParity {\n",
    );
    output.push_str(&body);
    output.push_str("}\n");
    output
}

/// Read every leaf verb and argument from `cli_command()` and `global_args()` in `args.rs`.
pub fn read_cli(source: &str) -> Result<Vec<CliWord>, String> {
    let file = syn::parse_file(source).map_err(|error| format!("args.rs: {error}"))?;
    let function = |name: &str| {
        file.items
            .iter()
            .find_map(|item| match item {
                Item::Fn(function) if function.sig.ident == name => Some(function),
                _ => None,
            })
            .ok_or_else(|| format!("args.rs declares no fn {name}"))
    };
    let tail = |name: &str| -> Result<Expr, String> {
        let body = &function(name)?.block;
        match body.stmts.as_slice() {
            [syn::Stmt::Expr(expression, None)] => Ok(expression.clone()),
            _ => Err(format!(
                "fn {name} in args.rs is not one builder expression"
            )),
        }
    };
    let globals = match tail("global_args")? {
        Expr::Array(array) => array
            .elems
            .iter()
            .map(argument_word)
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err("fn global_args in args.rs is not one array of arguments".to_owned()),
    };
    let mut words = Vec::new();
    let mut root = Node::default();
    walk(&tail("cli_command")?, &mut root, &globals)?;
    if root.name != "cowshed" {
        return Err(format!("cli_command builds `{}`, not `cowshed`", root.name));
    }
    for (id, spelling) in &root.arguments {
        words.push(CliWord {
            command: "*".to_owned(),
            argument: id.clone(),
            spelling: spelling.clone(),
        });
    }
    for child in &root.children {
        leaves(child, "", &mut words);
    }
    words.sort();
    Ok(words)
}

#[derive(Default)]
struct Node {
    name: String,
    arguments: Vec<(String, String)>,
    children: Vec<Node>,
}

fn leaves(node: &Node, prefix: &str, words: &mut Vec<CliWord>) {
    let command = if prefix.is_empty() {
        node.name.clone()
    } else {
        format!("{prefix} {}", node.name)
    };
    if node.children.is_empty() {
        words.push(CliWord {
            command: command.clone(),
            argument: String::new(),
            spelling: command.clone(),
        });
    }
    for (id, spelling) in &node.arguments {
        words.push(CliWord {
            command: command.clone(),
            argument: id.clone(),
            spelling: format!("{command} {spelling}"),
        });
    }
    for child in &node.children {
        leaves(child, &command, words);
    }
}

/// Settings that shape parsing but declare no verb or argument.
const COMMAND_SETTINGS: &[&str] = &[
    "no_binary_name",
    "disable_help_flag",
    "disable_help_subcommand",
    "disable_version_flag",
    "subcommand_required",
    "version",
];

fn walk(expression: &Expr, node: &mut Node, globals: &[(String, String)]) -> Result<(), String> {
    match expression {
        Expr::MethodCall(call) => {
            walk(&call.receiver, node, globals)?;
            command_method(call, node, globals)
        }
        Expr::Call(call) => {
            node.name = match callee(call).as_str() {
                "ClapCommand::new" | "leaf" => string_argument(call)?,
                other => {
                    return Err(format!(
                        "args.rs: a command is built by `{other}`, which the parity reader does not know"
                    ));
                }
            };
            Ok(())
        }
        Expr::Paren(inner) => walk(&inner.expr, node, globals),
        other => Err(format!(
            "args.rs: unsupported command expression `{}`",
            quote::quote!(#other)
        )),
    }
}

fn command_method(
    call: &ExprMethodCall,
    node: &mut Node,
    globals: &[(String, String)],
) -> Result<(), String> {
    let method = call.method.to_string();
    match method.as_str() {
        "subcommand" => {
            let [child] = single(call)?;
            let mut subcommand = Node::default();
            walk(child, &mut subcommand, globals)?;
            node.children.push(subcommand);
        }
        "arg" => {
            let [argument] = single(call)?;
            node.arguments.push(argument_word(argument)?);
        }
        "args" => match single(call)? {
            [Expr::Array(ExprArray { elems, .. })] => {
                for argument in elems {
                    node.arguments.push(argument_word(argument)?);
                }
            }
            [Expr::Call(inner)] if callee(inner) == "global_args" => {
                node.arguments.extend(globals.iter().cloned())
            }
            [other] => {
                return Err(format!(
                    "args.rs: `.args({})` is neither an array nor global_args()",
                    quote::quote!(#other)
                ));
            }
        },
        setting if COMMAND_SETTINGS.contains(&setting) => {}
        other => {
            return Err(format!(
                "args.rs: command method `.{other}` is unknown to the parity reader; teach it whether `.{other}` declares an argument"
            ));
        }
    }
    Ok(())
}

/// Argument settings that shape how one argument parses but add no word and no spelling.
const ARGUMENT_SETTINGS: &[&str] = &[
    "short",
    "action",
    "global",
    "value_name",
    "value_parser",
    "allow_hyphen_values",
    "num_args",
    "required",
    "overrides_with",
];

/// One argument's clap id and its spelling as typed.
fn argument_word(expression: &Expr) -> Result<(String, String), String> {
    let mut base = expression;
    let mut long = false;
    let mut last = false;
    while let Expr::MethodCall(call) = base {
        match call.method.to_string().as_str() {
            "long" => long = true,
            "last" => last = true,
            setting if ARGUMENT_SETTINGS.contains(&setting) => {}
            other => {
                return Err(format!(
                    "args.rs: argument method `.{other}` is unknown to the parity reader; teach it whether `.{other}` adds a spelling or a word"
                ));
            }
        }
        base = &call.receiver;
    }
    let Expr::Call(call) = base else {
        return Err(format!(
            "args.rs: unsupported argument expression `{}`",
            quote::quote!(#expression)
        ));
    };
    let id = string_argument(call)?;
    let spelling = match callee(call).as_str() {
        "flag" | "value" | "value_once" | "append_value" | "path_values" => format!("--{id}"),
        "positional" => format!("<{id}>"),
        "Arg::new" if last => format!("-- <{id}>"),
        "Arg::new" if long => format!("--{id}"),
        "Arg::new" => format!("<{id}>"),
        other => {
            return Err(format!(
                "args.rs: an argument is built by `{other}`, which the parity reader does not know"
            ));
        }
    };
    Ok((id, spelling))
}

fn callee(call: &ExprCall) -> String {
    match call.func.as_ref() {
        Expr::Path(path) => path
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join("::"),
        other => quote::quote!(#other).to_string(),
    }
}

fn string_argument(call: &ExprCall) -> Result<String, String> {
    match call.args.first() {
        Some(Expr::Lit(literal)) => match &literal.lit {
            Lit::Str(text) => Ok(text.value()),
            _ => Err(format!(
                "args.rs: `{}` is not named by a string literal",
                callee(call)
            )),
        },
        _ => Err(format!(
            "args.rs: `{}` is not named by a string literal",
            callee(call)
        )),
    }
}

fn single(call: &ExprMethodCall) -> Result<[&Expr; 1], String> {
    match call.args.iter().collect::<Vec<_>>().as_slice() {
        [only] => Ok([*only]),
        _ => Err(format!("args.rs: `.{}` takes one argument", call.method)),
    }
}
