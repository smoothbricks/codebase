# cowshed-cli/args+help+output

Scope: `packages/cowshed/crates/cowshed-cli/src/args.rs` (2893), `help.rs` (429), `output.rs` (204), `run.rs` (269),
`main.rs` (7), `lib.rs` (12). Supplementary (duplication only, not audited): `packages/cowshed/Cargo.toml` clap stanza,
`Cargo.lock` clap 4.6.6, `packages/cowshed/src/cli-trampoline.ts`, `packages/cowshed/src/types.ts`,
`packages/cowshed/crates/cowshed-cli/src/skill/generated.rs`, `cowshed-core/src/metadata.rs` `WorkspaceName::new`.

## Summary

- Clap is load-bearing as a matcher (`cli_command` + `try_get_matches_from`); do not drop it. Help is fully hand-rolled
  with clap's help flag disabled.
- The grammar is restated twice: clap builder flags vs `CommandSpec` option tables. The module comment claiming they are
  one table is false. Tests pin `CommandSpec` to itself and cannot catch clap drift.
- Bare-invocation onboarding and `cowshed --help` already disagree (trailing period). The overview test uses `contains`
  and cannot go red on that drift.
- Missing-argument sentences are written twice (`require_*` literals and `missing_required_message`) with a test as the
  only coupling.
- `args.rs` is a 2893-line god file (types, clap tree, help tables, parsers, 821 lines of tests). `cli_command` is 137
  lines.
- Global flags are named in four independent scanners. `--expected-*` help rows are copy-pasted across push/rebase/land.
- Allocations are once-per-invocation (CLI parse/help). Not findings. No `unsafe`. No TODO/FIXME. `clap_derive` is not
  in the lockfile.

## Findings

### F1 — HIGH — SSOT — Clap builder and CommandSpec are two grammars

Evidence: `packages/cowshed/crates/cowshed-cli/src/help.rs:1-7`

```
//! A [`CommandSpec`] lives beside its parser in [`crate::args`], so a flag's spelling and the one
//! line that explains it are written where the parser reads them. Everything a user sees is
//! rendered from that one value: the usage line a parse error hints, the command map, and the page
//! `cowshed <command> --help` prints. The usage line is therefore not a string anybody maintains —
//! it is the option table printed — which is why a flag cannot reach the parser and stay invisible.
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:550-570`

```
fn cli_command() -> ClapCommand {
    ClapCommand::new("cowshed")
        .no_binary_name(true)
        .disable_help_flag(true)
        .disable_help_subcommand(true)
        .version(package_version())
        .args(global_args())
        .subcommand(leaf("adopt").arg(positional("path", 0..=1)).args([
            value("capacity"),
            value("repo-id"),
            flag("quarantine"),
        ]))
        .subcommand(leaf("setup").args([flag("uninstall"), flag("force"), value("mount-root")]))
        .subcommand(leaf("new").arg(positional("name", 0..=1)).args([
            value("ref"),
            value("from"),
            flag("browse"),
            value("slot"),
            flag("register"),
            flag("git-worktree"),
        ]))
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:1270-1295` (`NEW.options` restates
`--ref/--from/--browse/--slot/--register/--git-worktree` as `Opt { spelling, meaning }`). Same split for every verb:
clap names in `cli_command` (550-686), help names in the `const …: CommandSpec` blocks (1025-2018), dispatch names again
in `cli_from_matches` (756-779). Problem: The parser reads clap `Arg` names. Help/usage/hints read `CommandSpec`. Adding
`--foo` to one and not the other is a live help-or-parse bug. I compared the two tables; they currently agree. The tests
that claim to pin "the same table the parser reads" only walk `CommandSpec` (`help.rs:336-366`, `args.rs:2818-2839`).
Fix: CommandSpec is the SSOT (custom help layout, typo correction, and "help wins on a half-typed line" are product
requirements clap's default help does not meet). Generate the clap `Arg`/`Command` tree from `CommandSpec` (parse
`Opt.spelling` into long/value, `args`/`trailing` into positionals). Delete the hand-written `cli_command` flag lists.
One table, one renderer, clap still matches. Cost/Risk: `cli_command`, every `const CommandSpec`, and `cli_from_matches`
move together. Parse tests stay. Help tests stay. Do not add the `derive` feature to do this — that pulls
`clap_derive`/`syn` for no gain.

### F2 — HIGH — DUPLICATION — Hand-rolled help beside a live clap matcher; do not drop clap

Evidence: `packages/cowshed/Cargo.toml:25`

```
clap = { version = "4", default-features = false, features = ["std", "error-context", "usage"] }
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:418-438,550-555,427`

```
pub fn parse_args<I, T>(args: I) -> Result<Cli, UsageError>
…
    match cli_command().try_get_matches_from(&args) {
        Ok(matches) => cli_from_matches(matches),
        Err(error) if error.kind() == ErrorKind::DisplayVersion => Ok(Cli {
…
/// `--help` is an answer, not a grammar check: it wins even on a half-typed line,
/// and clap never sees it because its help flag is disabled (stdout purity).
```

Evidence: clap is used for `last(true)` exec argv (`args.rs:615-620`), `ArgAction::Append` (`735-741`), `overrides_with`
last-value-wins (`718-725`), global flags (`688-705`), `subcommand_required` (`666-684`), version (`429-432`), and
`translate_clap` (`817-886`). `Cargo.lock` clap 4.6.6 depends on `clap_builder` + `clap_lex` + `anstyle` only. No
`clap_derive` package. Problem: Two wrong conclusions are available. Clap is not barely used: dropping it means
reimplementing hyphen-values, `--` last args, append vs override, globals-after-verb, and unknown-arg detection. Clap
help _is_ unused: `.disable_help_flag(true)` on root and every leaf, and `help.rs` reimplements usage/map/page/wrap.
That is duplication against the dependency's help renderer, not a reason to delete the matcher. Fix: Keep clap with the
current feature set (`std` + `error-context` + `usage`). Do not enable `derive` or `help`. Do not shell out. Collapse
the parallel grammar as in F1 so clap stops being a second author of flag names. `help.rs` stays: stdout-purity, exit 0,
`--help` on a half-typed line, and the command map are why clap's help was disabled. Cost/Risk: None if clap stays.
Dropping clap is the worse call: a custom matcher would be another ~500 lines next to the help tables that already
exist.

### F3 — MEDIUM — SSOT — Onboarding sentence already diverged

Evidence: `packages/cowshed/crates/cowshed-cli/src/help.rs:155-157` vs `help.rs:183`

```
pub fn onboarding_preamble() -> &'static str {
    "warm git workspaces — a copy-on-write checkout for each agent.\n\
     first time here? run cowshed setup, then cowshed adopt in your checkout.\n"
}
…
    page.push_str("\nfirst time here? run cowshed setup, then cowshed adopt in your checkout\n");
```

Evidence: `packages/cowshed/crates/cowshed-cli/src/help.rs:408-411` —
`overview.contains("first time here? run cowshed setup, then cowshed adopt in your checkout")` matches both the period
and no-period copies. Problem: Bare `cowshed` (preamble) prints the sentence with a period. `cowshed --help` (overview)
prints it without. Live copy drift. The test cannot go red (PERFORMANCE-HANDBOOK §7.10bb). Fix: `overview` must call
`onboarding_preamble()` (or a shared third line) instead of restating the sentence. Change the test to
`assert!(overview.contains(onboarding_preamble()) || overview.contains(the exact preamble line including the period))` —
equality on the shared string, not a prefix `contains`. Cost/Risk: Help goldens in `tests/output_contracts.rs` (other
slice) that snapshot overview text.

### F4 — MEDIUM — SSOT — Missing-argument sentences written twice

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:888-904`

```
/// Clap's missing-argument fallback must say the same thing as the hand-written
/// `require_*` checks, so `cowshed new` never answers with two different sentences
/// depending on which path noticed the gap.
fn missing_required_message(spec: &CommandSpec) -> String {
    match spec.name {
        "gateway" | "sccache" | "skill" => format!("{} action is required", spec.name),
        "new" => String::from("new requires a workspace name"),
        "fork" => String::from("fork requires a source workspace"),
        …
        "land" => String::from("land requires a workspace"),
        _ => format!("{} requires an argument", spec.name),
    }
}
```

Evidence: the same literals are passed into `require_workspace` at `args.rs:1306-1311`, `1336-1341`, `1371`,
`1473-1478`, `1656-1661`, `1707-1712`, `1830-1834`, `1992-1997`, and `parse_detach` `1810`. `args.rs:2843-2865` asserts
the two copies still match. Problem: The comment admits the duplication. The test is the coupling. A new required
positional that updates only one path yields two sentences for the same gap (the exact bug the comment names). Clap
never sees required positionals anyway (see F8), so the clap fallback map exists only because arity was left out of
clap. Fix: Put the missing-argument sentence on `CommandSpec` (one field). `require_*` and `translate_clap` both read
it. Delete `missing_required_message` and the duplicated literals. Keep the test as
`parse_args(["new"]).unwrap_err().message == NEW.missing`. Cost/Risk: `CommandSpec` grows one field; every
`const CommandSpec` and the `missing_positionals_*` test update.

### F5 — MEDIUM — STRUCTURE — args.rs is the CLI god file

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:1-2893` — 2071 lines of production (types 41-416, clap tree
550-686, error translation 817-947, twenty `CommandSpec` + `parse_*` pairs 1025-2026) plus tests 2072-2893.
`cli_command` is 550-686 (137 lines). Problem: Under the 5k-line automatic bar, but the seams are already named in the
file: clap tree, help tables, typed `Command`/`*Args`, per-verb validation, clap error translation. Twenty verbs share
one module. `cli_command` is over ~100 lines because it is the second grammar (F1). Fix: After F1, `cli_command` shrinks
to a fold over `COMMANDS`. Then split: `args/mod.rs` (Cli/Command/parse_args), `args/grammar.rs` (CommandSpec consts —
or keep them next to parse_* per verb in `args/new.rs` etc.), `args/clap.rs` (generated matcher + `translate_clap`),
`args/tests.rs`. Do not split before F1 or you copy the dual grammar into more files. Cost/Risk: `lib.rs` `pub mod args`
surface stays. Internal-only split.

### F6 — MEDIUM — TESTS — SSOT tests cannot detect clap/CommandSpec drift

Evidence: `packages/cowshed/crates/cowshed-cli/src/help.rs:336-366` walks `spec.options` vs `spec.page()` (both from
CommandSpec). `help.rs:370-389` walks `COMMANDS` vs `command_map()` (both from CommandSpec). `args.rs:2818-2839` walks
`spec.options` vs `spec.hint()` (both from CommandSpec). `args.rs:2778-2815`
`every_command_declares_its_project_discovery_requirement` is a hand-maintained argv table; adding a verb to `COMMANDS`
without a row still passes. Problem: PERFORMANCE-HANDBOOK §7.10bb. Substituting a clap flag that is not in `CommandSpec`
(or the reverse) leaves every "same table" test green. The only clap-vs-help pin is incidental (`unknown flag` hint
equals `spec.hint()` for `--unknown` on `new`). Fix: One test: for each clap leaf in `cli_command()`, every long flag
and positional name appears in that verb's `CommandSpec` (parse `Opt.spelling` / `args`), and every `CommandSpec` option
appears as a clap `Arg`. After F1 this test is tautological and can go. Until then it is the only test that can go red.
Drive `project_discovery` cases from `COMMANDS` plus a per-spec expected value, not a parallel argv list. Cost/Risk: The
new test depends on clap's `get_arguments()` API remaining stable, or on F1 landing first.

### F7 — LOW — DUPLICATION — `--expected-*` help rows copied three times

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:1879-1885`, `1918-1924`, `1970-1976` — identical `Opt` for
`--expected-workspace-incarnation` and `--expected-source-head` on PUSH, REBASE, LAND. Destination/onto/target heads are
near-copies (`1877-1890` vs `1978-1980`). Problem: Three tables of the same coordinator preconditions. A wording fix on
one help page will miss the others (already the kind of drift F3 demonstrates). Fix:
`const EXPECTED_WORKSPACE_INCARNATION: Opt` and `EXPECTED_SOURCE_HEAD: Opt` next to `GLOBALS`. Each command's
`options: &[…]` names those constants plus its own dest/onto/target row. Cost/Risk: Help snapshot strings unchanged if
the text is moved not edited.

### F8 — LOW — STRUCTURE — `positional` discards arity, so clap cannot require arguments

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:744-749`

```
fn positional(name: &'static str, _range: std::ops::RangeInclusive<usize>) -> Arg {
    Arg::new(name)
        .value_parser(value_parser!(OsString))
        .required(false)
        .num_args(1)
}
```

Every call site still writes `0..=1` (`args.rs:557,563,573,578,584,590,596,601,623,630,635,640,644,650,656,639`).
Required-ness is then re-checked in `parse_*` / `require_workspace`, which is why F4 exists. Problem: The range argument
is dead. Clap is told every positional is optional. Missing-arg errors are a second parser. Fix: Honor the range
(`required(true)` when min is 1) _or_ delete `_range` and stop pretending clap knows arity. Prefer honoring it after F1,
then delete `missing_required_message`. Cost/Risk: Clap missing-arg error kinds will change; `translate_clap` and F4's
test table must move with them.

### F9 — LOW — SSOT — Workspace-name charset restated against core

Evidence: `packages/cowshed/crates/cowshed-cli/src/args.rs:2056-2064`

```
        Err(MetadataError::ReservedSessionName) => {
            Err(UsageError::new("workspace name `main` is reserved", usage))
        }
        Err(_) => Err(UsageError::new(
            "workspace names must match [a-z0-9][a-z0-9-]{0,63}",
            usage,
        )),
```

Evidence: `packages/cowshed/crates/cowshed-core/src/metadata.rs:328-340` — `WorkspaceName::new` is `(1..=64)` and
`[a-z0-9][a-z0-9-]*`. The regex currently matches. Core `Display` is `invalid workspace name {name:?}`
(`metadata.rs:76`), so the CLI invented its own grammar string. Problem: If core tightens length or charset, this usage
error lies. The CLI already has the `MetadataError` in hand and throws it away. Fix: Map
`MetadataError::InvalidWorkspaceName` through a single formatter owned by core (or Display that already names the rule).
Delete the regex literal from `args.rs`. Cost/Risk: Cross-slice (CsCoreMetadata). Error text change is a CLI contract;
pin it in `validates_names_and_new_option_conflicts`.

### F10 — LOW — SSOT — Global flags named in four scanners

Evidence:

- `help.rs:125-138` `GLOBALS` (`--json`, `-q, --quiet`, `--project <git-root>`)
- `args.rs:688-705` `global_args()` clap
- `args.rs:443-451` `parse_help_request` (`--json` / `-q|--quiet` / `--project`)
- `run.rs:38-40` `option_before_child_argv(&arguments, "--json"| "--quiet"| "-q")` before parse, then
  `run_parsed(parsed, json)` uses that pre-scan rather than `parsed.global.json` Problem: Four authors of the same three
  flags. `--json` on a usage error must work before clap succeeds, so a pre-scan is justified; it should still share the
  token list with `GLOBALS`. Fix: One `&[Opt]` (already `GLOBALS`). Help renders it. Clap `global_args` is generated
  from it (F1). Help pre-scan and `run.rs` pre-scan iterate the same spellings. `run_parsed` should take
  `parsed.global.json` after a successful parse; keep the pre-scan only for the `Err(UsageError)` path in `run`.
  Cost/Risk: `run.rs` usage-error JSON mode. Test that `--json` on a bad argv still emits the envelope.

## Cross-slice questions

- `cowshed-core/src/metadata.rs` `WorkspaceName::new` owns the charset/length (F9). Does core want a user-facing grammar
  string, or should CLI keep a mapped message?
- `packages/cowshed/src/cli-trampoline.ts:44` `SERVICE_VERBS = { gateway: true, sccache: true }` is a deliberate subset
  of CLI verbs, not a full restatement of `COMMANDS`. A new launchd-managed verb in `args.rs` must update that set or
  daemon routing is wrong. Not owned here.
- `packages/cowshed/src/types.ts` `CreateOptions`/`ExecRequest`/`RemoveOptions` are NAPI DTOs (camelCase fields,
  `create` vs CLI `new`, `rename` vs CLI `mv`). They do not parse CLI flags. Rust/TS DTO SSOT is another slice
  (`XcutRustTs`).
- `packages/cowshed/crates/cowshed-cli/src/skill/generated.rs` is harness install paths from vercel-labs/skills. It does
  not restate CLI command or flag names.
- `packages/cowshed/docs/cli.md:66` says workspace names are `[a-z0-9][a-z0-9-]*` with no 64 cap. Docs vs
  `WorkspaceName` / F9.

## Non-findings (checked, clean)

- Clap earns its weight: in-process matcher, error typing, `OsString` / `--` last args, no openssl-class transitive.
  Features already have `default-features = false`. `anstyle` is `clap_builder`'s dependency, not a local extra. Wrong
  "just shell out" recommendation.
- `output.rs` is a thin write adapter over `cowshed_core::api::JsonEnvelope`. No second envelope schema. JSON tests pin
  frozen wire bytes (`output.rs:163-203`); that is the contract, not a rendered-string smell.
- `run.rs` dispatch (help/version/skill/gateway/sccache/setup vs runtime host/project) is one place.
  `finish`/`write_error` is the one error-write path. Gateway git-helper bypass is explicit and does not parse CLI args.
- `main.rs` / `lib.rs` are wiring only.
- No `unsafe`. Production `expect` is `package_version` on embedded `package.json` (invariant). No TODO/FIXME in the
  slice.
- Copies (`os().cloned()`, `parse_args` collect, `edit_distance` Vecs, `CommandSpec::usage` String, clap `Command`
  rebuilt per call) are once-per-invocation. Regime: CLI startup, not a hot loop. Not findings (PERFORMANCE-HANDBOOK
  §4.1 / assignment rule).
- Help layout constants (`WIDTH`, `MAP_FLAG_BUDGET`, `MEANING_COLUMN`) live in `help.rs` only.
- `announce` vs `error` share a format string on purpose (quiet never applies); different contracts.
- `SkillCommand` is `Install` only; clap `skill` subcommand_required matches.
- Compared clap flags vs CommandSpec options for every verb: no live flag-name divergence today. The bug is the missing
  coupling (F1/F6), not a current mismatch.
