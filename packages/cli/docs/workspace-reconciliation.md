# Workspace reconciliation: implemented foundation and proposed architecture

Status: the managed-file planning refactor is implemented. The update coordinator, secret lifecycle schema,
registry agent, and expanded Nx adapters below are proposals, not supported new commands or configuration.
Reviewed against `main` at `3a9f444a82887aaa9fb20c2f9148c505b107fed9` on 2026-09-11.

## The architectural boundary

Smoothbricks should reconcile a declared workspace, not accumulate procedures for reproducing one particular
machine. Keep policy in functions over explicit values. Keep filesystem access, package installation, Nix
evaluation, credential providers, subprocesses, and network calls in adapters with bounded operations.

The CLI already has useful pieces: typed CI/publish step definitions, source-derived deployment targets,
separate build/deployment tasks, lazy secret groups, Cargo workspace compilation, nextest archives, and extensive
regression tests. Preserve these. A new generic YAML framework or replacement Rust plugin is not the starting point.

The desired pipeline is:

```
read configuration + manifests + graph + installed release metadata
                      |
                WorkspaceFacts
                      |
         pure normalization and validation
                      |
       file plans / task plans / credential requirements
                      |
          preview or explicitly apply effects
```

File synchronization and remote secret provisioning must not share one undifferentiated apply operation.
A file update must not rotate credentials or deploy a Worker as a side effect.

## Implemented in this change

`monorepo/managed-content.ts` owns local-section grammar. `managed-plan.ts` computes desired file writes and
conflicts from snapshots. `managed-fs.ts` captures filesystem state and applies a successful plan. The existing
`managed-files.ts` still discovers repository facts and renders its descriptors, but renders every applicable
file before calling the filesystem shell.

The planner is independent of filesystem, process, environment, Nx, and credential APIs. Check, diff, and update
use the same decisions. A content or path conflict invalidates the entire planned write set. The shell uses
`lstat`, validates in-workspace source links, refuses broken/out-of-workspace links and symlinked parents, and
repairs the owner-execute bit even when bytes already match. Local tails and ordered inline blocks survive an
update. Orphaned/nested markers and lost/ambiguous anchors produce diagnostics rather than silent data loss.

Each file replacement uses a temporary sibling and rename. This is **not** a multi-file transaction: an I/O
failure during application can leave a completed prefix. It is not a defense against a hostile process racing
parent-directory changes. Re-running reconciles completed changes. A future workspace transaction needs a
journal, expected-before fingerprints, and an explicit concurrency policy.

Disabled descriptors remain untouched. This patch does not authorize deleting obsolete workflows. Safe removal
needs provenance: the previous generated fingerprint and explicit ownership, plus a conflict when an obsolete
file contains repository changes. Unknown files must never become deletion candidates simply because a template
list changed.

## One upgrade contract, two operations

The current update path renders managed files first and installs dependencies last. Consumer tool policy does
look up the published CLI, but the running process is still the old generator. The plugin is governed by its own
dependency policy; neither independently resolving versions nor installing a newer CLI after rendering proves a
coherent upgrade. Runtime synchronization can also inspect an already-realized profile from before the Nix edits.

Proposed public distinction:

- `smoo monorepo sync`: reconcile the currently installed, pinned toolset; deterministic and offline.
- `smoo monorepo update`: resolve an upgrade, install the compatible toolset, and synchronize using that new toolset.

An update should resolve a release manifest once. It records compatible CLI/plugin versions, tested Nx ranges,
configuration schema version, template version, and ordered migrations. CLI and plugin may have different package
versions; compatibility is the pair, not an assumption that their semvers match.

Stage the selected package and toolchain changes, install them, and invoke the newly installed CLI through its
explicit resolved path. Do not continue generation in the old process or rediscover an older global binary through
PATH. Then read the graph with the new plugin, render, validate, and check the fixed point before applying the
reviewed workspace changes. Preserve `workspace:` source development in Smoothbricks itself. Never replace linked
source development with registry packages implicitly.

A staging worktree is useful, but cannot be the only supported mechanism: dirty downstream workspaces need a
well-defined snapshot and conflict policy. Do not discard user edits. Record the exact selected versions before
network operations and support resuming a failed install without resolving a different release halfway through.

Acceptance fixtures must include an old published CLI upgrading a downstream repository, distinct compatible CLI
and plugin versions, multi-version migrations, a linked CLI, dirty local extensions, failed installation, and a
second synchronization with no changes. Run fixtures from published tarballs, not only workspace source imports.

## Devenv and direnv

Treat four things separately: the devenv executable, declared Nix inputs, locked input revisions, and managed module
content. A new module that references a new input must migrate `devenv.yaml` with it; copying the module alone is
not a valid upgrade. Use explicit lock updates, not an implicit lock refresh on every shell activation.

Evaluate the candidate environment before deriving Bun/Node/type-package pins from it. A previous
`.devenv/profile/bin` is not evidence that new Nix declarations resolved successfully. Retain the existing
repository-owned `devenv.nix` import boundary and use Nix options for customization rather than regex editing of
arbitrary Nix expressions.

Shell entry should establish the pinned tools and useful diagnostics without prompting for credentials unrelated
to the requested operation. It must not publish, rotate secrets, or silently rewrite lockfiles. A missing private
dependency prevents the operation that needs it, not entry into a shell capable of repairing it. CI installation
stays frozen; diagnostic unfrozen installation must not turn a frozen-install failure into a successful CI job.

Tests should cover clean Linux and macOS activation, a new Nix input, stale profiles, changed lock inputs,
noninteractive/no-provider operation, and the bootstrap-before-node_modules path. The current native TypeScript
bootstrap also needs its package-alias repair tested in both developer and CI branches.

## Nx generators are adapters, not a second policy engine

Use an Nx global sync generator for root managed configuration, and task sync generators for project-reference
files required by particular targets. Nx already supplies `nx sync` and `nx sync:check`. A generator should adapt a
Tree to the same pure plan, not independently rediscover policy or shell out to `smoo monorepo update`.

Do not create a dependency cycle: the CLI already depends on the Nx plugin. Shared policy must live below both
adapters, in a dependency-free module/package with one schema and a stable public boundary. Bootstrap code needed
before installation should be generated/bundled from that source, not maintained as a hand-copied twin.

Use `createTreeWithEmptyWorkspace` fixtures for file generation, preservation, migration and idempotence. Keep real
filesystem tests as well: a Tree does not prove symlink, permission, process, or crash behavior. Avoid graph
construction that recursively requires the same synchronization to complete. Graph inference must not fetch
secrets, install packages, or mutate the workspace.

## CI and publishing: strengthen the existing definitions

Both workflow generators already have typed step definitions. Extend that design into an explicit job dependency
and capability model rather than introducing another generic serialization layer. Derive prerequisites,
permissions, credential requirements, artifact consumers, and platform coverage from that model, then serialize
and validate the YAML.

Required invariants include a resolving `needs` edge for every artifact dependency, secret-free untrusted
validation, no deployment on an untrusted event, a protected publisher receiving only its own credentials, and a
release artifact manifest identifying source, versions, platform, configuration, and integrity. Preserve the
existing frozen install, fork-runner gate, prebuilt artifact validation, separate publishing job, and uncached
live-deployment targets.

Pass workflow inputs through `env`, then use quoted shell variables. The current publish workflow interpolates
`inputs.projects` inside shell source; quoting the expression in YAML does not make the expanded value inert shell
data. Keep dispatch input validation as an additional boundary, not as the only protection.

Reduce broad job/workflow token permissions to the operations that need them. Separate drift-healing dispatch
from ordinary validation. Keep self-healing as a convenience, but make a read-only, authoritative drift check part
of the relevant PR gate. Do not require a fresh network upgrade merely to check committed files.

Release lint/tests currently precede version mutation, while builds follow it. The comments and the actual
contract should agree. It can be valid to reuse a version-insensitive source check, but version-sensitive runtime
checks and shipped artifacts must describe the candidate being published. The independent macOS job also versions
before building; compare semantic release manifests rather than assuming its git commit timestamp must equal the
Linux producer's. Derive which matrix leg can execute native tests from the selected runner architecture rather
than hard-coding arm64 for a configurable runner.

Historical release repair currently needs a real build toolchain in the publisher. Isolate repair into an explicit
recovery path before trying to make the ordinary publisher minimal. Otherwise a superficially lean environment
removes tools needed for recovery or artifact validation.

Test parsed workflow structure and policy invariants in addition to snapshots and string assertions. Run
`actionlint`, shell checks, and a small representative fixture matrix: public TS-only, Rust workspace, private
registry consumer, deploy-only, multi-platform release, and the supported Actions providers. Never update a
snapshot merely to make a failing workflow test green.

## Declarative secrets: sources, provisioning, and destinations

The current `smoo.secrets` command/group map and status/run/set/sync facilities are useful foundations. The missing
abstraction is a complete lifecycle declaration: the logical secret, how to resolve it, how it may be provisioned,
which operations consume it, and the explicitly authorized destinations. Do not discover the intended source of
truth by reverse-parsing generated workflow YAML.

Evaluate SecretSpec for runtime resolution first: devenv already integrates it and recommends process-scoped
runtime loading. Smoothbricks should add lifecycle/distribution policy and adapters where necessary, not implement
a competing provider ecosystem without a demonstrated gap. A normalized catalog can generate SecretSpec routes
and non-secret example files without duplicating provider/profile decisions in two authored configurations.

Illustrative future schema, **not accepted by the current CLI**:

```json
{
  "smoo": {
    "secrets": {
      "SESSION_SIGNING_KEY": {
        "description": "Signs application sessions; rotate with an overlap window.",
        "group": "runtime",
        "source": {
          "kind": "1password",
          "reference": "op://Development/example-api/session-signing-key"
        },
        "provision": { "kind": "random", "bytes": 32, "encoding": "base64" },
        "targets": [
          { "kind": "github", "environment": "staging", "name": "SESSION_SIGNING_KEY" },
          {
            "kind": "wrangler",
            "config": "apps/api/wrangler.toml",
            "environment": "staging",
            "name": "SESSION_SIGNING_KEY",
            "mode": "version-only"
          }
        ]
      }
    }
  }
}
```

Support environment, 1Password reference, sops document/key, and argv-command sources. Support explicit
1Password storage/provisioning, GitHub repository/environment/organization targets, Wrangler targets, encrypted
sops document updates, and bounded custom-command sinks. Distinguish source identity from runtime variable names
and destination bindings. Provider-access credentials are bootstrap credentials, not copies automatically fanned
out to the destinations of the secrets they unlock.

Provisioning may generate an application-owned random key. It cannot invent a registry-issued access token; that
needs a setup template naming the issuing service, necessary permissions, storage location, and verification.
Generate documentation and empty example files from the catalog, never resolved values.

A names-only plan precedes an explicit apply. Resolve or generate once, persist to the canonical source, record
its stable identity/revision, then distribute the same value. Resume after a destination failure without
regenerating the source. Existing keys must not be rotated on shell reload, install, or monorepo update. Rotation
must account for consumers needing an overlap period.

GitHub and Worker secret stores do not provide plaintext comparison. Report existence and applied revision, not
"values match" based on an unavailable read. Do not put hashes of low-entropy secret values into public receipts.
Use provider IDs and non-secret revision metadata. Failure messages must not print provider argv, stdout, stderr,
URLs with credentials, or child environment values. The current local-secret helper still includes declared argv
in some errors; consolidate command execution behind a redacted, timeout-bounded adapter.

1Password supports JSON templates on stdin. Wrangler bulk input also supports stdin, but ordinary `wrangler secret`
operations can deploy immediately: separate creating a secret-bearing version from promoting that version. A
secret synchronization command must not accidentally become a production deployment. For sops, commit ciphertext
only, preserve recipients and document metadata, and keep plaintext out of intermediate files.

## Private npm: absence must not imply global failure

The added executable tests establish distinct Bun behaviors: a configured but unused private scope does
not require a token even while a public dependency is downloaded; a required uncached private dependency fails authentication; an already-installed frozen
workspace can install again without credentials or requests. These tests use loopback registries and fake tokens,
not real providers. They do not promise the same result for every registry, Bun version, optional dependency, or
lifecycle script.

The existing resolver restricts the private registry path to a Forgejo npm owner endpoint. Separate generic npm
read/authentication configuration from provider-specific publishing and durable-state APIs. Support multiple
scopes without relaxing the existing rule that a private package must never fall back to public publication.

Prefer a process-scoped install path first: the existing `smoo secrets run registry bun install` is an operation
boundary to improve. An optional `smoo npm` session agent should only be added with explicit unlock, status, lock,
TTL, and noninteractive behavior. The names here are proposed, not implemented by this patch.

A credentials-only agent can avoid repeated 1Password prompts, but passing its token into Bun also exposes it to
child processes and lifecycle scripts. A read-only registry proxy keeps the upstream token out of those process
environments, but is not the same security model as an SSH signing agent: authorized local code can still download
private packages while it is unlocked, and already-downloaded artifacts remain accessible after locking.

A proxy acceptance contract needs per-user socket/directory permissions, peer checks for its control interface,
loopback-only binding, authenticated local requests, explicit trusted registries and path prefixes, GET/HEAD-only
routing, redirect revalidation, and no forwarding upstream Authorization across origins. Do not expose an open
proxy, CONNECT endpoint, provider-command executor, or generic credential retrieval endpoint. Approving a
workspace must not implicitly approve arbitrary provider commands from another checkout.

Preserve registry metadata, tarball integrity, and canonical lockfile identity. An ephemeral port or capability URL
must not leak into a committed Bun lockfile. Tarball/CDN requests and redirects need the same coverage as package
metadata; merely proxying the initial manifest is insufficient. Fail promptly while locked, do not prompt from CI
or direnv, and never mask a required dependency's absence as successful installation.

Tests must cover missing/expired tokens, concurrent unlocks, TTL expiry, redirects, malicious hosts/paths,
metadata/tarball integrity, frozen-lock portability, source registry changes, daemon restarts, parallel workspaces,
secret-free logs, and the lifecycle-script threat model. Benchmark the wrapper before deciding that a daemon is
necessary for every developer.

## Nx: infer capabilities, preserve build semantics

Extend the existing shared target policies with semantic metadata for artifact production, source validation,
test compilation, test execution, deployment preparation, and live deployment. The CLI should consume those
capabilities rather than repeatedly infer meaning from target suffixes. Keep provider-specific executor options
local to their adapter and make user overrides explicit.

TypeScript production typechecking, test typechecking, test execution, and JavaScript emission are different
operations. Keep their inputs and cache results separate. Some tests intentionally typecheck through built package
exports, so eliminating every declaration/build prerequisite would be incorrect. Test the project-reference graph
with source imports and distribution exports, and invalidate typechecks for changed tsconfigs, transformers, and
compiler versions. Avoid invalidating all source checks for an unrelated workflow-comment edit: the current root
`sharedGlobals` includes the CI YAML itself.

The plugin deliberately disables versionless JSON hashing when the loaded Nx is older than its supported JSON
input feature. That guarded fallback is not an unconditional incompatibility. Still publish and test a coherent
Nx/tooling compatibility range instead of independently floating several Nx packages and relying on internal APIs.

## Rust: improve the existing model before adopting a replacement

Keep the current shared Cargo workspace compile/archive phase and per-package nextest execution. Replacing these
with independently compiled crates or a target directory per Nx task can multiply compilation and defeat shared
feature resolution. Preserve the nightly workspace feature-unification policy and the stable cargo-hakari route
until measurements justify a change.

Use Cargo's normalized metadata to inform crate identities, renamed/path dependencies, workspace inheritance,
normal/build/dev dependencies, and target-specific edges. `cargo metadata --no-deps` does not provide a full resolve
graph; do not mistake it for one. Obtain a full locked/offline graph when dependencies are available, or use an
explicitly limited manifest-derived workspace graph. Graph discovery must not opportunistically fetch private
sources or drop missing edges silently.

Replace machine-specific absolute external-crate paths with declared checkout identities and derived content
inputs. Do not simply remove the current hashes: that would turn a portability problem into unsound caching.
Track build-script inputs, included data, C/C++ toolchain/SDK configuration, rustc, features, profile, target triple,
lockfiles, and relevant environment settings. Never exclude a version from the cache identity of a binary that
embeds `CARGO_PKG_VERSION`.

Distinguish Cargo fetch, check/clippy, test compilation, nextest execution, doctests, cross compilation, and
native-host execution. Keep shared target directories scoped by compatible toolchain/target/profile/features,
coordinate concurrent writers, and avoid restoring overlapping mutable directories from independent Nx tasks.
Compiler caching is an optional measured layer, not a substitute for correct Nx inputs.

Assess `@monodon/rust` and `@nxrs/cargo` in compatibility fixtures. At review time Monodon's source manifest depends
on `@nx/devkit` 22 while this workspace uses the Nx 23 family; the inspected nxrs README labels the project WIP.
Those facts do not prove either is unusable, but they do not establish a drop-in replacement for this workspace's
cross-platform release and deployment policy either. Borrow metadata/executor/generator patterns first and retain
license attribution when copying implementation.

Measure cold build, no-change rebuild, leaf-crate edit, common-crate edit, version-only release, feature change, and
cross-target build. Record task counts, compiler invocations, cache hits, lock contention, disk use, and elapsed
time on the same pinned runners. A faster result with stale output is a failed experiment.

## Validation performed for this patch

58 focused tests passed through Nx in an isolated validation workspace, including the existing ownership tests
moved beside the pure module, new planner/filesystem regressions, property tests, and four real Bun install
contracts. The fixture used Bun 1.4.2 and Node 26.8.2 from a repository CI artifact, with the repository's installed
dependencies. Lint preceded tests. The changed integration module and its tests also passed a native TypeScript
check against the repository source imports.

This is not a passing full `cli` or `nx-plugin` suite. The normal native Typia/ttsc plugin bootstrap requires Go
modules unavailable in the network-isolated execution environment. No real Nix shell, remote secret provider,
private registry, deployment, or publication was exercised. Workflow rendering was not changed in this patch.

## Primary references

- [Nx sync generators](https://nx.dev/docs/kb/create-sync-generator)
- [Devenv input pinning](https://devenv.sh/pinning/)
- [Devenv SecretSpec integration](https://devenv.sh/integrations/secretspec/)
- [Bun npmrc support](https://bun.com/docs/pm/npmrc)
- [Cargo metadata](https://doc.rust-lang.org/cargo/commands/cargo-metadata.html)
- [1Password item templates and stdin](https://developer.1password.com/docs/cli/item-create/)
- [Wrangler Worker and secret commands](https://developers.cloudflare.com/workers/wrangler/commands/workers/)
- [SOPS documentation](https://getsops.io/docs/)
- [Monodon Rust package](https://github.com/cammisuli/monodon/tree/main/packages/rust)
- [nxrs Cargo plugin](https://github.com/nxrs/cargo)
