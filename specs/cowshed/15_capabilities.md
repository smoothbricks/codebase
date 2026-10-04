# Project capabilities

Cowshed's core owns sandboxing, copy-on-write workspaces, leases, the gateway and land. Project tooling is optional. A
plain Git repository needs neither `.envrc` nor Nix, and cowshed does not create, rewrite or require repository shell
hooks.

## Detection inputs

Detection reads convention files inside the workspace, never an enclosing checkout or the operator's shell environment.
Its input is the workspace mount, command cwd, host home, shared-cache root, private environment root, short runtime
directory, and optional public gateway trust bundle. Paths derive from those inputs; secrets and ambient PATH are not
detection inputs.

Project-scoped detectors inspect the workspace root, or the contained relative directory selected by that capability's
override. A tool invoked through a task runner gets the same cache and daemon authority as one invoked directly. There
is no recursive walk through dependencies or build trees. Direnv alone inspects command ancestors inside the workspace
and selects the nearest `.envrc`; an explicit directory override selects only that directory. Spawn admission refreshes
the convention snapshot, so adding or removing a convention takes effect without restarting the supervisor. An unchanged
snapshot reuses its rendered policy; a changed snapshot renders a matched sandbox/profile pair before applying job-mode
narrowing. Warm-host identity includes the resulting profile, environment and shell directory. At mint the same
detectors inspect the minted tree. A detector may examine file contents to disambiguate a convention; it never executes
project code during discovery. A convention that resolves outside the workspace is rejected, not followed. Missing files
mean absence; other filesystem errors report the path and failure.

| Detector | Convention                                                                     | Contribution                                                                                                        |
| -------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| direnv   | `.envrc`                                                                       | Contained shell activation, private approval state, bootstrap executable                                            |
| Nx       | `nx.json`                                                                      | Private workspace data/cache and short daemon socket namespace; discard inherited daemon records                    |
| cargo    | `Cargo.toml`                                                                   | Shared registry/git caches and cargo's exact cache-state files; Git fetch and trust settings                        |
| Go       | `go.mod` or `go.work`                                                          | Shared module/build caches; no generated GOENV or toolchain/proxy policy                                            |
| Bun      | `package.json` and `bun.lock` or `bun.lockb`                                   | Bun install cache and JavaScript trust/proxy settings                                                               |
| npm      | `package.json` and `package-lock.json` or `npm-shrinkwrap.json`                | npm content cache and JavaScript trust/proxy settings                                                               |
| pnpm     | `package.json` and `pnpm-lock.yaml`                                            | pnpm store and JavaScript trust/proxy settings                                                                      |
| uv       | `pyproject.toml` or `uv.lock`                                                  | uv cache and platform certificate opt-in                                                                            |
| Zig      | `build.zig`                                                                    | Zig global cache                                                                                                    |
| Gradle   | `settings.gradle`, `settings.gradle.kts`, `build.gradle` or `build.gradle.kts` | Gradle cache, not host credentials or configuration                                                                 |
| Nix      | `flake.nix` or `devenv.nix`                                                    | Nix client caches, immutable tool/store reads, canonical live daemon socket, TLS settings and bootstrap executables |
| sccache  | cargo convention and an installed host compiler-cache client                   | Compiler wrapper, exact host daemon socket and cache-client settings                                                |

A Nix/devenv convention does not activate a shell. Projects that want devenv activation use their own `.envrc` with
direnv's `use devenv`. There is no built-in devenv shell backend or `[devenv]` configuration. No detector recognizes a
repository name, a managed-repository marker or a project-specific compiler cache.

## One contribution contract

Each detector returns data through one `CapabilityContribution`:

- **Environment:** named actions `Own(value)`, `Default(value)`, `Append(line)` or `Unset`. Owned and unset variables
  cannot be changed by a caller overlay. Defaults preserve an explicit caller value; appended lines follow that value.
  Project activation still executes inside the child sandbox, not in the controller.
- **Filesystem grants:** exact paths or subtrees, with read or read/write access. Grants pass through the same
  protected-path validation as the core sandbox. Detection never grants host credentials, binaries' parent homes, a
  sibling workspace or cowshed controller state.
- **Cache mounts:** a shared cache source, optional private-environment link target, and any host-cache relocation
  descriptor. The same descriptor supplies environment, preparation and sandbox authority; no second table owns cache
  permissions. Only detected capabilities use their cache mounts. Without provisioned shared caches a tool uses private
  state.
- **Daemon isolation:** private directories to create and workspace-relative inherited state to discard at mint. The
  detector owns the convention-specific paths; the clone implementation only applies the returned paths.
- **Unix sockets:** canonical, individually admitted host service sockets. No wildcard socket grants or in-sandbox
  host-daemon fallback.
- **Bootstrap executables:** command-name-to-resolved-executable entries needed by a detected capability. Preparation
  installs exact private `.cowshed/tools/bin/<name>` links (read-only jobs use their exec-temp `tools/bin`) so an
  executable such as `npm-cli.js` remains reachable as `npm`. The link set is reconciled at preparation; removed
  capabilities leave no stale program links. The separate `.cowshed/bin` remains the workspace's shim directory. The
  core adds both bins and platform system directories; it does not borrow a workspace `.devenv/profile`, an entire login
  PATH or an entire Nix profile. A detector may resolve an individual bootstrap tool through its installation
  conventions. Store-resolved executables receive immutable Nix-store reads; that alone does not grant Nix caches or
  daemon access. The same read-only store grant follows any program link in the mode's `tools/bin` or the workspace's
  `.cowshed/bin` that targets the store, with or without a Nix project; a host with no store gets none.
- **HOME probes:** the sandboxed supervisor repeats detection beneath the HOME-wide read deny (04_sandbox.md), so every
  HOME path a detector inspects is also one it grants read: each bootstrap candidate beneath HOME as its literal program
  path, rustup's settings file, the compiler-cache GC root. The sandboxed search then sees what the host's saw. These
  grants are never HOME itself and never a directory listing; their ancestors beneath HOME are metadata-only.
- **Shell activation:** an optional contained direnv directory. Absence leaves the ordinary sandbox environment and
  supports argv and script jobs.

Host setup can relocate declared cache descriptors globally so host checkouts and clones retain identical cache path
spellings. Descriptor declarations live with their detectors; host preparation does not enable a capability in a
repository. Cargo's cache locks remain part of its detector's host relocation operation.

## Ordering and conflicts

The registry has a stable order: direnv, Nx, cargo, Go, Bun, npm, pnpm, uv, Zig, Gradle, Nix, sccache. Detection is
side-effect free. Contributions are merged before any directory is prepared or child is launched.

Cowshed core reserves HOME, XDG roots, TMPDIR, PATH, workspace token/port variables, gateway routing, isolated Git
identity and controller-owned Git configuration. A detector attempting to own a reserved variable fails. Two detectors
contributing different actions or values to the same variable fail with both capability names and the variable;
byte-identical contributions coalesce. Identical grants, mounts, bootstrap entries and isolation paths coalesce. A
read/write subtree subsumes a read grant only within the same validated path. Conflicting link targets fail rather than
depending on order. Capability grants never override immutable denies, and read-only jobs keep the existing narrowing
rules.

## Explicit overrides

`.cowshed.toml` retains storage and land configuration. `[capabilities.<detector>]` may set `disabled = true` or
`directory = "relative/project"`. A directory is workspace-relative, normalized, contained, and inspected for that
detector's convention. An override never activates an absent convention and never substitutes repository identity for
detection. Unknown capability names, keys and invalid paths fail explicitly. No configuration is required for the
conventions above or for a plain repository.

Directory overrides also root repository-relative contribution paths at that directory: for example Nx's workspace root
and inherited `.nx` rendezvous directory follow the selected Nx directory. Private environment state remains scoped to
the whole sandbox. Credential and controller-state denies remain unconditional core policy even when the corresponding
tool capability is absent.

## Required evidence

Every detector has a filesystem-backed test proving its convention enables its contributions and removing that
convention removes them. Composite tests cover conflicts, containment, absent shared caches and overrides that cannot
enable missing conventions. The generic-repository integration exercises adopt, new, exec and land in a plain Git
repository without `.envrc`, Nix conventions or managed-repository files. Release dogfood runs the installed release
build against distinct adopted repositories; host setup is broadcast before restarting the gateway and never raises
unattended authorization prompts.
