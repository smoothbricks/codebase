# Managed by `smoo monorepo`. Repo-owned configuration belongs in devenv.nix,
# which imports this module:
#
#   imports = [./devenv.smoo.nix];
#
# This is the SmoothBricks shell contract — the wiring that pairs with the other
# managed files (tooling/direnv/repo-path, setup-environment.ts, the CI actions)
# and must stay identical across repositories. `enterShell` is `types.lines`, so
# this module's prologue and epilogue merge around whatever devenv.nix adds:
# mkBefore establishes the workspace root, PATH, and toolchain before any
# project step runs, and mkAfter restores the caller's directory last.
#
# Every explanation lives out here in Nix comments. Hook bodies are exported as
# shell text, so a comment inside one becomes part of an environment variable.
{
  config,
  inputs,
  lib,
  pkgs,
  ...
}: let
  # ttsc's native-plugin patch compiles microsoft/typescript-go with one
  # corrected file. `go list -m` only fills `.Dir` after the module is already
  # in GOMODCACHE, which CI does not restore (and GOPROXY is often off under
  # nix). Pin the commit ttsc 0.30.4's patch requires so the tree is in the
  # devenv closure and TTSC_TYPESCRIPT_GO_DIR names it.
  typescriptGo = pkgs.fetchFromGitHub {
    owner = "microsoft";
    repo = "typescript-go";
    rev = "56ab4af42157";
    hash = "sha256-ebjX+1+9axEqsnZXCRZSumA38gPNdmoz98tMt2oftN4=";
  };

  # A repository that enables uv under languages.python gets its uv workspace
  # synced and activated by this contract (enterShell item 5), never by
  # devenv's own python tasks.
  python = config.languages.python;
  uvProject = python.enable && python.uv.enable;
in {
  env = lib.mkMerge [
    {
      # Nx otherwise defaults to three workers. Scale to the cores available in each
      # developer shell or CI runner; explicit --parallel flags still take precedence.
      NX_PARALLEL = "100%";
      # Every Nx daemon, when it starts, installs nx@latest from the registry
      # into a temporary directory to ask the newest release whether to prompt
      # for Nx Console and whether AI agent configuration is outdated, and
      # `nx configure-ai-agents` and `nx init` do the same. NX_USE_LOCAL answers
      # from the installed nx, so starting Nx installs nothing from the registry.
      # (A daemon still runs `git ls-remote` against GitHub's
      # nrwl/nx-ai-agents-config when an agent has both rules and Nx's MCP
      # configured; a cowshed sandbox runs Nx without a daemon.) It also makes
      # `nx migrate` run with the installed CLI instead of the newest one;
      # `NX_USE_LOCAL=false nx migrate` restores that for the one command.
      NX_USE_LOCAL = "true";
      TTSC_TYPESCRIPT_GO_DIR = "${typescriptGo}";
    }
    # Playwright's downloaded Ubuntu browser has no runtime closure on NixOS
    # (CI reached the executable, then failed loading libglib-2.0.so.0). Use
    # the lock-pinned Nix browser and its libraries; smoo passes this executable
    # to the child before Playwright imports. Darwin keeps its native browser path.
    (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
      PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH = lib.getExe pkgs.chromium;
    })
  ];
  # sccache is deliberately absent from this shell. It used to export
  # RUSTC_WRAPPER=sccache as a BARE NAME, which PATH then resolved per project —
  # which is precisely how an unpatched binary from one profile came to serve
  # clients from another. sccache now belongs to cowshed: `cowshed setup
  # --sccache` builds packages/cowshed/nix/sccache, pins the result with a nix GC
  # root, and supervises that exact store path. A repository shell has no
  # business deciding which compiler cache a host runs.

  # One toolchain for every repository and every workspace, pinned by devenv.lock
  # rather than by a rust-toolchain file. devenv resolves the channel through
  # rust-overlay, so a lock bump is the only thing that moves the compiler, and a
  # CI runner building this same shell gets the same rustc a laptop has.
  #
  # Nightly because the fleet needs `-Z` features (`-Zbuild-std`,
  # `panic=immediate-abort` for wasm artifacts) and there is no reason to keep a
  # second toolchain alongside for them. `version = "latest"` reads "newest
  # nightly the locked rust-overlay offers", not "whatever nightly exists today".
  #
  # `components` and fleet-wide `targets` belong to this module; repositories
  # add only platform- or product-specific targets in devenv.nix. These list
  # options merge, while `channel` and `version` are single-valued. A
  # `rust-toolchain.toml` is NOT the
  # mechanism here: nix cargo ignores it unless devenv is pointed at it with
  # `languages.rust.toolchainFile`, so such a file is decoration that silently
  # disagrees with the shell.
  languages.rust = {
    enable = true;
    channel = "nightly";
    version = "latest";
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
      "rust-src"
    ];
    # Every repository builds WASM, so the shared shell always carries that std.
    # Keeping it here prevents each repo from restating the same target.
    # CI validates on Linux, so a macOS shell carries Linux's std and can
    # type-check the arm CI compiles. rust-std alone reaches every crate whose
    # dependency graph is pure Rust; a dependency that compiles C for the target
    # — ring, openssl-sys, libgit2-sys, libz-sys — additionally needs a cross C
    # compiler, which is 0.4 GiB and so lives in the opt-in `linux-cross` profile
    # below rather than here, keeping it off every macOS shell and macOS runner.
    #
    # The reverse direction is not available at any price: type-checking an Apple
    # target from Linux needs an Apple SDK for those same C-building
    # dependencies, which is a licensing boundary rather than a missing package.
    targets = [
      "wasm32-unknown-unknown"
      "x86_64-unknown-linux-gnu"
    ];
  };

  # Opt-in Linux cross toolchain, activated by `devenv -P linux-cross` and driven
  # by the root `check:linux` script. devenv 2.2.3 has first-class profiles, so
  # this is one gated module in the shared file rather than a second config
  # directory that would have to re-import and re-pin everything here.
  #
  # It is a profile and NOT a default package because pkgsCross.gnu64's cc
  # closure is 0.4 GiB against a default shell closure of ~5.4 GiB — a 7% tax on
  # every shell entry and every CI cache restore, to serve one command that a
  # macOS laptop runs deliberately. Nothing on Darwin links a Linux object.
  #
  # Why a C compiler is needed at all, when rust-std above is already installed:
  # ring, openssl-sys, libgit2-sys and libz-sys compile C for the target from
  # their build scripts, and build scripts are compiled and run even under
  # `cargo check`, so std alone stops at `ToolNotFound: failed to find tool
  # "x86_64-linux-gnu-gcc"`. The `cc` crate probes triple-prefixed tool names,
  # which is precisely what this wrapper's bin/ exports.
  #
  # Each tool path is derived from the wrapper's own `targetPrefix` instead of
  # being written out, so a nixpkgs bump that renames the prefix carries these
  # with it rather than leaving four stale strings that fail at build-script time.
  #
  # This profile supplies the toolchain and nothing else. CARGO_BUILD_TARGET is
  # deliberately NOT set: the triple belongs to the Nx target that asks for it
  # (`cargo-lint-x64-linux` passes `--target` explicitly), not to the ambient
  # environment. An ambient triple would make the check silently host-local
  # whenever the profile was forgotten — reporting green having compiled macOS —
  # and that false green is the precise failure this whole profile exists to end.
  # With the flag on the target instead, running it outside this profile fails
  # loudly at `ToolNotFound` and a host lint can never be mistaken for a cross one.
  profiles.linux-cross.module = let
    crossCC = pkgs.pkgsCross.gnu64.stdenv.cc;
    crossLibc = lib.getDev crossCC.libc;
    tool = name: "${crossCC}/bin/${crossCC.targetPrefix}${name}";
  in {
    packages = [crossCC];
    env = {
      CC_x86_64_unknown_linux_gnu = tool "cc";
      CXX_x86_64_unknown_linux_gnu = tool "c++";
      AR_x86_64_unknown_linux_gnu = tool "ar";
      CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = tool "cc";
      # bindgen runs host libclang, which cannot infer the cross compiler's headers.
      BINDGEN_EXTRA_CLANG_ARGS_x86_64_unknown_linux_gnu = "--target=x86_64-unknown-linux-gnu -isystem ${crossLibc}/include -isystem ${pkgs.pkgsCross.gnu64.linuxHeaders}/include";
    };
  };

  # linux-cross is the only profile here, and the shape of the publish job is
  # why. A lean publish shell was measured and rejected, so the next reader does
  # not have to measure it again.
  #
  # The ceiling first: dropping rustc, Go, binaryen, the cargo helpers, python
  # and the clang/lldb tools leaves 2.21 GiB of a 5.41 GiB default closure
  # (`nix path-info -S` over the devenv profile, aarch64-darwin), and modules
  # only ADD — so collecting that saving means moving the toolchain OUT of base
  # and making every developer shell and every other CI job pass `-P`. A
  # forgotten flag then yields a shell that is quietly missing a compiler, which
  # is the same false green the paragraph above exists to prevent.
  #
  # Repair is what makes it unfixable rather than merely awkward. `smoo release
  # repair-pending` builds the historical release commits npm is missing, and it
  # gets their toolchain by running a plain `devenv shell` AT that checkout
  # (packages/cli/src/lib/devenv.ts). No flag is passed and none can be: devenv
  # throws for a profile the checked-out commit does not define — `Profile 'x'
  # not found. Available profiles: ...` — which is every release commit older
  # than the split. Leave the flag off and a post-split commit hands repair a
  # base without rustc; add it and every pre-split commit fails to evaluate.
  # Either way the break surfaces months later, in the one command whose whole
  # job is to recover a release that already went wrong.
  #
  # The publish job's OWN environment is not just publishing either: `smoo
  # release publish --prebuilt` runs each project's Nx `release-check` gate over
  # the merged artifacts, which is where a native or wasm artifact is validated
  # before it is packed. A lean publish shell removes those tools silently, from
  # the one step that exists to refuse a bad artifact.
  #
  # And it would not even pay: the Actions nix segment is keyed on
  # os+arch+hash(devenv.yaml, devenv.nix, devenv.lock) and is blind to the
  # profile, so a restore ships the whole NAR regardless, while the save exports
  # the closure that job realized. A lean publish job that won that immutable
  # key would hand the CI workflow a NAR with no toolchain in it. On any run
  # where repair has work the saving is zero anyway, because repair realizes the
  # historical default shell inside the same job.

  # One Go for every repository, same reasoning as the Rust toolchain: a compiler
  # is part of a cache key, so two of them mean two caches.
  #
  # Pinned to the patch release ttsc vendors inside its native package, because
  # that SDK is not overridable — ttsc exposes cache locations and
  # TTSC_TSGO_BINARY, never the Go it builds plugins with — so matching it is the
  # only way to have ONE Go rather than ours plus a dependency's. `smoo monorepo
  # check` enforces the match and fails with both versions when they diverge,
  # which is what makes this pin a maintained invariant instead of a comment that
  # rots.
  #
  # It needs its own input because the fleet's nixpkgs does not carry that patch:
  # devenv-nixpkgs rolling ships go 1.26.5 and its `go_1_27` is a release
  # candidate, while ttsc 0.28.3 vendors go1.26.7. Same idiom, and same reason,
  # for any single-package pin — one leaf package from a nixpkgs that has it,
  # lock-pinned, prebuilt, rather than a fleet-wide bump that would move rustc,
  # bun and node too. Not every pairing is reachable: ttsc 0.28.1/0.28.2 vendor
  # go1.26.6, which NO channel packages, so when no revision offers the vendored
  # patch the ttsc pin is what moves.
  #
  # The ownership boundary this pin sits inside, unchanged by it: our code is
  # built with the Go devenv provides, a dependency may vendor its own SDK, and
  # neither may leak GOROOT into the other. Go therefore arrives via `packages`
  # and NOT `languages.go.enable`, which would export GOROOT; on PATH only, each
  # toolchain resolves its own GOROOT from its own binary. With the versions
  # matched that crossing cannot misfire at all, so the unset in enterShell is
  # belt-and-braces rather than the fix.
  # Node tracks the newest AWS Lambda managed runtime, currently nodejs26.x. It
  # lives here rather than in each repository's devenv.nix because it is a
  # fleet-wide fact: declared in three places, the next bump has to find all
  # three, and the one it misses re-splits the fleet.
  #
  # An explicit major, NOT `nodejs_latest`. A tool that participates in cache keys
  # and native-addon ABI must not be spelled "whatever is newest" in one
  # repository and a fixed major in the others — and the two spellings agreeing
  # today is precisely what makes the drift invisible, because the fleet looks
  # unified right up to the lock bump that splits it. A floating attribute is a
  # version that changes without a commit, so it cannot be reviewed or bisected.
  #
  # 26 also closes a split this axis caused once: 24.0.x declares URLPattern in a
  # way that TS2403-conflicts with lib.dom in consumers emitting declarations,
  # which forced a ~24.13.0 floor on one side; 26 is DOM-compatible.
  #
  # `@types/node`, `engines.node` and `packageManager` are derived from this pin
  # rather than maintained beside it — smoo's syncRootRuntimeVersions reads the
  # shell's Node major, so this line moves them.
  #
  # Reading which version this resolves to: devenv.lock has TWO nixpkgs nodes and
  # the obvious one is a decoy. `.nodes.nixpkgs` is not what `pkgs` is; the
  # authoritative path is `.nodes.root.inputs.nixpkgs`, which indirects to
  # `nixpkgs_2`.
  packages =
    [
      (import inputs.nixpkgs-go {inherit (pkgs.stdenv.hostPlatform) system;}).go
      pkgs.nodejs_26
      pkgs.binaryen
      # Test runner for inferred Cargo test targets. Generated targets must never
      # depend on an ambient host installation that CI does not reproduce.
      pkgs.cargo-nextest
      # Target-dir GC for the inferred cargo-sweep target: cargo never removes
      # superseded artifacts on its own (a busy workspace accumulated ~18k stale
      # variants per crate and 26 GB of junk before this existed), and a sweep
      # prunes them without touching the warm current-fingerprint surface.
      pkgs.cargo-sweep
      # The stable-toolchain arm of smoo's workspace feature-unification policy:
      # `cargo hakari` generates and wires the workspace-hack crate that unifies
      # features when `[resolver] feature-unification` — nightly-only, and what
      # the channel above selects — is unavailable. `smoo monorepo validate` runs
      # `cargo hakari verify` for a workspace that took that route.
      pkgs.cargo-hakari
    ]
    ++ lib.optionals pkgs.stdenv.hostPlatform.isLinux [pkgs.chromium];

  # devenv wraps the Python interpreter to find native libraries in
  # `languages.python.libraries`, whose default is this checkout's
  # `.devenv/profile`. That path then lands in the interpreter's store path, the
  # profile that holds it and the uv environment built with it, so every clone
  # evaluates and builds its own interpreter and profile on first entry, and its
  # copied uv environment names another checkout's interpreter. Cleared, the
  # interpreter is one store path in every checkout. devenv still adds the C++
  # runtime; a wheel that needs another library names that library here.
  languages.python.libraries = lib.mkIf uvProject (lib.mkDefault []);

  # The stdenv build variables the shell derivation leaks beyond the ones devenv
  # already drops: generic names (`name`, `system`) that collide with any
  # script's own, and builder-only settings (NIX_CFLAGS_COMPILE,
  # SOURCE_DATE_EPOCH, the sandbox profiles) that change what a compiler run
  # from the shell produces. The shell is a place to run tools, not a builder.
  # mkOptionDefault: devenv's own default list stays, this one joins it.
  unsetEnvVars = lib.mkOptionDefault [
    "CONFIG_SHELL"
    "DETERMINISTIC_BUILD"
    "IN_NIX_SHELL"
    "MACOSX_DEPLOYMENT_TARGET"
    "NIX_CFLAGS_COMPILE"
    "NIX_COREFOUNDATION_RPATH"
    "NIX_DONT_SET_RPATH"
    "NIX_DONT_SET_RPATH_FOR_BUILD"
    "NIX_ENFORCE_NO_NATIVE"
    "NIX_IGNORE_LD_THROUGH_GCC"
    "NIX_INDENT_MAKE"
    "NIX_NO_SELF_RPATH"
    "NIX_STORE"
    "PATH_LOCALE"
    "SOURCE_DATE_EPOCH"
    "__darwinAllowLocalNetworking"
    "__impureHostDeps"
    "__propagatedImpureHostDeps"
    "__propagatedSandboxProfile"
    "__sandboxProfile"
    "cmakeFlags"
    "configureFlags"
    "mesonFlags"
    "name"
    "system"
  ];

  enterShell = lib.mkMerge [
    # Prologue, in order:
    #
    # 1. The devenv wrapper runs from tooling/direnv; every later step expects the
    #    workspace root.
    # 2. PATH order is most-specific → least-specific, the same list the git hooks
    #    use, so a hook and a shell resolve one binary the same way.
    # 3. ttsc drives the native TypeScript 7 binary while Nx imports the TypeScript
    #    6 API, so the two must be named separately.
    # 4. Go and ttsc caches: tooling/direnv/shared-caches.sh places them under
    #    the machine's shared caches root when it exists and states why.
    # 5. The shared setup-environment.ts bootstraps repository dependencies. It
    #    runs `bun install` — and, for a uv project, `uv sync` — only when that
    #    installer's inputs changed since its last successful run, so an
    #    unchanged checkout enters in the time it takes to hash a lockfile. It
    #    records those inputs, and the managed .envrc watches them, so a shell
    #    that direnv keeps loaded re-enters exactly when one of them changes.
    #
    #    The uv project environment is devenv's UV_PROJECT_ENVIRONMENT, synced
    #    with the interpreter devenv would use and activated here after the
    #    sync. It lives under this checkout and names no path of it: uv creates
    #    it relocatable, setup-environment.ts rewrites the editable installs of
    #    workspace members relative to it, and the interpreter is the same
    #    store path everywhere (languages.python.libraries above). A
    #    copy-on-write clone copies it along with everything else, installed.
    #    devenv's own virtualenv and uv sync tasks are refused below: they
    #    activate the environment from its own activation script — which in a
    #    clone names the original checkout — before this prologue runs, and key
    #    their skip on pyproject.toml alone.
    #
    #    A local install failure is reported and the shell still loads, so the
    #    tools to repair it stay available; CI fails. Repo-owned enterShell
    #    bodies merge after this prologue.
    # 6. Shell entry resolves the `shell` group of `smoo.secrets`, only when it
    #    installs, and nothing else. Shell entry happens on every direnv reload
    #    and every `devenv shell -- <command>`, and a provider command that runs
    #    then is a credential prompt on every one of them (1Password authorises
    #    per requesting process lineage, and this one is new each time). A
    #    credential only one deliberate command needs belongs to that command:
    #    `smoo secrets run <group> <command...>`. The full statement, and the
    #    derivation below, live in tooling/direnv/secret-references.ts.
    #
    #    A group is derived from what the repository already declares, so
    #    nothing restates it:
    #
    #    The variable `smoo.remoteCache.tokenSecret` names is group
    #    `nx-cache`, and shell entry never resolves it. Nx reads
    #    NX_SELF_HOSTED_REMOTE_CACHE_SERVER and _ACCESS_TOKEN from the
    #    environment when it runs: CI injects them into the job, a developer
    #    exports the token once in the terminal that wants the cache (for
    #    example `op signin`, then the declared command), and every nested
    #    shell inherits it. Absent, Nx runs with the local cache only.
    #
    #    A variable `.npmrc` interpolates as `${VAR}` is group `registry`,
    #    deferred rather than resolved, so setup-environment.ts installs
    #    without it. An installed checkout contacts no registry, which is why
    #    that costs nothing in the common case; an install that genuinely
    #    needs one fails naming the variable, and `smoo secrets run registry
    #    bun install` supplies it for that one command.
    # 7. GOROOT is unset rather than set. With devenv's Go pinned to the patch
    #    release ttsc vendors, a GOROOT crossing cannot misfire on version at all,
    #    so this is belt-and-braces rather than the fix — it keeps the isolation
    #    boundary intact even while the two are momentarily out of step, e.g. after
    #    a ttsc bump and before the Go pin follows it. The boundary itself: our Go
    #    comes from devenv, a dependency may vendor its own SDK, and neither may
    #    leak GOROOT into the other. An inherited GOROOT names exactly one
    #    toolchain and so is wrong for at least one side, surfacing as `compile:
    #    version does not match go tool version`. Absent the variable, every Go
    #    resolves its own GOROOT from its own binary, which makes "whose Go is
    #    whose" a property of which binary is invoked — the only thing that can
    #    actually be reasoned about.
    # 8. On Darwin, Xcode answers for the Apple toolchain: nix CC/CXX, a
    #    nix-store SDKROOT and DEVELOPER_DIR, and every store PATH entry carrying
    #    nixpkgs xcbuild's `xcrun` are dropped, each one announced, so Xcode's
    #    clang compiles against the licensed SDK and `xcrun` is Apple's.
    #    tooling/direnv/apple-developer.sh does it and states why.
    # Nx's fallback includes HOME or TMPDIR, either of which can exceed the
    # Unix socket limit in a checkout. Reuse devenv's short runtime directory,
    # which is keyed on this checkout's devenv root and so is one per
    # workspace. Unconditionally: an inherited value is another workspace's
    # shell (a shed entered from the host, a second repository from the first,
    # a subprocess of either), and one socket dir for two workspaces makes the
    # daemon refuse whichever came second ("received a message from a
    # different workspace"). Nobody supplies this deliberately.
    (lib.mkBefore ''
      cd "$DEVENV_ROOT/../.."
      export PATH="$("$PWD/tooling/direnv/repo-path")"
      # devenv enters the shell with TMPDIR unset (not empty) on every platform.
      # Tools then fall back to /tmp, which on Darwin is not the per-user
      # temporary directory the OS hands out, and a test that asserts the
      # parent carries TMPDIR (so its env_clear check means something) fails in
      # the shell and passes outside it. Ask the OS; an explicit value wins.
      if [ -z "''${TMPDIR:-}" ]; then
        TMPDIR="$(getconf DARWIN_USER_TEMP_DIR 2>/dev/null || true)"
        export TMPDIR="''${TMPDIR:-/tmp}"
      fi
      export TTSC_TSGO_BINARY="$PWD/node_modules/@typescript/native/bin/tsc"
      . "$DEVENV_ROOT/shared-caches.sh" /private/cowshed/caches
      unset GOROOT
      bun "$DEVENV_ROOT/setup-environment.ts"${lib.optionalString uvProject " --python ${python.package.interpreter}"} || exit $?
      ${lib.optionalString uvProject ''
        if [ -f pyproject.toml ]; then
          export VIRTUAL_ENV="$UV_PROJECT_ENVIRONMENT"
          export PATH="$VIRTUAL_ENV/bin:$PATH"
        fi
      ''}
      # Nx's workspace root and socket dir: tooling/direnv/nx-socket-dir.sh
      # gives every workspace its own socket dir, and in a cowshed checkout the
      # one literal path its host shells and sandboxed jobs all share, and
      # states why.
      . "$DEVENV_ROOT/nx-socket-dir.sh" /tmp
      ${lib.optionalString pkgs.stdenv.isDarwin ''
        . "$DEVENV_ROOT/apple-developer.sh" /nix/store
      ''}
    '')
    # Epilogue, after every project step (a project's own enterShell may resolve
    # SDKROOT/compilers; the toolchain identity must see the final values). The
    # stamp logic lives in toolchain-stamp.ts; see its header for why.
    (lib.mkAfter ''
      bun "$DEVENV_ROOT/toolchain-stamp.ts" || exit $?
      if [ -n "$DEVENV_SHELL_PWD" ]; then
        cd "$DEVENV_SHELL_PWD"
      fi
    '')
  ];

  assertions = [
    {
      assertion = !(uvProject && (python.venv.enable || python.uv.sync.enable));
      message = ''
        devenv.smoo.nix syncs and activates the uv project environment itself (enterShell item 5).
        Set languages.python.venv.enable and languages.python.uv.sync.enable to false in devenv.nix:
        devenv's tasks activate the environment from its own activation script, which in a
        copy-on-write clone names the original checkout, and skip the sync while uv.lock changes.
      '';
    }
  ];
}
