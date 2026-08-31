# The patched sccache, as a standalone flake. This directory — flake.nix, flake.lock, and the two
# .patch files beside them — is self-contained: it references nothing outside itself, so it builds
# from a bare copy with no repository, no cowshed, and no npm package around it.
#
# On a machine without cowshed (a NixOS build server is the case this exists for):
#
#   nix build /path/to/nix/sccache#sccache          # result/bin/sccache
#   nix profile install /path/to/nix/sccache#sccache
#   nix run /path/to/nix/sccache -- --version       # sccache 0.17.0-cowshed
#
# or as a NixOS module input:
#
#   inputs.cowshed-sccache.url = "path:/path/to/nix/sccache";
#   environment.systemPackages = [inputs.cowshed-sccache.packages.${system}.sccache];
#
# On a cowshed host, `cowshed setup --sccache` runs the first of those with `--out-link` and hands
# the resolved store path to launchd; see crates/cowshed-cli/src/sccache_nix.rs.
{
  description = "sccache patched for cowshed: one shared cache across workspace mount paths, one compile per cache key";

  # An exact nixpkgs revision, not a branch: this flake exists to name ONE sccache build, and the
  # store path it produces is the identity `cowshed setup --sccache` writes into the LaunchAgent
  # plist. A branch would let `nix flake update` slide the daemon's binary — and therefore the
  # cache's key space — out from under a host that never asked for it.
  #
  # Plain `github:NixOS/nixpkgs`, deliberately: a NixOS server installs this flake directly with no
  # cowshed involvement, and must not need a flakehub account or a registry entry to do it.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/e5bdc4a41d4c072fe1e3787eaa0320a384741d44";

  outputs = {
    self,
    nixpkgs,
  }: let
    # No flake-utils. Two lines of `genAttrs` do the whole job, and a second flake input is a
    # second thing to audit and lock for a package with exactly one dependency.
    #
    # `x86_64-darwin` is deliberately absent: nixpkgs 26.11 dropped it, so
    # `legacyPackages.x86_64-darwin` throws on evaluation
    # (https://nixos.org/manual/nixpkgs/unstable/release-notes#x86_64-darwin-26.11). Declaring a
    # system whose evaluation throws is worse than omitting it — `nix flake show` and
    # `nix flake check` would both fail, and an operator would read a nixpkgs release note instead
    # of a sentence about their own host. `cowshed setup --sccache` names the unsupported system
    # itself before it ever invokes nix. Sourcing that one system from 26.05 through a second input
    # was the alternative and is refused: it would mean two nixpkgs, two rustcs, and therefore two
    # cache key spaces behind one flake.
    systems = [
      "aarch64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ];
    forSystems = nixpkgs.lib.genAttrs systems;
  in {
    packages = forSystems (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
        inherit (pkgs) lib;

        # The version suffix is PROVENANCE, not a mechanism. It is the one place `sccache --version`
        # and `--show-stats` can tell an operator which build is answering, and that is all it does.
        #
        # An earlier version of this comment claimed sccache's client/server handshake compares
        # CARGO_PKG_VERSION and stops the old server on a mismatch. That is false, read against
        # mozilla/sccache 0.17.0: `connect_or_start_server` (commands.rs:310-348) connects, and only
        # starts a server on ConnectionRefused, TimedOut, or NotFound; `ServerInfo.version` is
        # merely reported (server.rs:2147, 2203-2206); a mismatch surfaces only as a hint attached
        # to a bincode failure in `request_stats`; and Shutdown is reachable only from the explicit
        # StopServer verb. No version string causes any server to stop.
        #
        # What actually contends is the socket. A unix bind UNLINKS the path first
        # (server.rs:510-514), so ANY client that auto-starts a server steals the LaunchAgent's
        # socket — whatever either side calls itself. That is the failure a pinned plist naming a
        # GC-rooted store path prevents, and it is prevented by there being one supervised server
        # for clients to find, not by a name.
        suffix = "cowshed";

        # 1. rust-basedir-cwd: SCCACHE_BASEDIRS normalization extended to Rust cache keys, plus
        #    SCCACHE_BASEDIR_CWD=1 so cwd, blanket CARGO_* env, and argument bytes key relative to
        #    the request cwd. cargo >= 1.97 path-independent -C metadata plus this is what lets
        #    every cowshed workspace share one cache at any mount path. env-dep values are never
        #    normalized. Present in the build iff `nm` finds `hash_normalized`.
        # 2. singleflight: concurrent misses of one cache key wait for the first compile to publish
        #    instead of running N rustcs. Present in the build iff `nm` finds `inflight_join`.
        #
        # This directory is the only copy of these patches. `smoo monorepo check` asserts that this
        # list and the `.patch` files beside it name each other exactly, so neither an orphaned file
        # nor a reference to a deleted one can land.
        patches = [
          ./sccache-rust-basedir-cwd.patch
          ./sccache-singleflight.patch
        ];

        sccache = pkgs.sccache.overrideAttrs (finalAttrs: previousAttrs: {
          inherit patches;
          version = "${previousAttrs.version}-${suffix}";
          # The rename is the point, not an accident: `src` deliberately stays nixpkgs' 0.17.0
          # tarball and the suffix names the patch set applied on top of it. Without this, every
          # evaluation — including the one `cowshed setup --sccache` runs — prints a 12-line
          # "override both version and src" warning at the operator.
          __intentionallyOverridingVersion = true;

          # Cargo default features are `all` (S3/GCS/redis/dist-client/…). Cowshed needs the local
          # disk cache and the UDS daemon and nothing else, so the cloud backends — and the openssl
          # they drag in — are stripped rather than built and never called.
          cargoBuildNoDefaultFeatures = true;
          cargoBuildFeatures = [];
          cargoCheckNoDefaultFeatures = true;
          cargoCheckFeatures = [];
          buildInputs = lib.filter (p: (lib.getName p) != "openssl") (previousAttrs.buildInputs or []);

          # `version` alone renames the derivation; sccache reports CARGO_PKG_VERSION, so the crate
          # manifest has to carry it too. Cargo.lock is deliberately NOT touched:
          # cargoSetupPostPatchHook diffs the source lock against the vendor's and fails on any
          # difference, so editing it reads as "cargoHash is out of date". Cargo refreshes the root
          # package's own version in the lock itself, offline, after that hook has run.
          postPatch =
            (previousAttrs.postPatch or "")
            + ''
              substituteInPlace Cargo.toml \
                --replace-fail 'version = "${previousAttrs.version}"' 'version = "${finalAttrs.version}"'
            '';
        });
      in {
        inherit sccache;
        default = sccache;
      }
    );

    # `nix run` from the flake dir, for a host inspecting the build it is about to install.
    apps = forSystems (system: {
      default = {
        type = "app";
        program = "${self.packages.${system}.sccache}/bin/sccache";
      };
    });
  };
}
