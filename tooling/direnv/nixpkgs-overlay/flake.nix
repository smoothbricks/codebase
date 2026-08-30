{
  outputs = {...}: {
    overlays.default = self: super:
      with super; let
        sources = callPackage ./_sources/generated.nix {};
        bunSource = sources."bun-${stdenvNoCC.hostPlatform.system}";
        sccacheSuffix = "cowshed";
      in {
        # Override bun with the version from nvfetcher
        bun = bun.overrideAttrs (finalAttrs: previousAttrs: {
          inherit (bunSource) version src;
        });
        # sccache patches — physical copies of packages/cowshed/patches/.
        # Cowshed is the authoritative source. This overlay is its own nix flake
        # and cannot reference paths outside its directory, so the copies are
        # physical: a symlink would not survive `nix flake` source copying.
        # `smoo monorepo check` asserts both directories hold the same filenames
        # with byte-identical contents, so divergence cannot land silently.
        # 1. rust-basedir-cwd: SCCACHE_BASEDIRS normalization extended to Rust
        #    cache keys, plus SCCACHE_BASEDIR_CWD=1 so cwd, blanket CARGO_* env,
        #    and argument bytes key relative to the request cwd. cargo >= 1.97
        #    path-independent -C metadata plus this is what lets every cowshed
        #    workspace share one cache at any mount path. env-dep values are
        #    never normalized. Prove it with `nm sccache | grep hash_normalized`.
        # 2. singleflight: concurrent misses of one cache key wait for the first
        #    compile to publish instead of running N rustcs. Prove it with
        #    `nm sccache | grep inflight_join`.
        # Cargo default features are `all` (S3/GCS/redis/dist-client/…). Cowshed
        # only needs the local disk cache and the UDS daemon, so strip them.
        # devenv applies this overlay more than once, so the override has to be idempotent: the
        # second pass sees the first pass's result as `super.sccache`. Guarding on the suffix makes
        # re-application a no-op — without it `version` compounds into `0.17.0-cowshed-cowshed`
        # and postPatch searches Cargo.toml for a version string this override itself invented.
        # `lib.unique` guards the patch list the same way.
        # The version is bumped with the patch set on purpose. sccache's client/server handshake
        # compares `CARGO_PKG_VERSION`: on a mismatch the client stops the old server and starts
        # its own, which is a clear, recoverable event. With both builds reporting a bare "0.17.0"
        # a NEW client meeting an OLD server instead trips the changed single-flight protocol and
        # dies on `sccache rustc -vV` with SIGTERM, which reads as a broken toolchain rather than a
        # stale binary. `cowshed setup` copies this build to a host-stable path for launchd, so the
        # server can lag the client by exactly one `setup` — the case this suffix makes legible.
        sccache =
          if lib.hasSuffix "-${sccacheSuffix}" sccache.version
          then sccache
          else
            sccache.overrideAttrs (finalAttrs: previousAttrs: {
              version = "${previousAttrs.version}-${sccacheSuffix}";
              cargoBuildNoDefaultFeatures = true;
              cargoBuildFeatures = [];
              cargoCheckNoDefaultFeatures = true;
              cargoCheckFeatures = [];
              buildInputs = lib.filter (p: (lib.getName p) != "openssl") (previousAttrs.buildInputs or []);
              patches = lib.unique ((previousAttrs.patches or [])
                ++ [
                  ./sccache-rust-basedir-cwd.patch
                  ./sccache-singleflight.patch
                ]);
              # `version` alone renames the derivation; sccache reports CARGO_PKG_VERSION, so the
              # crate manifest has to carry it too. Cargo.lock is deliberately NOT touched:
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
      };
  };
}
