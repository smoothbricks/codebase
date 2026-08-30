{
  outputs = {...}: {
    overlays.default = self: super:
      with super; let
        sources = callPackage ./_sources/generated.nix {};
        bunSource = sources."bun-${stdenvNoCC.hostPlatform.system}";
      in {
        # Override bun with the version from nvfetcher
        bun = bun.overrideAttrs (finalAttrs: previousAttrs: {
          inherit (bunSource) version src;
        });
        # sccache patches (same files as packages/cowshed/patches/):
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
        # lib.unique: devenv applies this overlay more than once, which would
        # otherwise append (and double-apply) the patches.
        sccache = sccache.overrideAttrs (finalAttrs: previousAttrs: {
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
        });
      };
  };
}
