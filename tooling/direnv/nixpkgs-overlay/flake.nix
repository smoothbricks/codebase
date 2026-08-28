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
        # sccache with the Rust-hasher basedir patch (same patch as
        # packages/cowshed/patches/): SCCACHE_BASEDIRS normalization extended to
        # Rust cache keys, plus the per-request SCCACHE_BASEDIR_CWD=1 opt-in that
        # keys cwd, blanket CARGO_* env values, and argument bytes relative to the
        # request cwd. With cargo >= 1.97's path-independent -C metadata this is
        # what lets every cowshed workspace share one cache at any mount path.
        # env-dep values are never normalized, so crates that compile
        # env!("CARGO_MANIFEST_DIR") into their output fail closed. Targets 0.17.x.
        # To check that a given build carries the patch, look for the mangled
        # symbol `generate_hash_key::hash_normalized`, the nested fn this patch
        # alone introduces (`nm sccache | grep hash_normalized`; upstream 0.17.0
        # has zero occurrences of that name). Searching the binary for the string
        # SCCACHE_BASEDIR_CWD reads zero on a patched build too: the name appears
        # only in a 19-byte equality compare, which LLVM lowers to immediate
        # loads, so no literal survives. SCCACHE_BASEDIRS survives only because it
        # is passed to env::var_os.
        # lib.unique: devenv applies this overlay more than once, which is
        # invisible for the idempotent bun override but would append (and
        # double-apply) the patch.
        sccache = sccache.overrideAttrs (finalAttrs: previousAttrs: {
          patches = lib.unique ((previousAttrs.patches or []) ++ [./sccache-rust-basedir-cwd.patch]);
        });
      };
  };
}
