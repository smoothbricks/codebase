# The mbx cowshed setup installs beside the installed cowshed (specs/cowshed/14_nix.md, "mbx"): mbx
# v1.29.1 and upstream's later fixes, plus compact predictions, scheduler admission, grant
# descriptors, short session roots,
# Cargo shim-chain selection, selected default sysroots keyed by the toolchain parts a compilation
# uses, version-2 activity-only admission reports,
# Nix cc-wrapper and store-input keying, precompiled headers, more modeled compiler flags, the
# refusal of host links a Nix wrapper shapes, edits counted against HEAD, LRU refresh for exposed
# hits, per-target build-script predictions, nested Cargo targets, single-donor target seeding and
# the pool rule that starts a reservation on the cores that are free (MBX_RESERVE_CPUS), one
# reservation endpoint per Nx run (`mbx reservations`), library compiles keyed without the
# `-C linker` path they never run, Clippy members keyed alike under every installation of a
# toolchain, precompiled headers whose module file dependencies Clang's driver lists under every
# Clang release, generated-source directories shared under `mbx exec` as OUT_DIR is when
# MBX_SHARE_ENV names their variable, with a build script's declared dependency on such a variable
# keyed as the build gave it, and the managed target a collection removed, placed again for a
# build that names its own directory below `target/`, and host links with no Nix wrapper in them
# (an unwrapped driver, an absolute unwrapped `-fuse-ld` linker) identified while a Nix shell's
# link variables are set, every variable MBX_SHARE_ENV lists shared for each compilation it is
# set for, a precompiled header built under mbx recorded so its users key by that build, and each
# use of a store object or result recorded by mbx itself, so eviction keeps what builds read on a
# noatime volume and frees a result with its descriptor blob, and every target path a native link
# reads spelled physically so one -oso_prefix strips them all from a macOS debug map, and
# -Zosx-rpath-install-name keyed as written rather than bypassing every compilation that carries
# it, from
# smoothbricks/mr-boxington at the pinned revision.
#
#   nix build /path/to/nix/mbx#mbx   # result/bin/mbx
#
# On a cowshed host, `cowshed setup` runs that with `--out-link` under cowshed's support directory,
# the GC root that keeps the build, and links `dev.cowshed/bin/mbx` to the rooted executable; see
# crates/cowshed-cli/src/capabilities/mbx.rs.
{
  description = "mbx at the revision cowshed pins";

  # Keep the exact revision and mbx's own locked nixpkgs: setup installs this one build for
  # every Cargo checkout (specs/cowshed/05_gateway.md, "Pool selection and revision").
  inputs.mbx.url = "github:smoothbricks/mr-boxington/8cd8f6b23d0984192a86befbb4cb1bdd3ffac8e2";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
