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
# Clang release, and generated-source directories shared under `mbx exec` as OUT_DIR is when
# MBX_SHARE_ENV names their variable, from smoothbricks/mr-boxington at the pinned revision.
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
  inputs.mbx.url = "github:smoothbricks/mr-boxington/affe13e4d843587557cff467a3f9ccd696ec8f8b";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
