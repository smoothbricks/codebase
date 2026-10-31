# The mbx cowshed setup installs beside the installed cowshed (specs/cowshed/14_nix.md, "mbx"): mbx
# v1.29.0 plus compact predictions, scheduler admission, grant descriptors, short session roots,
# Cargo shim-chain selection, selected default sysroots, version-2 activity-only admission reports,
# Nix cc-wrapper and store-input keying, precompiled headers, more modeled compiler flags, the
# refusal of host links a Nix wrapper shapes, edits counted against HEAD, LRU refresh for exposed
# hits, per-target build-script predictions, nested Cargo targets, single-donor target seeding and
# the pool rule that starts a reservation on the cores that are free (MBX_RESERVE_CPUS), from
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
  inputs.mbx.url = "github:smoothbricks/mr-boxington/5b9b0f86a0ab481d9ad5b85e586f4ba6872f74fb";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
