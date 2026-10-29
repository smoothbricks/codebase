# The mbx cowshed setup installs beside the installed cowshed (specs/cowshed/14_nix.md, "mbx"): mbx
# v1.28.0 plus the compact-prediction, scheduler-admission and grant-descriptor changes, from smoothbricks/mr-boxington at
# the revision cowshed pins,
# as a standalone flake that references nothing outside itself.
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
  inputs.mbx.url = "github:smoothbricks/mr-boxington/9d91cf452ec59682a9a4581bdd7e39e2b247be22";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
