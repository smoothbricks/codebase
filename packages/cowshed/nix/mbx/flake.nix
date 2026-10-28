# The mbx cowshed setup installs beside the installed cowshed (specs/cowshed/14_nix.md, "mbx"): mbx
# v1.28.0 at the revision cowshed pins, as a standalone flake that references nothing outside
# itself.
#
#   nix build /path/to/nix/mbx#mbx   # result/bin/mbx
#
# On a cowshed host, `cowshed setup` runs that with `--out-link` under cowshed's support directory,
# the GC root that keeps the build, and links `dev.cowshed/bin/mbx` to the rooted executable; see
# crates/cowshed-cli/src/capabilities/mbx.rs.
{
  description = "mbx at the revision cowshed pins";

  # An exact revision, and mbx's own locked nixpkgs rather than a `follows`: the store path names
  # the build, so the mbx a project's devenv resolves through mbx's flake at this revision and the
  # one this flake builds are the same executable, and that realpath equality is the pin proof
  # (specs/cowshed/05_gateway.md, "Pool selection and revision").
  inputs.mbx.url = "github:jdx/mr-boxington/44f50c329a83dedd012e1e49cf9f265f79226a83";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
