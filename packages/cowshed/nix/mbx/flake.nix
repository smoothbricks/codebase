# The mbx cowshed setup installs beside the installed cowshed (specs/cowshed/14_nix.md, "mbx"): mbx
# v1.28.0 plus compact predictions, scheduler admission, grant descriptors, short session roots,
# Cargo shim-chain selection, selected default sysroots and bounded live admission activity, from
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
  inputs.mbx.url = "github:smoothbricks/mr-boxington/5434eb066d6d141a734151d5155bec9f2ec34f0f";

  outputs = {mbx, ...}: {
    packages = builtins.mapAttrs (_: packages: {mbx = packages.default;}) mbx.packages;
  };
}
