# Check Devenv is installed
if ! has devenv; then
  nix profile add --accept-flake-config nixpkgs#devenv
else
  # Check Devenv is new enough
  if [[ $(devenv version | cut -d ' ' -f 2 | cut -d '.' -f 1) -lt 1 ]]; then
    echo "Devenv version is less than 1.0: Please update devenv."
    echo "# nix profile remove .\*devenv"
    echo "# nix profile add --accept-flake-config nixpkgs#devenv"
    exit 1
  fi
fi

# Devenv (Nix)
eval "$(devenv direnvrc)"
use devenv

# Watch JS tooling changes
watch_file bun.lock
watch_file package.json
watch_file setup-environment.ts

unset \
  CONFIG_SHELL HOST_PATH IN_NIX_SHELL MACOSX_DEPLOYMENT_TARGET NIX_BUILD_CORES NIX_CFLAGS_COMPILE \
  NIX_COREFOUNDATION_RPATH NIX_DONT_SET_RPATH NIX_DONT_SET_RPATH_FOR_BUILD NIX_ENFORCE_NO_NATIVE \
  NIX_IGNORE_LD_THROUGH_GCC NIX_INDENT_MAKE NIX_NO_SELF_RPATH NIX_STORE PATH_LOCALE SOURCE_DATE_EPOCH \
  DETERMINISTIC_BUILD __darwinAllowLocalNetworking __impureHostDeps __propagatedImpureHostDeps \
  __propagatedSandboxProfile __sandboxProfile buildInputs builder configureFlags depsBuildBuild \
  depsBuildBuildPropagated depsBuildTarget depsBuildTargetPropagated depsHostHost depsHostHostPropagated \
  depsTargetTarget depsTargetTargetPropagated doCheck doInstallCheck dontAddDisableDepTrack \
  name nativeBuildInputs out outputs patches propagatedBuildInputs propagatedNativeBuildInputs shell shellHook \
  stdenv strictDeps system __structuredAttrs cmakeFlags mesonFlags
