let
  config = import ./devenv.nix {
    inputs = {};
    lib.optionalAttrs = condition: attrs:
      if condition
      then attrs
      else {};
    pkgs.stdenv = {
      isLinux = true;
      cc = "/nix/store/native-cc";
    };
  };
  expected = {
    CC_x86_64_unknown_linux_gnu = "/nix/store/native-cc/bin/cc";
    CXX_x86_64_unknown_linux_gnu = "/nix/store/native-cc/bin/c++";
  };
in
  if config ? env && config.env == expected
  then "Linux target compiler environment uses the Nix wrapper\n"
  else builtins.throw "Linux target compiler environment does not use the Nix wrapper"
