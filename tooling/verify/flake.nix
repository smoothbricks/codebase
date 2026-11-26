{
  description = "Standalone formal verification and UB-checking toolchain (Miri, Kani, Alive2, CBMC, Z3, Lean 4)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/825e2028c29b702a4a5f085f08095d12099784f2";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
  }: let
    supportedSystems = [
      "aarch64-darwin"
      "x86_64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ];

    kaniVersion = "0.68.0";
    # Exact nightly matching kani-0.68.0's rust-toolchain-version (rustc 1.100.0-nightly).
    # Keeps cargo-miri and kani-compiler aligned on one rustc ABI.
    rustNightlyDate = "2026-08-21";

    kaniSources = {
      aarch64-darwin = {
        triple = "aarch64-apple-darwin";
        hash = "sha256-pdOaXV50glOlU6pi8pXGw5coeSeijl4yaR1/8u2gw5g=";
      };
      x86_64-darwin = {
        triple = "x86_64-apple-darwin";
        hash = "sha256-sTZ/aUwTUKvK4TNwH3izvj/W6b62ZoGiLqC8rAzDSAg=";
      };
      aarch64-linux = {
        triple = "aarch64-unknown-linux-gnu";
        hash = "sha256-AELSaIvxVQJw3vSnnJaq7qE4YxgYy0q1SAEg8tUalCw=";
      };
      x86_64-linux = {
        triple = "x86_64-unknown-linux-gnu";
        hash = "sha256-MuK0hNc+3gu/ZKLPCHnEJZQiSX2K7E7mfUSN6a54Q9M=";
      };
    };

    forEachSystem = f:
      nixpkgs.lib.genAttrs supportedSystems (system:
        f {
          inherit system;
          pkgs = import nixpkgs {
            inherit system;
            overlays = [rust-overlay.overlays.default];
          };
        });
  in {
    packages = forEachSystem ({
      system,
      pkgs,
    }: let
      rustToolchain = pkgs.rust-bin.nightly.${rustNightlyDate}.default.override {
        extensions = [
          "miri"
          "rust-src"
          "llvm-tools-preview"
          "rustc-dev"
        ];
      };

      kaniMeta = kaniSources.${system};
      kani = pkgs.stdenvNoCC.mkDerivation {
        pname = "kani";
        version = kaniVersion;

        src = pkgs.fetchurl {
          url = "https://github.com/model-checking/kani/releases/download/kani-${kaniVersion}/kani-${kaniVersion}-${kaniMeta.triple}.tar.gz";
          inherit (kaniMeta) hash;
        };

        nativeBuildInputs =
          [pkgs.makeWrapper]
          ++ pkgs.lib.optionals pkgs.stdenvNoCC.isLinux [pkgs.autoPatchelfHook];

        buildInputs = pkgs.lib.optionals pkgs.stdenvNoCC.isLinux [
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
        ];

        dontConfigure = true;
        dontBuild = true;

        installPhase = let
          libVar =
            if pkgs.stdenvNoCC.isDarwin
            then "DYLD_FALLBACK_LIBRARY_PATH"
            else "LD_LIBRARY_PATH";
        in ''
          runHook preInstall
          mkdir -p "$out/libexec/kani-${kaniVersion}" "$out/bin"
          cp -r . "$out/libexec/kani-${kaniVersion}/"
          ln -s "${rustToolchain}" "$out/libexec/kani-${kaniVersion}/toolchain"
          makeWrapper "$out/libexec/kani-${kaniVersion}/bin/kani-driver" "$out/bin/kani" \
            --unset RUSTC \
            --unset RUSTC_WRAPPER \
            --unset RUSTUP_TOOLCHAIN \
            --unset CARGO \
            --prefix PATH : "${rustToolchain}/bin:$out/libexec/kani-${kaniVersion}/bin" \
            --prefix ${libVar} : "${rustToolchain}/lib"
          makeWrapper "$out/libexec/kani-${kaniVersion}/bin/kani-driver" "$out/bin/cargo-kani" \
            --argv0 cargo-kani \
            --unset RUSTC \
            --unset RUSTC_WRAPPER \
            --unset RUSTUP_TOOLCHAIN \
            --unset CARGO \
            --prefix PATH : "${rustToolchain}/bin:$out/libexec/kani-${kaniVersion}/bin" \
            --prefix ${libVar} : "${rustToolchain}/lib"
          runHook postInstall
        '';

        meta = {
          description = "Kani Rust Verifier (prebuilt release ${kaniVersion})";
          homepage = "https://github.com/model-checking/kani";
          license = with pkgs.lib.licenses; [asl20 mit];
          platforms = supportedSystems;
        };
      };

      miriWrapper = pkgs.writeShellScriptBin "miri" ''
        unset RUSTC RUSTC_WRAPPER RUSTUP_TOOLCHAIN CARGO
        export MIRIFLAGS="''${MIRIFLAGS:--Zmiri-disable-isolation}"
        export PATH="${rustToolchain}/bin:$PATH"
        exec "${rustToolchain}/bin/cargo" miri "$@"
      '';
    in {
      inherit kani rustToolchain;
      miri = miriWrapper;
    });

    devShells = forEachSystem ({
      system,
      pkgs,
    }: let
      own = self.packages.${system};
    in {
      default = pkgs.mkShell {
        name = "smoothbricks-verify";
        packages = [
          own.miri
          own.rustToolchain
          own.kani
          pkgs.alive2
          pkgs.cbmc
          pkgs.z3
          pkgs.lean4
        ];
      };
    });
  };
}
