{
  outputs = {...}: {
    overlays.default = self: super:
      with super; {
        # Overrides go here, see: https://github.com/cachix/devenv/issues/478#issuecomment-1663735284
        bun = bun.overrideAttrs (finalAttrs: previousAttrs: {
          # Use same ICU as Node.js does
          postPatchelf = lib.optionalString stdenvNoCC.hostPlatform.isDarwin ''
            wrapProgram $out/bin/bun \
              --prefix DYLD_LIBRARY_PATH : ${lib.makeLibraryPath [icu]}
          '';
        });
      };
  };
}
