/**
 * A package's foreign functions: C its executables call through scriptc's FFI. A path that is absolute or climbs out of
 * its root is refused: Nx hashes only what lies beneath the project, and the run removes only its own staging.
 */
export interface ScriptcNativeFfi {
  /**
   * The FFI manifest, relative to cwd. It lists no libraries: every platform builds it, and a prebuilt archive holds one
   * triple's code. The run hands scriptc a staged copy of it that links the archive it builds.
   */
  manifest: string;
  /**
   * Ambient declaration files, each relative to cwd, to where it is staged beside the emitted JavaScript, relative to
   * that JavaScript's root. tsc never emits a declaration file, and scriptc binds a call only through a declaration it
   * can resolve from the JavaScript it compiles.
   */
  declarations: Record<string, string>;
  /** C sources, relative to cwd, compiled for the run's triple into the one archive the staged manifest links. */
  sources: string[];
}

export interface ScriptcNativeOptions {
  /** The project root, relative to the workspace root. */
  cwd: string;
  /** The package's emit config (`tsconfig.lib.json`), relative to cwd: the run emits it, transformed, into staging. */
  tsConfig: string;
  /** The triple `SCRIPTC_TARGET` names (`aarch64-linux-musl`). */
  triple: string;
  /** Where the executables are written (`dist/bin/linux-arm64`), relative to cwd. */
  outputDirectory: string;
  /** Each executable's name and its entry module, relative to the emitted JavaScript's root. */
  entries: Record<string, string>;
  /** Absent for a package that calls no C. */
  ffi?: ScriptcNativeFfi;
}
