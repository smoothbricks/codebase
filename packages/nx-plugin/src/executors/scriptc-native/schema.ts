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
}
