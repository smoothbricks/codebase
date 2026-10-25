export interface ScriptcNativeOptions {
  /** The project root, relative to the workspace root. */
  cwd: string;
  /** The host's directory under `<outputRoot>/bin` (`darwin-arm64`); an option so it keys the task's hash. */
  platform: string;
  /** The JavaScript `tsc-js` emitted, relative to cwd. */
  javascriptRoot: string;
  /** Where `tsconfig.json` and `bin/<platform>/<name>` are written, relative to cwd. */
  outputRoot: string;
  /** Each executable's name and its emitted JavaScript entry, relative to cwd. */
  entries: Record<string, string>;
}
