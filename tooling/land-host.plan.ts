/** One scripted call of the stand-in `nx` that land-host.test.ts gives tooling/land-host.sh (land-host.fake-nx.ts). */
export interface Call {
  /** The exact arguments the script must pass. An argument ending in `*` matches by prefix. */
  readonly args: readonly string[];
  readonly exit: number;
  /** What the run prints: the output of its tasks and Nx's closing summary. */
  readonly output?: string;
}
