/** One scripted call of the stand-in `nx` that land-host.test.ts gives tooling/land-host.sh (land-host.fake-nx.ts). */
export interface Call {
  /** The exact arguments the script must pass. An argument ending in `*` matches by prefix. */
  readonly args: readonly string[];
  readonly exit: number;
  /** What the run prints: the output of its tasks and Nx's closing summary. */
  readonly output?: string;
  /** Refuse unless both output descriptors name the same regular capture file. */
  readonly requireFileOutput?: boolean;
  /** The rows of the run summary Nx writes into its cache directory; a call that names none leaves no summary. */
  readonly run?: readonly RunRow[];
}

/** One task row of a run summary (`run.json`): the task ran at `hash` and exited with `status`. */
export interface RunRow {
  readonly taskId: string;
  readonly hash: string;
  readonly status: number;
}
