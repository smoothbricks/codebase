/** One scripted call of the stand-in `nx` that land-host.test.ts gives tooling/land-host.sh (land-host.fake-nx.ts). */
export interface Call {
  /** The arguments the script must pass, `--outputStyle=...` aside. An argument ending in `*` matches by prefix. */
  readonly args: readonly string[];
  readonly exit: number;
  /** What the run prints: the output of its tasks and Nx's closing summary. */
  readonly output?: string;
  /** The bounded-exec verdict records and JUnit reports the run's tasks leave in the checkout's workspace data. */
  readonly records?: readonly { readonly task: string; readonly record: unknown; readonly report?: string }[];
  /** The task graph written to the call's `--graph=FILE` argument. */
  readonly graph?: unknown;
}
