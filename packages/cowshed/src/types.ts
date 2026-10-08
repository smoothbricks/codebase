import type * as Api from './api.generated.js';
import type { JobLogsArguments } from './native.generated.js';

export type * from './api.generated.js';
export type { JobLogsArguments } from './native.generated.js';

export class CowshedError extends Error {
  readonly code: Api.ErrorCode;
  readonly hint: string;
  /** Present only on a keyed exec that spawned nothing, naming why and the job its key admitted. */
  readonly admission?: Api.AdmissionRefusal;

  constructor(
    code: Api.ErrorCode,
    message: string,
    hint: string,
    options?: ErrorOptions & { readonly admission?: Api.AdmissionRefusal },
  ) {
    super(message, options);
    this.name = 'CowshedError';
    this.code = code;
    this.hint = hint;
    if (options?.admission !== undefined) {
      this.admission = options.admission;
    }
  }
}

/** Client-only attachment preference; the controller owns the workspace's identity. */
export interface PathOptions {
  readonly noAttach?: boolean;
}

/** A resolved reference crosses separately, not as JSON reconstructed from its name. */
export type RebaseOptions = Api.RebaseOptions & { readonly into?: WorkspaceRef };
export type LandOptions = Api.LandOptions & { readonly into?: WorkspaceRef };

/** JS strings are the byte-exact UTF-8 subset of the core's OS-native argument type. */
type Utf8Command<Command> = Command extends { readonly argv: readonly Api.CommandArg[] }
  ? Omit<Command, 'argv'> & { readonly argv: readonly string[] }
  : Command;

/**
 * Exactly one member of a union of records: each member forbids the keys only the others declare,
 * so naming two members' keys at once does not type-check rather than silently choosing one.
 */
type Exclusive<Union, Keys extends PropertyKey = Union extends unknown ? keyof Union : never> = Union extends unknown
  ? Union & { readonly [Key in Exclude<Keys, keyof Union>]?: never }
  : never;

export type ExecCommand = Exclusive<Utf8Command<Api.ExecCommand>>;

type ExecOptionFields = Omit<
  Api.ExecParams,
  'repoId' | 'workspace' | 'workspaceIncarnation' | 'session' | 'argv' | 'script' | 'stdin'
>;

/** Defaults and UTF-8 stdin sugar over the generated controller request, never another DTO list. */
export type ExecOptions = {
  readonly [Key in keyof ExecOptionFields]?: Exclude<ExecOptionFields[Key], null>;
} & (
  | { readonly stdin?: string; readonly stdinWorkspacePath?: never }
  | { readonly stdin?: never; readonly stdinWorkspacePath?: Api.WorkspacePath }
);
export type ExecRequest = ExecCommand & ExecOptions;

/** Affine inherited descriptor; it may be consumed by exactly one connection attempt. */
export interface CoordinatorEndpoint {
  readonly __opaqueCoordinatorEndpoint: unique symbol;
}

export interface Project {
  readonly repoId: string;
  readonly gitRoot: string;
  main(): Promise<WorkspaceRef>;
  workspace(name: string): Promise<WorkspaceRef>;
  workspaceAt(path: string): Promise<WorkspaceRef>;
  path(name: string, options?: PathOptions): Promise<Api.WorkspaceInfo>;
  listWorkspaces(): Promise<readonly Api.WorkspaceInfo[]>;
}

export interface Coordinator {
  adopt(options?: Api.AdoptOptions): Promise<WorkspaceRef>;
  create(name: string, options?: Api.CreateOptions): Promise<WorkspaceRef>;
  fork(source: string, destination: string): Promise<WorkspaceRef>;
  rename(source: string, destination: string): Promise<WorkspaceRef>;
  moveCheckout(destination: string): Promise<WorkspaceRef>;
  grant(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet>;
  revoke(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet>;
  rebase(workspace: string, options?: RebaseOptions): Promise<Api.RebaseReport>;
  land(workspace: string, options?: LandOptions): Promise<Api.LandReport>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  resize(workspace: string, capacity: string, volume: Api.ResizeVolume): Promise<Api.ResizeResult>;
  remove(workspace: string, options?: Api.RemoveOptions): Promise<Api.RemoveReport>;
  gc(options?: Api.GcOptions): Promise<Api.GcReport>;
  doctor(): Promise<Api.DoctorReport>;
  worker(workspace: string): Promise<WorkspaceHandle>;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  shell(session?: string): Promise<Session>;
  listJobs(): Promise<readonly Api.JobInfo[]>;
  job(id: number): Promise<JobHandle>;
  jobByKey(admissionKey: Api.AdmissionKey): Promise<JobHandle>;
  checkpoint(options?: Api.CheckpointOptions): Promise<string>;
  push(options?: Api.PushOptions): Promise<Api.PushReport>;
  grants(): Promise<Api.GrantSet>;
}

export interface Session {
  readonly isNamed: boolean;
  exec(request: ExecRequest): Promise<JobHandle>;
}

/** One `job.logs` chunk: where it ends, whether the stream has closed, and its bytes. */
export type JobLogs = Api.LogsChunk & { readonly bytes: Uint8Array };

export interface JobHandle {
  readonly id: number;
  status(): Promise<Api.JobInfo>;
  /**
   * The supervisor's resource sample while the job runs, frozen once it ends. Before the job
   * owns a process this rejects with the typed not-ready conflict, never a zero sample.
   */
  resources(): Promise<Api.JobResourceSample>;
  /**
   * The job's resource samples: the latest once it owns a process, another every `everyMs` while
   * it runs, then its terminal sample once. Each step of the loop asks the controller for one
   * sample, so a slow reader gets the latest, never a backlog; leaving the loop ends the
   * subscription, never the job.
   */
  progress(everyMs: number): AsyncIterable<Api.JobResourceSample>;
  /**
   * One stream's bytes from `offset`, at most one chunk. Reading again from `nextOffset` continues
   * where this chunk ended. Without `follow`, stop at an empty chunk or `eof`: an empty chunk is
   * the current end even when the stream is still open (`eof: false`). With `follow`, the supervisor
   * waits for written bytes or the job's end instead of answering an empty chunk while the job runs.
   */
  logs(args: JobLogsArguments): Promise<JobLogs>;
  /**
   * The TCP ports the job's process group listens on now, read from the kernel: a listening
   * child counts, a port another process holds never does, and nothing connects to a port.
   */
  listeningPorts(): Promise<Api.JobListeningPorts>;
  /**
   * A bounded raw slice of both streams after `cursor`, or their latest bounded tail when there is
   * none; `next` is the cursor that continues after it. A cursor past what the job admitted is a
   * usage error.
   */
  tail(cursor: Api.JobJournalCursor | undefined, limits: Api.JobTailLimits): Promise<Api.JobTail>;
  detach(): Promise<void>;
  wait(): Promise<Api.JobInfo>;
  kill(): Promise<void>;
}

export interface WorkspaceRef {
  readonly name: string;
  readonly mountPath: string;
  info(): Promise<Api.WorkspaceInfo>;
  attach(options?: Api.AttachOptions): Promise<void>;
  grants(): Promise<Api.GrantSet>;
}
