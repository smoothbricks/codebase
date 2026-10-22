import type * as Api from './api.generated.js';
import type {
  HostAttachAllArguments,
  HostCredentialAddArguments,
  HostDiskCommandArguments,
  HostListProjectsArguments,
  HostSkillInstallArguments,
  JobLogsArguments,
  ProjectListArguments,
  SccacheStartArguments,
} from './native.generated.js';

export type * from './api.generated.js';
export type {
  HostAttachAllArguments,
  HostCredentialAddArguments,
  HostDiskCommandArguments,
  HostListProjectsArguments,
  HostSkillInstallArguments,
  JobLogsArguments,
  ProjectListArguments,
  SccacheStartArguments,
} from './native.generated.js';

/** A line a launched verb's descriptor carried that the verb's protocol does not allow (07_api.md, "Launchers"). */
export type ProtocolViolation = {
  readonly descriptor: 'state' | 'handle';
  readonly bytes: Uint8Array;
  readonly reason: string;
};

export type CowshedErrorOptions = ErrorOptions &
  Pick<Api.CowshedError, 'admission' | 'cpuAdmission' | 'priority' | 'stdin'> & {
    readonly violation?: ProtocolViolation;
  };

export class CowshedError extends Error {
  readonly code: Api.ErrorCode;
  readonly hint: string;
  /** Present only on a keyed exec that spawned nothing, naming why and the job its key admitted. */
  readonly admission?: Api.AdmissionRefusal;
  /** Present only on a refused CPU admission: a class a worker may not ask for, a gate whose bound expired, a gateway that refused. */
  readonly cpuAdmission?: Api.CpuAdmissionRefusal;
  /** Present only when an owned job's child could not be placed at its nice, so its command never ran. */
  readonly priority?: Api.PriorityFailure;
  readonly stdin: Api.StdinRefusal | undefined;
  /** Present only on a launcher's `integrity` error: the line its verb's descriptor broke the protocol with. */
  readonly violation?: ProtocolViolation;

  constructor(code: Api.ErrorCode, message: string, hint: string, options?: CowshedErrorOptions) {
    super(message, options);
    this.name = 'CowshedError';
    this.code = code;
    this.hint = hint;
    if (options?.admission !== undefined) {
      this.admission = options.admission;
    }
    if (options?.cpuAdmission !== undefined) {
      this.cpuAdmission = options.cpuAdmission;
    }
    if (options?.priority !== undefined) {
      this.priority = options.priority;
    }
    this.stdin = options?.stdin;
    if (options?.violation !== undefined) {
      this.violation = options.violation;
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

/** An attachment, rather than an admission-time producer, owns this job's input. */
export type OpenStdin = Extract<Api.ExecStdin, { readonly kind: 'open' }>;

/** Defaults and raw/UTF-8 stdin sugar over the generated request, never another DTO list. */
export type ExecOptions = {
  readonly [Key in keyof ExecOptionFields]?: Exclude<ExecOptionFields[Key], null>;
} & (
  | { readonly stdin?: string | Uint8Array | OpenStdin; readonly stdinWorkspacePath?: never }
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
  /** The workspace that currently occupies build slot `slot`; rejects `not-found` when none does. */
  workspaceInSlot(slot: number): Promise<WorkspaceRef>;
  path(name: string, options?: PathOptions): Promise<Api.WorkspaceInfo>;
  /**
   * The published workspaces. `landing: 'measure'` measures every attached session against main's
   * landing branch; `'landed'` keeps only the sessions whose work is provably fully landed. A
   * workspace whose mount is unproven has no row and is named in `unproven`.
   */
  listWorkspaces(options?: ProjectListArguments): Promise<WorkspaceListing>;
}

/**
 * A project's listing: a row for each workspace whose mount state is proven, and each workspace
 * whose mount is unproven by name. That one is neither attached nor detached, so it has no row.
 */
export interface WorkspaceListing {
  readonly workspaces: readonly Api.WorkspaceInfo[];
  readonly unproven: readonly Api.UnprovenWorkspace[];
}

/**
 * One `Host.credentialAdd` call: a secret named by `env` or `command` travels alone, and an
 * `upload` secret travels with its bytes, so no call sends bytes beside a named source or an
 * upload without them.
 */
export type CredentialAddCall =
  | [request: HostCredentialAddArguments & { readonly secret: Exclude<Api.CredentialSecretSource, { kind: 'upload' }> }]
  | [request: HostCredentialAddArguments & { readonly secret: { readonly kind: 'upload' } }, secret: Uint8Array];

/**
 * The host's own verbs over one `cowshed host` connection, served by the binary the package
 * launcher picks (07_api.md, "Host operations"). A project-subject operation names the project by
 * a path inside its checkout, as `--project` does.
 */
export interface Host {
  listProjects(options?: HostListProjectsArguments): Promise<readonly Api.ProjectWorkspaces[]>;
  attachAll(options?: HostAttachAllArguments): Promise<readonly Api.WorkspaceInfo[]>;
  detachAll(): Promise<void>;
  /** Retires a project no controller can open; calling it is the explicit abandonment. */
  forgetAbandoned(repoId: Api.RepoRequest['repoId']): Promise<Api.ProjectRetirementReport>;
  mountMain(repoId: Api.RepoRequest['repoId']): Promise<Api.MountResult>;
  gatewayStatus(): Promise<Api.GatewayStatus>;
  credentialStatus(project: Api.ProjectPathRequest['project']): Promise<Api.CredentialReport>;
  credentialRemove(
    project: Api.CredentialRemoveRequest['project'],
    origin: Api.CredentialRemoveRequest['origin'],
  ): Promise<Api.CredentialReport>;
  identityAdd(
    project: Api.IdentityAddRequest['project'],
    remote: Api.IdentityAddRequest['remote'],
  ): Promise<Api.IdentityReport>;
  skillInstall(options?: HostSkillInstallArguments): Promise<Api.SkillInstallReport>;
  buildState(project: Api.ProjectPathRequest['project']): Promise<Api.BuildStateListing>;
  clearNxStore(project: Api.ProjectPathRequest['project']): Promise<Api.NxStoreClearResult>;
  /** Installs this package's build and starts the gateway onto it, refusing a downgrade. */
  gatewayStart(): Promise<Api.GatewayStatus>;
  /** Stops the gateway; `purge` also removes the installed binary. */
  gatewayStop(options?: Partial<Api.GatewayStopRequest>): Promise<void>;
  /** Who the kernel shows holding the lock file at `path`. */
  lockHolders(path: Api.HostPathRequest['path']): Promise<Api.LockObservation>;
  /** Every process of this user holding something inside the directory tree at `path`. */
  treeHolders(path: Api.HostPathRequest['path']): Promise<readonly Api.TreeHolder[]>;
  /**
   * Asks the job supervisor named by the `COWSHED_SUPERVISOR_SOCKET` this process had when it
   * opened the host to retire `handle`'s retained root and its descendants.
   */
  retireDescendants(
    handle: Api.RetireDescendantsRequest['handle'],
    graceMs: Api.RetireDescendantsRequest['graceMs'],
  ): Promise<Api.JobSubtreeRetirement>;
  /**
   * Enrols a scoped gateway registry credential (`credential add`) and answers the project's
   * routes. A secret named by `env` or `command` is resolved by `cowshed host`, whose environment
   * is this process's; an `upload` secret is these bytes, sent as the call's upload frame.
   */
  credentialAdd(...call: CredentialAddCall): Promise<Api.CredentialReport>;
  /**
   * Runs one `diskutil` or `hdiutil` argv through cowshed's disk runner, as `disk-command` does:
   * under the host's disk lease, an image verb never killed. `stdin` is the tool's whole input.
   * The events come once the tool has ended: its whole stdout in order, then its whole stderr,
   * then its exit.
   */
  diskCommand(request: HostDiskCommandArguments): AsyncIterable<Api.DiskCommandEvent>;
  /** Ends the connection and with it the `cowshed host` process; later calls reject. */
  close(): Promise<void>;
}

/**
 * The sccache daemon's control over one `cowshed sccache host` connection, which the package
 * launcher routes to the installed build as it routes every `sccache` verb (07_api.md, "Host
 * operations"). A connection and a process of its own: `Host` never opens it.
 */
export interface SccacheHost {
  /** Installs (or repairs) the sccache LaunchAgent, its cap `capacity` (`40g`, `1t`) or derived. */
  start(options?: SccacheStartArguments): Promise<Api.SccacheStatus>;
  /** Removes the sccache LaunchAgent; the daemon stops. */
  stop(): Promise<void>;
  status(): Promise<Api.SccacheStatus>;
  /** Ends the connection and with it the `cowshed sccache host` process; later calls reject. */
  close(): Promise<void>;
}

export interface Coordinator {
  adopt(options?: Api.AdoptOptions): Promise<Adopted>;
  fork(workspace: string, options?: Partial<Api.ForkOptions> & { source?: string }): Promise<WorkspaceRef>;
  rename(source: string, destination: string): Promise<WorkspaceRef>;
  moveCheckout(destination: string): Promise<WorkspaceRef>;
  /** Changes the adopted project's repository identity; main is the reference it answers with. */
  changeRepoId(newRepoId: string): Promise<WorkspaceRef>;
  grant(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet>;
  revoke(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet>;
  /** The project's standing grants every workspace runs under. */
  projectGrants(): Promise<Api.ProjectGrants>;
  grantProject(delta: Api.ProjectGrantDelta): Promise<Api.ProjectGrants>;
  revokeProject(delta: Api.ProjectGrantDelta): Promise<Api.ProjectGrants>;
  rebase(workspace: string, options?: RebaseOptions): Promise<Api.RebaseReport>;
  land(workspace: string, options?: LandOptions): Promise<Api.LandReport>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  resize(workspace: string, capacity: string, volume: Api.ResizeVolume): Promise<Api.ResizeResult>;
  /** Rewrites the workspace's image contiguously; a busy workspace refuses before its image is touched. */
  defragment(workspace: string): Promise<Api.DefragmentResult>;
  /** Deletes the workspace's Nx cache and task database contents on its build volume. */
  clearNxState(workspace: string): Promise<Api.NxStateClearResult>;
  /** Rebuilds one keyless workspace's CA identity at its preserved incarnation. */
  rekey(workspace: string): Promise<Api.RekeyResult>;
  /** Discard live build state for an existing seed clone; never refresh a seed or alter source files. */
  reseed(workspace: string, from: string, dryRun: boolean): Promise<Api.ReseedResult>;
  remove(workspace: string, options?: Api.RemoveOptions): Promise<Api.RemoveReport>;
  /** Removes the adopted project end to end: every session, its images and main's adoption. */
  removeProject(options?: Api.RemoveProjectOptions): Promise<Api.RemoveProjectReport>;
  gc(options?: Api.GcOptions): Promise<Api.GcReport>;
  /**
   * Moves one recovery image (`sessions/.trash/<ws>-<incarnation>.dmg`) with its companions and a manifest out of the
   * store into `<to>/<ws>-<incarnation>/` (`cowshed gc --recover`); both paths absolute. It never deletes.
   */
  recoverImage(image: string, to: string): Promise<Api.RecoveredImage>;
  doctor(): Promise<Api.DoctorReport>;
  worker(workspace: string): Promise<WorkspaceHandle>;
  /**
   * Admits one job under coordinator authority: the only route that may ask for the gate class (a gate waits for its
   * grant with no deadline, ahead of queued background work) or the host-gate scope. The worker capability supplies only
   * the workspace incarnation the job is fenced to; a worker's own exec never admits a gate and is refused with the typed
   * class-not-permitted error.
   */
  exec(worker: WorkspaceHandle, request: ExecRequest): Promise<JobHandle>;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  shell(session?: string): Promise<Session>;
  listJobs(): Promise<readonly Api.JobInfo[]>;
  job(id: number): Promise<JobHandle>;
  /** Returns null only when the key is provably absent in this workspace incarnation; failures reject. */
  jobByKey(admissionKey: Api.AdmissionKey): Promise<JobHandle | null>;
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

export interface JobAttachOptions {
  readonly cursor?: Api.JobJournalCursor;
}

/** Input view on one durable job. EOF and detachment never cancel the job. */
export interface JobAttachment {
  /** Resolves only after all bytes are delivered under the core's bounded backpressure. */
  write(chunk: Uint8Array): Promise<void>;
  /** Sends stdin EOF once; repeated calls are idempotent. */
  end(): Promise<void>;
  detach(): Promise<void>;
}

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
  /** Identity-fenced retained process lives, including exited ones and explicit coverage gaps. */
  processes(): Promise<Api.JobProcessTree>;
  /**
   * The canonical ordered observation stream: birth, exec, sparse changes, fixed-minute
   * heartbeats and final usage on exit. Leaving the loop closes only this reader, never the job.
   */
  processEvents(everyMs: number): AsyncIterable<Api.JobProcessEvent>;
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
  attach(options?: JobAttachOptions): Promise<JobAttachment>;
  detach(): Promise<void>;
  wait(): Promise<Api.JobInfo>;
  kill(): Promise<void>;
}

/**
 * What an adopt made: main's reference, and, when the call re-adopted main, what the replacement
 * kept of what it replaced (02_workspaces.md, "Re-adoption", step 5).
 */
export interface Adopted {
  readonly workspace: WorkspaceRef;
  readonly reAdoption: Api.ReAdoption | null;
}

export interface WorkspaceRef {
  readonly name: string;
  readonly mountPath: string;
  info(): Promise<Api.WorkspaceInfo>;
  attach(options?: Api.AttachOptions): Promise<void>;
  grants(): Promise<Api.GrantSet>;
}
