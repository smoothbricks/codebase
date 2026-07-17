export type ErrorCode =
  | 'internal'
  | 'usage'
  | 'not-found'
  | 'conflict'
  | 'environment-missing'
  | 'sandbox-denied'
  | 'integrity';

export class CowshedError extends Error {
  readonly code: ErrorCode;
  readonly hint: string;

  constructor(code: ErrorCode, message: string, hint: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'CowshedError';
    this.code = code;
    this.hint = hint;
  }
}

export type WorkspaceRole = 'main' | 'workspace';
export type ImageFormat = 'asif' | 'sparse';
export type WorkspaceState = 'attached' | 'detached';
export type EnsureAction = 'alreadyMounted' | 'attached' | 'healed';
export type EgressMode = 'intercept' | 'opaque';
export type SimVerb = 'openurl' | 'install';
export type RunSandboxMode = 'readWrite' | 'readOnly';
export type JobStream = 'stdout' | 'stderr';
export type JobState = 'queued' | 'running' | 'exited' | 'signaled' | 'killed' | 'outputLimit' | 'failed';

export type RevisionTarget = { readonly branch: string } | { readonly ref: string } | { readonly oid: string };

export type ExpectedRefHead = { readonly missing: true } | { readonly oid: string };

export interface CheckpointInfo {
  readonly label: string;
  readonly revision: number;
  readonly pinned: boolean;
}

export interface WorkspaceInfo {
  readonly repoId: string;
  readonly workspace: string;
  readonly workspaceIncarnation: string;
  readonly role: WorkspaceRole;
  readonly imageFormat: ImageFormat;
  readonly mount: string;
  readonly state: WorkspaceState;
  readonly branch?: string;
  readonly baseCommit?: string;
  readonly createdAt?: string;
  readonly checkpoints: readonly CheckpointInfo[];
  readonly snapshotStale: boolean;
}

export interface PortBlock {
  readonly base: number;
  readonly size: number;
}

export interface EgressRule {
  readonly host: string;
  readonly ports?: readonly number[];
  readonly mode?: EgressMode;
  readonly impersonate?: string;
}

export interface GrantSet {
  readonly revision: number;
  readonly portBlock?: PortBlock;
  readonly read: readonly string[];
  readonly write: readonly string[];
  readonly egress: readonly EgressRule[];
  readonly repos?: readonly string[];
  readonly sim: readonly SimVerb[];
}

export interface EnsureReport {
  readonly workspace: string;
  readonly mount: string;
  readonly action: EnsureAction;
  readonly goEnv: string;
  readonly workspaceToken: string;
  readonly portBlock?: PortBlock;
}

export interface AttachOptions {
  readonly browse?: boolean;
}

export interface AdoptOptions {
  readonly path?: string;
  readonly repoId?: string;
  readonly capacity?: string;
  readonly quarantine?: boolean;
  readonly imageFormat?: ImageFormat;
}

export interface CreateOptions {
  readonly revision?: RevisionTarget;
  readonly fromWorkspace?: string;
  readonly browse?: boolean;
  readonly slot?: number;
}

export interface GrantDelta {
  readonly read?: readonly string[];
  readonly write?: readonly string[];
  readonly egress?: readonly EgressRule[];
  readonly repos?: readonly string[];
  readonly sim?: readonly SimVerb[];
  readonly expectedRevision?: number;
}

export interface RebaseOptions {
  readonly onto?: RevisionTarget;
  readonly fresh?: boolean;
  readonly expectedWorkspaceIncarnation?: string;
  readonly expectedSourceHead?: string;
  readonly expectedOntoHead?: string;
}

export interface LandOptions {
  readonly targetBranch?: string;
  readonly check?: readonly string[];
  readonly retire?: boolean;
  readonly pushOnly?: boolean;
  readonly expectedWorkspaceIncarnation?: string;
  readonly expectedSourceHead?: string;
  readonly expectedTargetHead?: ExpectedRefHead;
}

export interface RemoveOptions {
  readonly force?: boolean;
  readonly restore?: boolean;
}

export interface GcOptions {
  readonly dryRun?: boolean;
}

export interface OutputPublication {
  readonly path: string;
  readonly policy: 'createNew' | 'replace';
}

export interface ExecRequest {
  readonly argv: readonly string[];
  readonly cwd?: string;
  readonly mode?: RunSandboxMode;
  readonly env?: Readonly<Record<string, string>>;
  readonly trace?: { readonly traceId: string; readonly spanId: string };
  /** UTF-8 stdin. Use stdinWorkspacePath for an existing workspace-relative file. */
  readonly stdin?: string;
  readonly stdinWorkspacePath?: string;
  readonly stdoutCopy?: OutputPublication;
  readonly stderrCopy?: OutputPublication;
}

export interface PushOptions {
  readonly branch?: string;
  readonly expectedWorkspaceIncarnation?: string;
  readonly expectedSourceHead?: string;
  readonly expectedDestinationHead?: ExpectedRefHead;
}

export interface LandReport {
  readonly landedHead: string;
  readonly targetBranch: string;
  readonly previousTargetHead?: string;
  readonly targetWasCheckedOut: boolean;
  readonly retired: boolean;
}

export interface GcCandidate {
  readonly identity: string;
  readonly path: string;
  readonly bytes: number;
  readonly reason:
    | 'retiredWorkspace'
    | 'orphanStagingImage'
    | 'orphanStagingMetadata'
    | 'expiredCheckpoint'
    | 'detachedImageCompaction';
}

export interface GcReport {
  readonly examined: number;
  readonly reclaimed: number;
  readonly retainedPinned: number;
  readonly freedBytes: number;
  readonly dryRun: boolean;
  readonly candidates: readonly GcCandidate[];
}

export interface PushReport {
  readonly sourceHead: string;
  readonly destinationRef: string;
  readonly previousDestinationHead?: string;
}

export interface JobInfo {
  readonly repoId: string;
  readonly workspaceIncarnation: string;
  readonly jobId: number;
  readonly state: JobState;
  readonly pid?: number;
  readonly grantRevision: number;
  readonly argv: readonly string[];
  readonly cwd?: string;
  readonly started: string;
  readonly durationMs?: number;
  readonly exit?: unknown;
  readonly stdout: unknown;
  readonly stderr: unknown;
  readonly trace: { readonly traceId: string; readonly spanId: string };
  readonly outputLimit?: unknown;
  readonly stdin: unknown;
}

/**
 * Affine inherited descriptor accepted by the controller handshake.
 * The value owns the descriptor and may be consumed by exactly one openProject or
 * connectCoordinator call.
 */
export interface CoordinatorEndpoint {
  readonly __opaqueCoordinatorEndpoint: unique symbol;
}

export interface Project {
  readonly repoId: string;
  readonly gitRoot: string;
  main(): Promise<WorkspaceRef>;
  workspace(name: string): Promise<WorkspaceRef>;
  listWorkspaces(): Promise<readonly WorkspaceInfo[]>;
}

/** Authority-carrying coordinator. Retain this object for the mutation lifecycle. */
export interface Coordinator {
  adopt(options?: AdoptOptions): Promise<WorkspaceRef>;
  create(name: string, options?: CreateOptions): Promise<WorkspaceRef>;
  fork(source: string, destination: string): Promise<WorkspaceRef>;
  grant(workspace: string, delta: GrantDelta): Promise<GrantSet>;
  revoke(workspace: string, delta: GrantDelta): Promise<GrantSet>;
  rebase(workspace: string, options?: RebaseOptions): Promise<string>;
  land(workspace: string, options?: LandOptions): Promise<LandReport>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  destroy(workspace: string, options?: RemoveOptions): Promise<void>;
  gc(options?: GcOptions): Promise<GcReport>;
  worker(workspace: string): Promise<WorkspaceHandle>;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  shell(session?: string): Promise<Session>;
  listJobs(): Promise<readonly JobInfo[]>;
  job(id: number): Promise<JobHandle>;
  push(options?: PushOptions): Promise<PushReport>;
  grants(): Promise<GrantSet>;
}

export interface Session {
  readonly isNamed: boolean;
  exec(request: ExecRequest): Promise<JobHandle>;
}

export interface JobHandle {
  readonly id: number;
  status(): Promise<JobInfo>;
  /** Buffered output; follow resolves after the followed stream closes. */
  readLogs(stream: JobStream, follow?: boolean): Promise<Uint8Array>;
  attach(): Promise<JobAttachment>;
  detach(): Promise<void>;
  wait(): Promise<JobInfo>;
  kill(): Promise<void>;
}

export interface JobAttachment {
  detach(): Promise<void>;
}

export interface WorkspaceRef {
  readonly name: string;
  readonly mountPath: string;
  info(): Promise<WorkspaceInfo>;
  ensure(): Promise<EnsureReport>;
  attach(options?: AttachOptions): Promise<void>;
  grants(): Promise<GrantSet>;
}
