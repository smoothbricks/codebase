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
  /** Present only when the caller asked to pay for the landing measurement. */
  readonly landing?: WorkspaceLanding;
}

/** Where a workspace's commits stand relative to the branch that outlives it. */
export type LandingCommits =
  | {
      readonly state: 'measured';
      readonly targetBranch: string;
      readonly targetHead: string;
      readonly unlanded: number;
      readonly landed: number;
      readonly behind: number;
    }
  | { readonly state: 'indeterminate'; readonly reason: string };

export interface WorkspaceLanding {
  /** Dirty working-tree paths; absent when the tree could not be read (a different fact from clean). */
  readonly dirtyFiles?: number;
  readonly commits: LandingCommits;
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

export interface AttachOptions {
  readonly browse?: boolean;
  readonly observedPath?: string;
}
export interface PathOptions {
  readonly noAttach?: boolean;
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
  readonly register?: boolean;
  readonly gitWorktree?: boolean;
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
  readonly abandon?: boolean;
}

export interface CheckpointOptions {
  readonly label?: string;
  readonly keep?: boolean;
}

export interface ResizeResult {
  readonly workspace: string;
  readonly previousCapacity: string;
  readonly capacity: string;
}

export type FindingSeverity = 'info' | 'warning' | 'error';

export interface Finding {
  readonly code: string;
  readonly severity: FindingSeverity;
  readonly message: string;
  readonly hint: string;
  readonly path?: string;
}

export interface DoctorReport {
  readonly healthy: boolean;
  readonly findings: readonly Finding[];
}

export interface AbandonedWork {
  readonly head: string;
  readonly targetBranch: string;
  readonly targetHead?: string;
  readonly unlandedCommits: number;
  readonly bundle: string;
}

export interface RemoveReport {
  readonly abandoned?: AbandonedWork;
}

export interface GcOptions {
  readonly dryRun?: boolean;
}

export interface OutputPublication {
  readonly path: string;
  readonly policy: 'createNew' | 'replace';
}

export interface ExecRequest {
  /**
   * UTF-8 arguments. Not `CommandArg[]` like `JobInfo.argv`, and deliberately so: a JS string is
   * UTF-16 and cannot hold a non-UTF-8 byte sequence, so `string[]` is exactly the set this
   * caller can express and `String -> CommandArg` is exact for all of it.
   */
  readonly argv: readonly string[];
  readonly cwd?: string;
  readonly mode?: RunSandboxMode;
  readonly env?: Readonly<Record<string, string>>;
  readonly trace?: TraceContext;
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
    | 'orphanStagingMount'
    | 'expiredCheckpoint'
    | 'detachedImageCompaction';
}

export interface GcReport {
  readonly examined: number;
  readonly reclaimed: number;
  readonly retainedPinned: number;
  /** Staging entries an operation still holds the lifecycle lock for: not garbage yet. */
  readonly retainedActive: number;
  readonly freedBytes: number;
  readonly dryRun: boolean;
  readonly candidates: readonly GcCandidate[];
}

export interface PushReport {
  readonly sourceHead: string;
  readonly destinationRef: string;
  readonly previousDestinationHead?: string;
}

/**
 * Byte-exact bytes spelled as JSON, tagged with which spelling carries them.
 *
 * `utf8` holds the bytes verbatim. `base64` holds canonical padded standard base64 of a byte
 * sequence that is *not* valid UTF-8, so the tag is a fact about the bytes rather than a
 * producer's choice and decoding is never lossy. Rust: `CommandArg` and `BinaryData` in
 * `cowshed-core::api::dto` share this grammar.
 */
export interface TaggedBytes {
  readonly encoding: 'utf8' | 'base64';
  readonly data: string;
}

/** One immutable operating-system command argument, preserved byte-for-byte. */
export type CommandArg = TaggedBytes;

/** Stream bytes small enough to live in the job DTO instead of a protected file. */
export type BinaryPayload = TaggedBytes;

/** How a process ended. Both the `signaled` and `killed` job states carry `signaled`. */
export type ExitStatus =
  | { readonly kind: 'exited'; readonly code: number }
  | { readonly kind: 'signaled'; readonly signal: number; readonly coreDumped: boolean };

/** Where the tamper-evident copy of a stream lives. */
export type ProtectedOutput =
  | { readonly kind: 'inline'; readonly data: BinaryPayload }
  | { readonly kind: 'file'; readonly path: string };

/**
 * How a stream reached storage. `redirect` additionally names the workspace path the job itself
 * wrote to; `artifact` is the protected copy either way.
 */
export type OutputStorage =
  | { readonly kind: 'captured'; readonly artifact: ProtectedOutput }
  | { readonly kind: 'redirect'; readonly source: string; readonly artifact: ProtectedOutput };

/** A bounded, human-readable projection of a stream. Never the stream itself. */
export interface OutputSummary {
  readonly version: number;
  readonly text: string;
  readonly truncated: boolean;
}

/** One captured job stream. `bytes` and `sha256` describe the whole stream, not the summary. */
export interface StreamInfo {
  readonly storage: OutputStorage;
  readonly bytes: number;
  /** 64 lowercase hexadecimal characters. */
  readonly sha256: string;
  readonly summary: OutputSummary;
}

export type StdinKind = 'empty' | 'inline' | 'stream' | 'workspaceFile';

export interface StdinInfo {
  readonly kind: StdinKind;
  readonly bytes: number;
  /** Present only for the `workspaceFile` kind. */
  readonly workspacePath?: string;
  /** False when a streamed stdin was still open when the job ended. */
  readonly complete: boolean;
}

/** The limit a job crossed, and the byte count that crossed it. */
export interface OutputLimitInfo {
  readonly limitBytes: number;
  readonly crossingBytes: number;
}

export interface TraceContext {
  /** 32 lowercase hexadecimal characters. */
  readonly traceId: string;
  /** 16 lowercase hexadecimal characters. */
  readonly spanId: string;
}

export interface JobInfo {
  readonly repoId: string;
  readonly workspaceIncarnation: string;
  readonly jobId: number;
  readonly state: JobState;
  readonly pid?: number;
  readonly grantRevision: number;
  readonly argv: readonly CommandArg[];
  /**
   * The job's working directory, or `null` for the workspace root. Explicitly `null` rather than
   * absent: unlike every other optional here, the controller always emits this key.
   */
  readonly cwd: string | null;
  readonly started: string;
  /** Present exactly for terminal states. */
  readonly durationMs?: number;
  readonly exit?: ExitStatus;
  readonly stdout: StreamInfo;
  readonly stderr: StreamInfo;
  readonly trace: TraceContext;
  /** Present exactly for the `outputLimit` state. */
  readonly outputLimit?: OutputLimitInfo;
  readonly stdin: StdinInfo;
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
  workspaceAt(path: string): Promise<WorkspaceRef>;
  path(name: string, options?: PathOptions): Promise<WorkspaceInfo>;
  listWorkspaces(): Promise<readonly WorkspaceInfo[]>;
}

/** Authority-carrying coordinator. Retain this object for the mutation lifecycle. */
export interface Coordinator {
  adopt(options?: AdoptOptions): Promise<WorkspaceRef>;
  create(name: string, options?: CreateOptions): Promise<WorkspaceRef>;
  fork(source: string, destination: string): Promise<WorkspaceRef>;
  rename(source: string, destination: string): Promise<WorkspaceRef>;
  moveCheckout(destination: string): Promise<WorkspaceRef>;
  grant(workspace: string, delta: GrantDelta): Promise<GrantSet>;
  revoke(workspace: string, delta: GrantDelta): Promise<GrantSet>;
  rebase(workspace: string, options?: RebaseOptions): Promise<string>;
  land(workspace: string, options?: LandOptions): Promise<LandReport>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  resize(workspace: string, capacity: string): Promise<ResizeResult>;
  remove(workspace: string, options?: RemoveOptions): Promise<RemoveReport>;
  gc(options?: GcOptions): Promise<GcReport>;
  doctor(): Promise<DoctorReport>;
  worker(workspace: string): Promise<WorkspaceHandle>;
}

export interface WorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(request: ExecRequest): Promise<JobHandle>;
  shell(session?: string): Promise<Session>;
  listJobs(): Promise<readonly JobInfo[]>;
  job(id: number): Promise<JobHandle>;
  checkpoint(options?: CheckpointOptions): Promise<string>;
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
  attach(options?: AttachOptions): Promise<void>;
  grants(): Promise<GrantSet>;
}
