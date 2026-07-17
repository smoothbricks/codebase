import typia from 'typia';
import {
  loadNativeModule,
  type NativeCoordinatorHandle,
  type NativeJobAttachmentHandle,
  type NativeJobHandle,
  type NativeProjectHandle,
  type NativeSessionHandle,
  type NativeWorkspaceHandle,
  type NativeWorkspaceRefHandle,
} from './native.js';
import {
  type AdoptOptions,
  type AttachOptions,
  type Coordinator,
  type CoordinatorEndpoint,
  CowshedError,
  type CreateOptions,
  type EnsureReport,
  type ErrorCode,
  type ExecRequest,
  type GcOptions,
  type GcReport,
  type GrantDelta,
  type GrantSet,
  type JobAttachment,
  type JobHandle,
  type JobInfo,
  type LandOptions,
  type LandReport,
  type Project,
  type PushOptions,
  type PushReport,
  type RebaseOptions,
  type RemoveOptions,
  type Session,
  type WorkspaceHandle,
  type WorkspaceInfo,
  type WorkspaceRef,
} from './types.js';

export {
  type AdoptOptions,
  type AttachOptions,
  type CheckpointInfo,
  type Coordinator,
  type CoordinatorEndpoint,
  CowshedError,
  type EgressMode,
  type EgressRule,
  type EnsureAction,
  type EnsureReport,
  type ErrorCode,
  type ExecRequest,
  type ExpectedRefHead,
  type GcCandidate,
  type GcOptions,
  type GcReport,
  type GrantDelta,
  type GrantSet,
  type ImageFormat,
  type JobAttachment,
  type JobHandle,
  type JobInfo,
  type JobState,
  type JobStream,
  type LandOptions,
  type LandReport,
  type OutputPublication,
  type PortBlock,
  type Project,
  type PushOptions,
  type PushReport,
  type RebaseOptions,
  type RemoveOptions,
  type RevisionTarget,
  type RunSandboxMode,
  type Session,
  type SimVerb,
  type WorkspaceHandle,
  type WorkspaceInfo,
  type WorkspaceRef,
  type WorkspaceRole,
  type WorkspaceState,
} from './types.js';

interface NativeError {
  readonly code: ErrorCode;
  readonly message: string;
}

const native = loadNativeModule();
const isNativeError = typia.createIs<NativeError>();
const parseWorkspaceInfo = typia.json.createAssertParse<WorkspaceInfo>();
const parseWorkspaceInfos = typia.json.createAssertParse<WorkspaceInfo[]>();
const parseEnsureReport = typia.json.createAssertParse<EnsureReport>();
const parseGrantSet = typia.json.createAssertParse<GrantSet>();
const parseLandReport = typia.json.createAssertParse<LandReport>();
const parseGcReport = typia.json.createAssertParse<GcReport>();
const parsePushReport = typia.json.createAssertParse<PushReport>();
const parseJobInfo = typia.json.createAssertParse<JobInfo>();
const parseJobInfos = typia.json.createAssertParse<JobInfo[]>();
const assertAttachOptions = typia.createAssertEquals<AttachOptions>();
const NEXT_HINT_MARKER = '\nnext: ';

function normalizeNativeError(error: unknown): unknown {
  if (!isNativeError(error)) {
    return error;
  }

  const marker = error.message.lastIndexOf(NEXT_HINT_MARKER);
  if (marker < 0) {
    return new CowshedError(error.code, error.message, 'cowshed doctor --json', { cause: error });
  }

  return new CowshedError(
    error.code,
    error.message.slice(0, marker),
    error.message.slice(marker + NEXT_HINT_MARKER.length),
    { cause: error },
  );
}

function callNative<T>(call: () => T): T {
  try {
    return call();
  } catch (error) {
    throw normalizeNativeError(error);
  }
}

async function callNativeAsync<T>(call: () => Promise<T>): Promise<T> {
  try {
    return await call();
  } catch (error) {
    throw normalizeNativeError(error);
  }
}

function encodeAttachOptions(options: AttachOptions | undefined): string | undefined {
  if (options === undefined) {
    return undefined;
  }

  const encoded = JSON.stringify(assertAttachOptions(options));
  if (encoded === undefined) {
    // invariant throw: a validated plain AttachOptions object is JSON-serializable.
    throw new Error('validated attach options did not serialize');
  }
  return encoded;
}

function encodeJson(kind: string, value: object): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    // invariant throw: validated option objects are JSON-serializable.
    throw new Error(`validated ${kind} did not serialize`);
  }
  return encoded;
}

function encodeAdoptOptions(options: AdoptOptions | undefined): string {
  return encodeJson('adopt options', typia.assert<AdoptOptions>(options ?? {}));
}

function encodeCreateOptions(options: CreateOptions | undefined): string {
  return encodeJson('create options', typia.assert<CreateOptions>(options ?? {}));
}

function encodeGrantDelta(delta: GrantDelta): string {
  return encodeJson('grant delta', typia.assert<GrantDelta>(delta));
}

function encodeRebaseOptions(options: RebaseOptions | undefined): string {
  return encodeJson('rebase options', typia.assert<RebaseOptions>(options ?? {}));
}

function encodeLandOptions(options: LandOptions | undefined): string {
  return encodeJson('land options', typia.assert<LandOptions>(options ?? {}));
}

function encodeRemoveOptions(options: RemoveOptions | undefined): string {
  return encodeJson('remove options', typia.assert<RemoveOptions>(options ?? {}));
}

function encodeGcOptions(options: GcOptions | undefined): string {
  return encodeJson('GC options', typia.assert<GcOptions>(options ?? {}));
}

function encodeExecRequest(request: ExecRequest): string {
  return encodeJson('exec request', typia.assert<ExecRequest>(request));
}

function encodePushOptions(options: PushOptions | undefined): string {
  return encodeJson('push options', typia.assert<PushOptions>(options ?? {}));
}

class ProjectImpl implements Project {
  readonly #native: NativeProjectHandle;

  constructor(nativeProject: NativeProjectHandle) {
    this.#native = nativeProject;
  }

  get repoId(): string {
    return this.#native.repoId;
  }

  get gitRoot(): string {
    return this.#native.gitRoot;
  }

  async main(): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => this.#native.main()));
  }

  async workspace(name: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => this.#native.workspace(name)));
  }

  async listWorkspaces(): Promise<readonly WorkspaceInfo[]> {
    return parseWorkspaceInfos(await callNativeAsync(() => this.#native.listWorkspaces()));
  }
}

class WorkspaceRefImpl implements WorkspaceRef {
  readonly #native: NativeWorkspaceRefHandle;

  constructor(nativeWorkspace: NativeWorkspaceRefHandle) {
    this.#native = nativeWorkspace;
  }

  get name(): string {
    return this.#native.name;
  }

  get mountPath(): string {
    return this.#native.mountPath;
  }

  async info(): Promise<WorkspaceInfo> {
    return parseWorkspaceInfo(await callNativeAsync(() => this.#native.infoJson()));
  }

  async ensure(): Promise<EnsureReport> {
    return parseEnsureReport(await callNativeAsync(() => this.#native.ensureJson()));
  }

  async attach(options?: AttachOptions): Promise<void> {
    await callNativeAsync(() => this.#native.attach(encodeAttachOptions(options)));
  }

  async grants(): Promise<GrantSet> {
    return parseGrantSet(await callNativeAsync(() => this.#native.grantsJson()));
  }
}

class CoordinatorImpl implements Coordinator {
  readonly #native: NativeCoordinatorHandle;

  constructor(nativeCoordinator: NativeCoordinatorHandle) {
    this.#native = nativeCoordinator;
  }

  async adopt(options?: AdoptOptions): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => this.#native.adopt(encodeAdoptOptions(options))));
  }

  async create(name: string, options?: CreateOptions): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => this.#native.create(name, encodeCreateOptions(options))));
  }

  async fork(source: string, destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => this.#native.fork(source, destination)));
  }

  async grant(workspace: string, delta: GrantDelta): Promise<GrantSet> {
    return parseGrantSet(await callNativeAsync(() => this.#native.grant(workspace, encodeGrantDelta(delta))));
  }

  async revoke(workspace: string, delta: GrantDelta): Promise<GrantSet> {
    return parseGrantSet(await callNativeAsync(() => this.#native.revoke(workspace, encodeGrantDelta(delta))));
  }

  async rebase(workspace: string, options?: RebaseOptions): Promise<string> {
    return callNativeAsync(() => this.#native.rebase(workspace, encodeRebaseOptions(options)));
  }

  async land(workspace: string, options?: LandOptions): Promise<LandReport> {
    return parseLandReport(await callNativeAsync(() => this.#native.land(workspace, encodeLandOptions(options))));
  }

  async restore(workspace: string, label: string): Promise<void> {
    await callNativeAsync(() => this.#native.restore(workspace, label));
  }

  async detach(workspace: string): Promise<void> {
    await callNativeAsync(() => this.#native.detach(workspace));
  }

  async destroy(workspace: string, options?: RemoveOptions): Promise<void> {
    await callNativeAsync(() => this.#native.destroy(workspace, encodeRemoveOptions(options)));
  }

  async gc(options?: GcOptions): Promise<GcReport> {
    return parseGcReport(await callNativeAsync(() => this.#native.gc(encodeGcOptions(options))));
  }

  async worker(workspace: string): Promise<WorkspaceHandle> {
    return new WorkspaceHandleImpl(await callNativeAsync(() => this.#native.worker(workspace)));
  }
}

class WorkspaceHandleImpl implements WorkspaceHandle {
  readonly #native: NativeWorkspaceHandle;

  constructor(nativeWorkspace: NativeWorkspaceHandle) {
    this.#native = nativeWorkspace;
  }

  get name(): string {
    return this.#native.name;
  }

  get mountPath(): string {
    return this.#native.mountPath;
  }

  async exec(request: ExecRequest): Promise<JobHandle> {
    return new JobHandleImpl(await callNativeAsync(() => this.#native.exec(encodeExecRequest(request))));
  }

  async shell(session?: string): Promise<Session> {
    return new SessionImpl(await callNativeAsync(() => this.#native.shell(session)));
  }

  async listJobs(): Promise<readonly JobInfo[]> {
    return parseJobInfos(await callNativeAsync(() => this.#native.listJobs()));
  }

  async job(id: number): Promise<JobHandle> {
    return new JobHandleImpl(await callNativeAsync(() => this.#native.job(id)));
  }

  async push(options?: PushOptions): Promise<PushReport> {
    return parsePushReport(await callNativeAsync(() => this.#native.push(encodePushOptions(options))));
  }

  async grants(): Promise<GrantSet> {
    return parseGrantSet(await callNativeAsync(() => this.#native.grantsJson()));
  }
}

class SessionImpl implements Session {
  readonly #native: NativeSessionHandle;

  constructor(nativeSession: NativeSessionHandle) {
    this.#native = nativeSession;
  }

  get isNamed(): boolean {
    return this.#native.isNamed;
  }

  async exec(request: ExecRequest): Promise<JobHandle> {
    return new JobHandleImpl(await callNativeAsync(() => this.#native.exec(encodeExecRequest(request))));
  }
}

class JobHandleImpl implements JobHandle {
  readonly #native: NativeJobHandle;

  constructor(nativeJob: NativeJobHandle) {
    this.#native = nativeJob;
  }

  get id(): number {
    return this.#native.id;
  }

  async status(): Promise<JobInfo> {
    return parseJobInfo(await callNativeAsync(() => this.#native.statusJson()));
  }

  async readLogs(stream: 'stdout' | 'stderr', follow = false): Promise<Uint8Array> {
    return callNativeAsync(() => this.#native.readLogs(stream, follow));
  }

  async attach(): Promise<JobAttachment> {
    return new JobAttachmentImpl(await callNativeAsync(() => this.#native.attach()));
  }

  async detach(): Promise<void> {
    await callNativeAsync(() => this.#native.detach());
  }

  async wait(): Promise<JobInfo> {
    return parseJobInfo(await callNativeAsync(() => this.#native.wait()));
  }

  async kill(): Promise<void> {
    await callNativeAsync(() => this.#native.kill());
  }
}

class JobAttachmentImpl implements JobAttachment {
  readonly #native: NativeJobAttachmentHandle;

  constructor(nativeAttachment: NativeJobAttachmentHandle) {
    this.#native = nativeAttachment;
  }

  async detach(): Promise<void> {
    await callNativeAsync(() => this.#native.detach());
  }
}

/** Takes ownership of an inherited controller descriptor. Dropping an unused endpoint closes it. */
export function coordinatorEndpoint(descriptor: number): CoordinatorEndpoint {
  return callNative(() => native.coordinatorEndpoint(descriptor));
}

/**
 * Opens a read-only, capability-scoped project over one authenticated inherited endpoint.
 * The endpoint is consumed even when the handshake or project open fails.
 */
export async function openProject(endpoint: CoordinatorEndpoint, path: string): Promise<Project> {
  return new ProjectImpl(await callNativeAsync(() => native.openProject(endpoint, path)));
}

/**
 * Connects with retained coordinator authority. The affine endpoint is consumed exactly once;
 * retain the returned coordinator for the full workspace and job lifecycle.
 */
export async function connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<Coordinator> {
  return new CoordinatorImpl(await callNativeAsync(() => native.connectCoordinator(endpoint, path)));
}
