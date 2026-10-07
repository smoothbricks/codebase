import typia from 'typia';
import type * as Api from './api.generated.js';
import { exec } from './exec.js';
import { packageRootFromModule, runLauncher } from './launcher.js';
import * as N from './native.generated.js';
import {
  type EventIterator,
  loadNativeModule,
  type NativeCoordinatorHandle,
  type NativeJobHandle,
  type NativeProjectHandle,
  type NativeWorkspaceHandle,
  type NativeWorkspaceRefHandle,
} from './native.js';
import {
  type Coordinator,
  type CoordinatorEndpoint,
  CowshedError,
  type ErrorCode,
  type ExecRequest,
  type JobHandle,
  type JobLogs,
  type LandOptions,
  type PathOptions,
  type Project,
  type RebaseOptions,
  type Session,
  type WorkspaceHandle,
  type WorkspaceRef,
} from './types.js';
import { parseWorkspaceTarget } from './validators.generated.js';

export type * from './types.js';
export { CowshedError } from './types.js';

/**
 * The napi rejection shape. `hint` is a real property on the JS `Error`, set by `to_napi_error`
 * in crates/cowshed-napi/src/lib.rs — not a suffix on `message` behind a delimiter both languages
 * had to spell identically. An error missing any of the three is not ours and is rethrown as-is
 * rather than dressed up with an invented hint.
 */
interface NativeError {
  readonly code: ErrorCode;
  readonly message: string;
  readonly hint: string;
}

const native = loadNativeModule();
const isNativeError = typia.createIs<NativeError>();

function normalizeNativeError(error: unknown): unknown {
  if (!isNativeError(error)) {
    return error;
  }

  return new CowshedError(error.code, error.message, error.hint, { cause: error });
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

/**
 * A native stream's events, its rejections normalized as `callNativeAsync` normalizes a call's.
 * `return` reaches the native iterator at once, never behind a `next` in flight.
 */
function callNativeEvents<T>(events: EventIterator<T>): EventIterator<T> {
  const iterator: EventIterator<T> = {
    next: () => callNativeAsync(() => events.next()),
    return: () => callNativeAsync(() => events.return()),
    [Symbol.asyncIterator]: () => iterator,
  };
  return iterator;
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
    return this.workspace('main');
  }

  async workspace(name: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.projectWorkspace(this.#native, { workspace: name })));
  }

  async workspaceAt(path: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.projectWorkspaceAt(this.#native, { path })));
  }

  async path(name: string, options?: PathOptions): Promise<Api.WorkspaceInfo> {
    const workspace = await this.workspace(name);
    if (!(options?.noAttach ?? false)) {
      await workspace.attach();
    }
    return workspace.info();
  }

  async listWorkspaces(): Promise<readonly Api.WorkspaceInfo[]> {
    const views = await callNativeAsync(() => N.projectList(this.#native, {}));
    return views.map((view) => view.info);
  }
}

class WorkspaceRefImpl implements WorkspaceRef {
  readonly #native: NativeWorkspaceRefHandle;

  constructor(nativeWorkspace: NativeWorkspaceRefHandle) {
    this.#native = nativeWorkspace;
  }

  /**
   * The incarnation-pinned target behind a reference this module handed out. A `WorkspaceRef`
   * built anywhere else has no resolved incarnation behind it, so it is refused rather than
   * trusted by name.
   */
  static targetOf(reference: WorkspaceRef): Api.WorkspaceTarget {
    if (!(#native in reference)) {
      throw new CowshedError(
        'usage',
        `workspace ${reference.name} is not a reference this project resolved`,
        'resolve the workspace with project.workspace(name) and pass that reference',
      );
    }
    return parseWorkspaceTarget(reference.#native.targetJson);
  }

  get name(): string {
    return this.#native.name;
  }

  get mountPath(): string {
    return this.#native.mountPath;
  }

  async info(): Promise<Api.WorkspaceInfo> {
    return callNativeAsync(() => N.workspaceInfo(this.#native, {}));
  }

  async attach(options?: Api.AttachOptions): Promise<void> {
    await callNativeAsync(() => N.workspaceAttach(this.#native, { options: options ?? {} }));
  }

  async grants(): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.workspaceGrants(this.#native, {}));
  }
}

class CoordinatorImpl implements Coordinator {
  readonly #native: NativeCoordinatorHandle;

  constructor(nativeCoordinator: NativeCoordinatorHandle) {
    this.#native = nativeCoordinator;
  }

  async adopt(options?: Api.AdoptOptions): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(
      await callNativeAsync(() => N.coordinatorAdopt(this.#native, { options: options ?? {} })),
    );
  }

  async create(name: string, options?: Api.CreateOptions): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(
      await callNativeAsync(() => N.coordinatorCreate(this.#native, { workspace: name, options: options ?? {} })),
    );
  }

  async fork(source: string, destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.coordinatorFork(this.#native, { source, destination })));
  }

  async rename(source: string, destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(
      await callNativeAsync(() => N.coordinatorRename(this.#native, { source, destination })),
    );
  }

  async moveCheckout(destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.coordinatorMoveCheckout(this.#native, { destination })));
  }

  async grant(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.coordinatorGrant(this.#native, { workspace, delta }));
  }

  async revoke(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.coordinatorRevoke(this.#native, { workspace, delta }));
  }

  async rebase(workspace: string, options?: RebaseOptions): Promise<Api.RebaseReport> {
    // `into` with `onto` is refused by the controller, the one place that decides a destination.
    const { into, ...rest } = options ?? {};
    const target = into === undefined ? undefined : WorkspaceRefImpl.targetOf(into);
    return callNativeAsync(() => N.coordinatorRebase(this.#native, { workspace, into: target, options: rest }));
  }

  async land(workspace: string, options?: LandOptions): Promise<Api.LandReport> {
    const { into, ...rest } = options ?? {};
    const target = into === undefined ? undefined : WorkspaceRefImpl.targetOf(into);
    return callNativeAsync(() => N.coordinatorLand(this.#native, { workspace, into: target, options: rest }));
  }

  async restore(workspace: string, label: string): Promise<void> {
    await callNativeAsync(() => N.coordinatorRestore(this.#native, { workspace, label }));
  }

  async detach(workspace: string): Promise<void> {
    await callNativeAsync(() => N.coordinatorDetach(this.#native, { workspace }));
  }

  async resize(workspace: string, capacity: string, volume: Api.ResizeVolume): Promise<Api.ResizeResult> {
    return callNativeAsync(() => N.coordinatorResize(this.#native, { workspace, capacity, volume }));
  }

  async remove(workspace: string, options?: Api.RemoveOptions): Promise<Api.RemoveReport> {
    return callNativeAsync(() => N.coordinatorDestroy(this.#native, { workspace, options: options ?? {} }));
  }

  async gc(options?: Api.GcOptions): Promise<Api.GcReport> {
    return callNativeAsync(() => N.coordinatorGc(this.#native, { options: options ?? {} }));
  }

  async doctor(): Promise<Api.DoctorReport> {
    return callNativeAsync(() => N.coordinatorDoctor(this.#native, {}));
  }

  async worker(workspace: string): Promise<WorkspaceHandle> {
    return new WorkspaceHandleImpl(await callNativeAsync(() => N.coordinatorWorker(this.#native, { workspace })));
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
    return new JobHandleImpl(await callNativeAsync(() => exec(this.#native, null, request)));
  }

  async shell(session?: string): Promise<Session> {
    const name = session ?? null;
    await callNativeAsync(() => N.workerShell(this.#native, { session: name }));
    return new SessionImpl(this.#native, name);
  }

  async listJobs(): Promise<readonly Api.JobInfo[]> {
    return callNativeAsync(() => N.workerListJobs(this.#native, {}));
  }

  async job(id: number): Promise<JobHandle> {
    return new JobHandleImpl(await callNativeAsync(() => N.workerJob(this.#native, { jobId: id })));
  }

  async checkpoint(options?: Api.CheckpointOptions): Promise<string> {
    const { label } = await callNativeAsync(() => N.workerCheckpoint(this.#native, { options: options ?? {} }));
    return label;
  }

  async push(options?: Api.PushOptions): Promise<Api.PushReport> {
    return callNativeAsync(() => N.workerPush(this.#native, { options: options ?? {} }));
  }

  async grants(): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.workspaceGrants(this.#native, {}));
  }
}

/** A shell session: the worker it was opened through, and its name, which each exec names. */
class SessionImpl implements Session {
  readonly #worker: NativeWorkspaceHandle;
  readonly #name: string | null;

  constructor(worker: NativeWorkspaceHandle, name: string | null) {
    this.#worker = worker;
    this.#name = name;
  }

  get isNamed(): boolean {
    return this.#name !== null;
  }

  async exec(request: ExecRequest): Promise<JobHandle> {
    return new JobHandleImpl(await callNativeAsync(() => exec(this.#worker, this.#name, request)));
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

  async status(): Promise<Api.JobInfo> {
    return callNativeAsync(() => N.jobStatus(this.#native, {}));
  }

  progress(everyMs: number): AsyncIterable<Api.JobResourceSample> {
    return callNativeEvents(N.jobProgress(this.#native, { everyMs }));
  }

  async logs(args: N.JobLogsArguments): Promise<JobLogs> {
    return callNativeAsync(() => N.jobLogs(this.#native, args));
  }

  async tail(cursor: Api.JobJournalCursor | undefined, limits: Api.JobTailLimits): Promise<Api.JobTail> {
    // An undefined cursor is omitted from the request's JSON: the latest tail, never `null`.
    return callNativeAsync(() => N.jobTail(this.#native, { cursor, limits }));
  }

  async detach(): Promise<void> {
    await callNativeAsync(() => N.jobDetach(this.#native, {}));
  }

  async wait(): Promise<Api.JobInfo> {
    return callNativeAsync(() => N.jobWait(this.#native, {}));
  }

  async kill(): Promise<void> {
    await callNativeAsync(() => N.jobKill(this.#native, {}));
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

/**
 * Runs any CLI verb with the exact argv, stdout/stderr, and exit-code contract of the standalone
 * binary, because it *is* the standalone binary: this runs the package's `bin/cowshed` launcher,
 * which picks the same packaged `cowshed` the `cowshed` command runs.
 *
 * This is the escape hatch for host-management verbs — gateway, sccache, skill, setup, version,
 * help — that do not belong to a project capability. It is not an in-process runtime: the addon
 * does not link the CLI, so there is exactly one implementation of these verbs and exactly one
 * place a missing binary is reported from.
 */
export async function runCli(argv: readonly string[]): Promise<number> {
  return runLauncher(packageRootFromModule(import.meta.url), typia.assert<readonly string[]>(argv));
}
