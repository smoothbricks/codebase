import typia from 'typia';
import type * as Api from './api.generated.js';
import { coordinatorExec, exec } from './exec.js';
import { launcherPath, packageRootFromModule, runLauncher } from './launcher.js';
import * as N from './native.generated.js';
import {
  type EventIterator,
  loadNativeModule,
  type NativeCoordinatorHandle,
  type NativeHostHandle,
  type NativeJobAttachmentHandle,
  type NativeJobHandle,
  type NativeProjectHandle,
  type NativeSccacheHostHandle,
  type NativeWorkspaceHandle,
  type NativeWorkspaceRefHandle,
} from './native.js';
import {
  type Adopted,
  type Coordinator,
  type CoordinatorEndpoint,
  CowshedError,
  type CredentialAddCall,
  type ExecRequest,
  type Host,
  type HostAttachAllArguments,
  type HostDiskCommandArguments,
  type HostListProjectsArguments,
  type HostSkillInstallArguments,
  type JobAttachment,
  type JobAttachOptions,
  type JobHandle,
  type JobLogs,
  type LandOptions,
  type PathOptions,
  type Project,
  type ProjectListArguments,
  type RebaseOptions,
  type SccacheHost,
  type SccacheStartArguments,
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
 * rather than dressed up with an invented hint. Every typed refusal detail follows the generated
 * canonical CowshedError declaration without a JSON-string roundtrip.
 */
type NativeError = Api.CowshedError;

const native = loadNativeModule();
const isNativeError = typia.createIs<NativeError>();

function normalizeNativeError(error: unknown): unknown {
  if (!isNativeError(error)) {
    return error;
  }

  return new CowshedError(error.code, error.message, error.hint, {
    cause: error,
    admission: error.admission,
    cpuAdmission: error.cpuAdmission,
    priority: error.priority,
    stdin: error.stdin,
  });
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

  async workspaceInSlot(slot: number): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.projectWorkspaceInSlot(this.#native, { slot })));
  }

  async path(name: string, options?: PathOptions): Promise<Api.WorkspaceInfo> {
    const workspace = await this.workspace(name);
    if (!(options?.noAttach ?? false)) {
      await workspace.attach();
    }
    return workspace.info();
  }

  async listWorkspaces(options?: ProjectListArguments): Promise<readonly Api.WorkspaceInfo[]> {
    const views = await callNativeAsync(() => N.projectList(this.#native, options ?? {}));
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

class HostImpl implements Host {
  readonly #native: NativeHostHandle;

  constructor(nativeHost: NativeHostHandle) {
    this.#native = nativeHost;
  }

  async listProjects(options?: HostListProjectsArguments): Promise<readonly Api.ProjectWorkspaces[]> {
    return callNativeAsync(() => N.hostListProjects(this.#native, options ?? {}));
  }

  async attachAll(options?: HostAttachAllArguments): Promise<readonly Api.WorkspaceInfo[]> {
    return callNativeAsync(() => N.hostAttachAll(this.#native, options ?? { browse: false }));
  }

  async detachAll(): Promise<void> {
    await callNativeAsync(() => N.hostDetachAll(this.#native, {}));
  }

  async clearAttachBudget(): Promise<void> {
    await callNativeAsync(() => N.hostClearAttachBudget(this.#native, {}));
  }

  async forgetAbandoned(repoId: Api.RepoRequest['repoId']): Promise<Api.ProjectRetirementReport> {
    return callNativeAsync(() => N.hostForgetAbandoned(this.#native, { repoId }));
  }

  async mountMain(repoId: Api.RepoRequest['repoId']): Promise<Api.MountResult> {
    return callNativeAsync(() => N.hostMountMain(this.#native, { repoId }));
  }

  async gatewayStatus(): Promise<Api.GatewayStatus> {
    return callNativeAsync(() => N.hostGatewayStatus(this.#native, {}));
  }

  async credentialStatus(project: Api.ProjectPathRequest['project']): Promise<Api.CredentialReport> {
    return callNativeAsync(() => N.hostCredentialStatus(this.#native, { project }));
  }

  async credentialRemove(
    project: Api.CredentialRemoveRequest['project'],
    origin: Api.CredentialRemoveRequest['origin'],
  ): Promise<Api.CredentialReport> {
    return callNativeAsync(() => N.hostCredentialRemove(this.#native, { project, origin }));
  }

  async identityAdd(
    project: Api.IdentityAddRequest['project'],
    remote: Api.IdentityAddRequest['remote'],
  ): Promise<Api.IdentityReport> {
    return callNativeAsync(() => N.hostIdentityAdd(this.#native, { project, remote }));
  }

  async skillInstall(options?: HostSkillInstallArguments): Promise<Api.SkillInstallReport> {
    return callNativeAsync(() => N.hostSkillInstall(this.#native, options ?? {}));
  }

  async buildState(project: Api.ProjectPathRequest['project']): Promise<Api.BuildStateListing> {
    return callNativeAsync(() => N.hostBuildState(this.#native, { project }));
  }

  async clearNxStore(project: Api.ProjectPathRequest['project']): Promise<Api.NxStoreClearResult> {
    return callNativeAsync(() => N.hostClearNxStore(this.#native, { project }));
  }

  async gatewayStart(): Promise<Api.GatewayStatus> {
    return callNativeAsync(() => N.hostGatewayStart(this.#native, {}));
  }

  async gatewayStop(options?: Partial<Api.GatewayStopRequest>): Promise<void> {
    await callNativeAsync(() => N.hostGatewayStop(this.#native, { purge: options?.purge ?? false }));
  }

  async lockHolders(path: Api.HostPathRequest['path']): Promise<Api.LockObservation> {
    return callNativeAsync(() => N.hostLockHolders(this.#native, { path }));
  }

  async treeHolders(path: Api.HostPathRequest['path']): Promise<readonly Api.TreeHolder[]> {
    return callNativeAsync(() => N.hostTreeHolders(this.#native, { path }));
  }

  async retireDescendants(
    handle: Api.RetireDescendantsRequest['handle'],
    graceMs: Api.RetireDescendantsRequest['graceMs'],
  ): Promise<Api.JobSubtreeRetirement> {
    return callNativeAsync(() => N.hostRetireDescendants(this.#native, { handle, graceMs }));
  }

  async credentialAdd(...call: CredentialAddCall): Promise<Api.CredentialReport> {
    const [request, secret] = call;
    return callNativeAsync(() => N.hostCredentialAdd(this.#native, request, secret));
  }

  diskCommand(request: HostDiskCommandArguments): AsyncIterable<Api.DiskCommandEvent> {
    return callNativeEvents(N.hostDiskCommand(this.#native, request));
  }

  async close(): Promise<void> {
    await callNativeAsync(() => this.#native.close());
  }
}

class SccacheHostImpl implements SccacheHost {
  readonly #native: NativeSccacheHostHandle;

  constructor(nativeHost: NativeSccacheHostHandle) {
    this.#native = nativeHost;
  }

  async start(options?: SccacheStartArguments): Promise<Api.SccacheStatus> {
    return callNativeAsync(() => N.sccacheStart(this.#native, options ?? {}));
  }

  async stop(): Promise<void> {
    await callNativeAsync(() => N.sccacheStop(this.#native, {}));
  }

  async status(): Promise<Api.SccacheStatus> {
    return callNativeAsync(() => N.sccacheStatus(this.#native, {}));
  }

  async close(): Promise<void> {
    await callNativeAsync(() => this.#native.close());
  }
}

class CoordinatorImpl implements Coordinator {
  readonly #native: NativeCoordinatorHandle;

  constructor(nativeCoordinator: NativeCoordinatorHandle) {
    this.#native = nativeCoordinator;
  }

  async adopt(options?: Api.AdoptOptions): Promise<Adopted> {
    const adopted = await callNativeAsync(() => N.coordinatorAdopt(this.#native, { options: options ?? {} }));
    return { workspace: new WorkspaceRefImpl(adopted.workspace), reAdoption: adopted.reAdoption };
  }

  async fork(workspace: string, options?: Partial<Api.ForkOptions> & { source?: string }): Promise<WorkspaceRef> {
    const { source, ...requested } = options ?? {};
    const mint: Api.ForkOptions = {
      clean: false,
      browse: false,
      register: false,
      gitWorktree: false,
      ...requested,
    };
    return new WorkspaceRefImpl(
      await callNativeAsync(() => N.coordinatorFork(this.#native, { workspace, source, options: mint })),
    );
  }

  async rename(source: string, destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(
      await callNativeAsync(() => N.coordinatorRename(this.#native, { source, destination })),
    );
  }

  async moveCheckout(destination: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.coordinatorMoveCheckout(this.#native, { destination })));
  }

  async changeRepoId(newRepoId: string): Promise<WorkspaceRef> {
    return new WorkspaceRefImpl(await callNativeAsync(() => N.coordinatorChangeRepoId(this.#native, { newRepoId })));
  }

  async grant(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.coordinatorGrant(this.#native, { workspace, delta }));
  }

  async revoke(workspace: string, delta: Api.GrantDelta): Promise<Api.GrantSet> {
    return callNativeAsync(() => N.coordinatorRevoke(this.#native, { workspace, delta }));
  }

  async projectGrants(): Promise<Api.ProjectGrants> {
    return callNativeAsync(() => N.coordinatorProjectGrants(this.#native, {}));
  }

  async grantProject(delta: Api.ProjectGrantDelta): Promise<Api.ProjectGrants> {
    return callNativeAsync(() => N.coordinatorGrantProject(this.#native, { delta }));
  }

  async revokeProject(delta: Api.ProjectGrantDelta): Promise<Api.ProjectGrants> {
    return callNativeAsync(() => N.coordinatorRevokeProject(this.#native, { delta }));
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

  async defragment(workspace: string): Promise<Api.DefragmentResult> {
    return callNativeAsync(() => N.coordinatorDefragment(this.#native, { workspace }));
  }

  async clearNxState(workspace: string): Promise<Api.NxStateClearResult> {
    return callNativeAsync(() => N.coordinatorClearNxState(this.#native, { workspace }));
  }

  async rekey(workspace: string): Promise<Api.RekeyResult> {
    return callNativeAsync(() => N.coordinatorRekey(this.#native, { workspace }));
  }

  async reseed(workspace: string, from: string, dryRun: boolean): Promise<Api.ReseedResult> {
    return callNativeAsync(() => N.coordinatorReseed(this.#native, { workspace, from, dryRun }));
  }

  async remove(workspace: string, options?: Api.RemoveOptions): Promise<Api.RemoveReport> {
    return callNativeAsync(() => N.coordinatorDestroy(this.#native, { workspace, options: options ?? {} }));
  }

  async removeProject(options?: Api.RemoveProjectOptions): Promise<Api.RemoveProjectReport> {
    return callNativeAsync(() => N.coordinatorRemoveProject(this.#native, { options: options ?? {} }));
  }

  async gc(options?: Api.GcOptions): Promise<Api.GcReport> {
    return callNativeAsync(() => N.coordinatorGc(this.#native, { options: options ?? {} }));
  }

  async doctor(): Promise<Api.DoctorReport> {
    return callNativeAsync(() => N.coordinatorDoctor(this.#native, {}));
  }

  async exec(worker: WorkspaceHandle, request: ExecRequest): Promise<JobHandle> {
    return WorkspaceHandleImpl.execAsCoordinator(this.#native, worker, request);
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

  /**
   * Admits one job through coordinator.exec in the incarnation a worker capability was minted for. A handle this module
   * did not mint has no resolved incarnation behind it, so it is refused rather than trusted by name.
   */
  static async execAsCoordinator(
    coordinator: NativeCoordinatorHandle,
    worker: WorkspaceHandle,
    request: ExecRequest,
  ): Promise<JobHandle> {
    if (!(#native in worker)) {
      throw new CowshedError(
        'usage',
        `workspace ${worker.name} is not a capability this project minted`,
        'mint it with coordinator.worker(name) and pass that handle',
      );
    }
    const fence = { workspace: worker.#native.name, workspaceIncarnation: worker.#native.workspaceIncarnation };
    const id = await callNativeAsync(() => coordinatorExec(coordinator, fence, request));
    return worker.job(id);
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

  async jobByKey(admissionKey: Api.AdmissionKey): Promise<JobHandle | null> {
    const job = await callNativeAsync(() => N.workerJobByKey(this.#native, { admissionKey }));
    return job === null ? null : new JobHandleImpl(job);
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

  async resources(): Promise<Api.JobResourceSample> {
    return callNativeAsync(() => N.jobResources(this.#native, {}));
  }

  progress(everyMs: number): AsyncIterable<Api.JobResourceSample> {
    return callNativeEvents(N.jobProgress(this.#native, { everyMs }));
  }

  async processes(): Promise<Api.JobProcessTree> {
    return callNativeAsync(() => N.jobProcesses(this.#native, {}));
  }

  processEvents(everyMs: number): AsyncIterable<Api.JobProcessEvent> {
    return callNativeEvents(N.jobProcessEvents(this.#native, { everyMs }));
  }

  async logs(args: N.JobLogsArguments): Promise<JobLogs> {
    return callNativeAsync(() => N.jobLogs(this.#native, args));
  }

  async listeningPorts(): Promise<Api.JobListeningPorts> {
    return callNativeAsync(() => N.jobListeningPorts(this.#native, {}));
  }

  async tail(cursor: Api.JobJournalCursor | undefined, limits: Api.JobTailLimits): Promise<Api.JobTail> {
    // An undefined cursor is omitted from the request's JSON: the latest tail, never `null`.
    return callNativeAsync(() => N.jobTail(this.#native, { cursor, limits }));
  }

  async attach(options?: JobAttachOptions): Promise<JobAttachment> {
    return new JobAttachmentImpl(await callNativeAsync(() => N.jobHandleAttach(this.#native, options?.cursor)));
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

class JobAttachmentImpl implements JobAttachment {
  readonly #native: NativeJobAttachmentHandle;

  constructor(nativeAttachment: NativeJobAttachmentHandle) {
    this.#native = nativeAttachment;
  }

  async write(chunk: Uint8Array): Promise<void> {
    await callNativeAsync(() => N.jobAttachmentWrite(this.#native, chunk));
  }

  async end(): Promise<void> {
    await callNativeAsync(() => N.jobAttachmentEnd(this.#native));
  }

  async detach(): Promise<void> {
    await callNativeAsync(() => N.jobAttachmentDetach(this.#native));
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
 * Starts `cowshed host` through the package's `bin/cowshed` launcher — the binary every verb runs —
 * on a socketpair, and connects to it. Its stdout and stderr are this process's; closing the
 * connection ends it.
 */
export async function openHost(): Promise<Host> {
  const launcher = launcherPath(packageRootFromModule(import.meta.url));
  return new HostImpl(await callNativeAsync(() => native.openHost(launcher)));
}

/**
 * Starts `cowshed sccache host` through the package's `bin/cowshed` launcher, which runs the
 * installed build for every `sccache` verb, on a socketpair, and connects to it. Its stdout and
 * stderr are this process's; closing the connection ends it.
 */
export async function openSccacheHost(): Promise<SccacheHost> {
  const launcher = launcherPath(packageRootFromModule(import.meta.url));
  return new SccacheHostImpl(await callNativeAsync(() => native.openSccacheHost(launcher)));
}

/**
 * Runs any CLI verb with the exact argv, stdout/stderr, and exit-code contract of the standalone
 * binary, because it *is* the standalone binary: this runs the package's `bin/cowshed` launcher,
 * which picks the same packaged `cowshed` the `cowshed` command runs.
 *
 * For typed calls to the host's own verbs use `openHost`; this runs a verb with the binary's exact
 * argv, stdout and exit status. It is not an in-process runtime: the addon does not link the CLI,
 * so there is exactly one implementation of every verb and exactly one place a missing binary is
 * reported from.
 */
export async function runCli(argv: readonly string[]): Promise<number> {
  return runLauncher(packageRootFromModule(import.meta.url), typia.assert<readonly string[]>(argv));
}
