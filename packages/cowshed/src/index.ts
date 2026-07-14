import typia from 'typia';
import { loadNativeModule, type NativeProjectHandle, type NativeWorkspaceRefHandle } from './native.js';
import {
  type AttachOptions,
  type CoordinatorEndpoint,
  CowshedError,
  type EnsureReport,
  type ErrorCode,
  type GrantSet,
  type Project,
  type WorkspaceInfo,
  type WorkspaceRef,
} from './types.js';

export {
  type AttachOptions,
  type CheckpointInfo,
  type CoordinatorEndpoint,
  CowshedError,
  type EgressMode,
  type EgressRule,
  type EnsureAction,
  type EnsureReport,
  type ErrorCode,
  type GrantSet,
  type ImageFormat,
  type PortBlock,
  type Project,
  type SimVerb,
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
