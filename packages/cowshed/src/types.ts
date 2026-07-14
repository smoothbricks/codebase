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

/**
 * Affine inherited descriptor accepted by the controller handshake.
 * The value owns the descriptor and may be consumed by exactly one openProject call.
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

export interface WorkspaceRef {
  readonly name: string;
  readonly mountPath: string;
  info(): Promise<WorkspaceInfo>;
  ensure(): Promise<EnsureReport>;
  attach(options?: AttachOptions): Promise<void>;
  grants(): Promise<GrantSet>;
}
