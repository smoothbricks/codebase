/// <reference types="node" />

import { createRequire } from 'node:module';
import typia from 'typia';
import type { CoordinatorEndpoint } from './types.js';

export interface NativeProjectHandle {
  readonly repoId: string;
  readonly gitRoot: string;
  main(): Promise<NativeWorkspaceRefHandle>;
  workspace(name: string): Promise<NativeWorkspaceRefHandle>;
  listWorkspaces(): Promise<string>;
}

export interface NativeWorkspaceRefHandle {
  readonly name: string;
  readonly mountPath: string;
  infoJson(): Promise<string>;
  ensureJson(): Promise<string>;
  attach(optionsJson?: string): Promise<void>;
  grantsJson(): Promise<string>;
}

export interface NativeCoordinatorHandle {
  adopt(optionsJson: string): Promise<NativeWorkspaceRefHandle>;
  create(name: string, optionsJson: string): Promise<NativeWorkspaceRefHandle>;
  fork(source: string, destination: string): Promise<NativeWorkspaceRefHandle>;
  grant(workspace: string, deltaJson: string): Promise<string>;
  revoke(workspace: string, deltaJson: string): Promise<string>;
  rebase(workspace: string, optionsJson: string): Promise<string>;
  land(workspace: string, optionsJson: string): Promise<string>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  destroy(workspace: string, optionsJson: string): Promise<void>;
  gc(optionsJson: string): Promise<string>;
  worker(workspace: string): Promise<NativeWorkspaceHandle>;
}

export interface NativeWorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(requestJson: string): Promise<NativeJobHandle>;
  shell(session?: string): Promise<NativeSessionHandle>;
  listJobs(): Promise<string>;
  job(id: number): Promise<NativeJobHandle>;
  push(optionsJson: string): Promise<string>;
  grantsJson(): Promise<string>;
}

export interface NativeSessionHandle {
  readonly isNamed: boolean;
  exec(requestJson: string): Promise<NativeJobHandle>;
}

export interface NativeJobHandle {
  readonly id: number;
  statusJson(): Promise<string>;
  readLogs(stream: string, follow: boolean): Promise<Buffer>;
  attach(): Promise<NativeJobAttachmentHandle>;
  detach(): Promise<void>;
  wait(): Promise<string>;
  kill(): Promise<void>;
}

export interface NativeJobAttachmentHandle {
  detach(): Promise<void>;
}

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
  connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<NativeCoordinatorHandle>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

function nativeBinaryName(): string {
  switch (process.platform) {
    case 'darwin':
      if (process.arch === 'arm64' || process.arch === 'x64') {
        return `cowshed.darwin-${process.arch}.node`;
      }
      break;
    case 'linux':
      if (process.arch === 'arm64' || process.arch === 'x64') {
        return `cowshed.linux-${process.arch}-gnu.node`;
      }
      break;
  }
  throw new Error(`Unsupported Cowshed native target: ${process.platform}-${process.arch}`);
}

export function loadNativeModule(): NativeModule {
  const binaryName = nativeBinaryName();
  const override = process.env.COWSHED_NODE_PATH;
  const candidates = [
    ...(override ? [override] : []),
    new URL(`../dist/${binaryName}`, import.meta.url).pathname,
    new URL(`./${binaryName}`, import.meta.url).pathname,
  ];
  const require = createRequire(import.meta.url);
  let lastError: unknown;

  for (const path of candidates) {
    try {
      return assertNativeModule(require(path));
    } catch (error) {
      lastError = error;
    }
  }

  throw new Error(`Could not load ${binaryName}. Run \`nx run cowshed:cargo-napi\` for this platform.`, {
    cause: lastError,
  });
}
