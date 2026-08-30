/// <reference types="node" />

import { createRequire } from 'node:module';
import typia from 'typia';
import { platformDirectory } from './platform.js';
import type { CoordinatorEndpoint } from './types.js';

export interface NativeProjectHandle {
  readonly repoId: string;
  readonly gitRoot: string;
  main(): Promise<NativeWorkspaceRefHandle>;
  workspace(name: string): Promise<NativeWorkspaceRefHandle>;
  workspaceAt(path: string): Promise<NativeWorkspaceRefHandle>;
  path(name: string, noAttach: boolean): Promise<string>;
  listWorkspaces(): Promise<string>;
}

export interface NativeWorkspaceRefHandle {
  readonly name: string;
  readonly mountPath: string;
  infoJson(): Promise<string>;
  attach(optionsJson?: string): Promise<void>;
  grantsJson(): Promise<string>;
}

export interface NativeCoordinatorHandle {
  adopt(optionsJson: string): Promise<NativeWorkspaceRefHandle>;
  create(name: string, optionsJson: string): Promise<NativeWorkspaceRefHandle>;
  fork(source: string, destination: string): Promise<NativeWorkspaceRefHandle>;
  rename(source: string, destination: string): Promise<NativeWorkspaceRefHandle>;
  moveCheckout(destination: string): Promise<NativeWorkspaceRefHandle>;
  grant(workspace: string, deltaJson: string): Promise<string>;
  revoke(workspace: string, deltaJson: string): Promise<string>;
  rebase(workspace: string, optionsJson: string): Promise<string>;
  land(workspace: string, optionsJson: string): Promise<string>;
  restore(workspace: string, label: string): Promise<void>;
  detach(workspace: string): Promise<void>;
  resize(workspace: string, capacity: string): Promise<string>;
  remove(workspace: string, optionsJson: string): Promise<string>;
  gc(optionsJson: string): Promise<string>;
  doctor(): Promise<string>;
  worker(workspace: string): Promise<NativeWorkspaceHandle>;
}

export interface NativeWorkspaceHandle {
  readonly name: string;
  readonly mountPath: string;
  exec(requestJson: string): Promise<NativeJobHandle>;
  shell(session?: string): Promise<NativeSessionHandle>;
  listJobs(): Promise<string>;
  job(id: number): Promise<NativeJobHandle>;
  checkpoint(optionsJson: string): Promise<string>;
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
  runCli(argv: readonly string[]): Promise<number>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

interface NativeBinary {
  directory: string;
  fileName: string;
}

function nativeBinary(): NativeBinary {
  const directory = platformDirectory(process.platform, process.arch);
  if (directory === null) {
    throw new Error(`Unsupported Cowshed native target: ${process.platform}-${process.arch}`);
  }
  return { directory, fileName: `cowshed.${directory}.node` };
}

export function loadNativeModule(): NativeModule {
  const { directory, fileName } = nativeBinary();
  const override = process.env.COWSHED_NODE_PATH;
  // NAPI_DEBUG_ADDON is set only by the inferred napi-test target: the test suite loads the
  // dev-profile addon from .cache/native-debug (never packaged; `files` ships dist/ wholesale)
  // instead of the release artifacts. Both URL depths cover running from src/ and dist/ts/.
  const debugCandidates =
    process.env.NAPI_DEBUG_ADDON === '1'
      ? [
          new URL(`../.cache/native-debug/${fileName}`, import.meta.url).pathname,
          new URL(`../../.cache/native-debug/${fileName}`, import.meta.url).pathname,
        ]
      : [];
  const candidates = [
    ...(override ? [override] : []),
    ...debugCandidates,
    new URL(`../dist/native/host/${fileName}`, import.meta.url).pathname,
    new URL(`../dist/native/${directory}/${fileName}`, import.meta.url).pathname,
    new URL(`../native/host/${fileName}`, import.meta.url).pathname,
    new URL(`../native/${directory}/${fileName}`, import.meta.url).pathname,
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

  throw new Error(`Could not load ${fileName}. Run \`nx build cowshed\` for this platform.`, {
    cause: lastError,
  });
}
