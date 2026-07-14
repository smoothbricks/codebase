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

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

export function loadNativeModule(): NativeModule {
  const override = process.env.COWSHED_NODE_PATH;
  const candidates = [
    ...(override ? [override] : []),
    new URL('../dist/cowshed.node', import.meta.url).pathname,
    new URL('./cowshed.node', import.meta.url).pathname,
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

  throw new Error('Could not load cowshed.node. Run `nx run cowshed:cargo-napi` for this platform.', {
    cause: lastError,
  });
}
