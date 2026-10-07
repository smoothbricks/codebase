/// <reference types="node" />

import { createRequire } from 'node:module';
import typia from 'typia';
import type {
  NativeCoordinatorOperations,
  NativeJobHandleOperations,
  NativeProjectOperations,
  NativeWorkspaceHandleOperations,
  NativeWorkspaceRefOperations,
} from './native.generated.js';
import { platformDirectory } from './platform.js';
import type { CoordinatorEndpoint } from './types.js';

// Every controller operation's method is declared in `native.generated.ts`, from the operation
// table the addon's adapters are generated from, and each handle extends exactly the operations
// its authority admits. What follows is only what no operation declares: each handle's identity,
// read from the call that minted it.

export interface NativeProjectHandle extends NativeProjectOperations {
  readonly repoId: string;
  readonly gitRoot: string;
}

export interface NativeWorkspaceRefHandle extends NativeWorkspaceRefOperations {
  readonly name: string;
  readonly mountPath: string;
  /** The reference as a land or rebase target, pinned to the incarnation it was resolved at. */
  readonly targetJson: string;
}

export type NativeCoordinatorHandle = NativeCoordinatorOperations;

export interface NativeWorkspaceHandle extends NativeWorkspaceHandleOperations {
  readonly name: string;
  readonly mountPath: string;
}

export interface NativeJobHandle extends NativeJobHandleOperations {
  readonly id: number;
}

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
  connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<NativeCoordinatorHandle>;
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
  // napi-rs loaders honour NAPI_RS_NATIVE_LIBRARY_PATH first; this is that same hook, not a
  // bespoke cowshed name. The override stays — only the spelling is the ecosystem one.
  const override = process.env.NAPI_RS_NATIVE_LIBRARY_PATH;
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
