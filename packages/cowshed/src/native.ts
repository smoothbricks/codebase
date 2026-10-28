/// <reference types="node" />

import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import typia from 'typia';
import type {
  NativeCoordinatorOperations,
  NativeEvents,
  NativeHostOperations,
  NativeJobAttachmentCapabilities,
  NativeJobHandleCapabilities,
  NativeJobHandleOperations,
  NativeProjectOperations,
  NativeWorkspaceHandleOperations,
  NativeWorkspaceRefOperations,
} from './native.generated.js';
import { addonDirectory, addonSearch } from './platform.js';
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

/** A host connection's adapters and the close that ends it. */
export type NativeHostHandle = NativeHostOperations & { close(): Promise<void> };

export interface NativeWorkspaceHandle extends NativeWorkspaceHandleOperations {
  readonly workspaceIncarnation: string;
  readonly name: string;
  readonly mountPath: string;
}

export interface NativeJobHandle extends NativeJobHandleOperations, NativeJobHandleCapabilities {
  readonly id: number;
}

/**
 * A stream-lane call's events as an async iterator. Unlike an async generator, whose `return`
 * waits behind a `next` in flight, `return` here closes the call at once.
 */
export interface EventIterator<T> {
  next(): Promise<IteratorResult<T, undefined>>;
  return(): Promise<IteratorResult<T, undefined>>;
  [Symbol.asyncIterator](): EventIterator<T>;
}

/**
 * Iterates the call `open` starts, each event parsed by `parse`. The first `next` opens the call
 * and each `next` sends one demand; `return` closes the call -- even while a `next` waits for its
 * event, which then resolves done -- and opens none that was never opened. A `next` whose event
 * fails to parse closes the call before it rejects.
 */
export function eventIterator<T>(open: () => Promise<NativeEvents>, parse: (json: string) => T): EventIterator<T> {
  const done: IteratorResult<T, undefined> = { done: true, value: undefined };
  let events: Promise<NativeEvents> | undefined;
  let returned = false;
  const iterator: EventIterator<T> = {
    async next() {
      if (returned) {
        return done;
      }
      events ??= open();
      const opened = await events;
      const event = await opened.next();
      if (event === null) {
        return done;
      }
      try {
        return { done: false, value: parse(event) };
      } catch (error) {
        await opened.close();
        throw error;
      }
    },
    async return() {
      returned = true;
      // A call that failed to open has no close to send; its failure rejected the `next` that
      // opened it.
      await events?.then(
        (opened) => opened.close(),
        () => undefined,
      );
      return done;
    },
    [Symbol.asyncIterator]: () => iterator,
  };
  return iterator;
}
export type NativeJobAttachmentHandle = NativeJobAttachmentCapabilities;

interface NativeModule {
  coordinatorEndpoint(descriptor: number): CoordinatorEndpoint;
  openProject(endpoint: CoordinatorEndpoint, path: string): Promise<NativeProjectHandle>;
  connectCoordinator(endpoint: CoordinatorEndpoint, path: string): Promise<NativeCoordinatorHandle>;
  openHost(launcher: string): Promise<NativeHostHandle>;
}

const assertNativeModule = typia.createAssert<NativeModule>();

interface NativeBinary {
  directory: string;
  fileName: string;
}

function nativeBinary(): NativeBinary {
  const directory = addonDirectory(process.platform, process.arch);
  if (directory === null) {
    throw new Error(`Unsupported Cowshed native target: ${process.platform}-${process.arch}`);
  }
  return { directory, fileName: `cowshed.${directory}.node` };
}

export function loadNativeModule(): NativeModule {
  const { directory, fileName } = nativeBinary();
  const { paths, producer } = addonSearch({
    modulePath: fileURLToPath(import.meta.url),
    exists: existsSync,
    directory,
    fileName,
    // napi-rs loaders honour NAPI_RS_NATIVE_LIBRARY_PATH first; this is that same hook, not a
    // bespoke cowshed name. The override stays — only the spelling is the ecosystem one.
    override: process.env.NAPI_RS_NATIVE_LIBRARY_PATH,
    // NAPI_DEBUG_ADDON is set only by the napi-test targets: the test suite loads the dev-profile
    // addon of its own graph from .cache/native-debug (never packaged) instead of the dev build.
    debug: process.env.NAPI_DEBUG_ADDON === '1',
  });
  const require = createRequire(import.meta.url);
  let lastError: unknown;

  for (const path of paths) {
    try {
      return assertNativeModule(require(path));
    } catch (error) {
      lastError = error;
    }
  }

  const remedy = producer === null ? '' : ` Run "nx run ${producer}" for this platform.`;
  throw new Error(`Could not load ${fileName} from ${paths.join(', ')}.${remedy}`, { cause: lastError });
}
