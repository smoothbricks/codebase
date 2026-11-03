import { CPU_TOKENS_ENV, parseInheritedTokens } from './runner-sizing.js';

/**
 * What a CPU pool puts in a command's environment (cpu-pool.ts serves the pool; this module decides what the command
 * sees of it, so it is keyed: executors.json lists cpu-pool.js as output-neutral, which it may only be while it writes
 * no variable the command reads).
 */

/** The environment name a child reads its parent's pool endpoint from; its count is `CPU_TOKENS_ENV`. */
export const CPU_POOL_ENV = 'BOUNDED_EXEC_CPU_POOL';

/** An inherited pool descriptor; only a grant from its endpoint proves a live allocation. */
export interface InheritedCpuPool {
  readonly socket: string;
  /** The count the parent names beside its endpoint. */
  readonly tokens: number;
}

/**
 * The inherited descriptor, or null when malformed or absent. A missing or bare count, or an empty
 * or NUL-bearing endpoint, proves no allocation; the count is a positive whole number. A valid
 * descriptor still needs a live endpoint grant.
 */
export function readInheritedCpuPool(environment: Readonly<NodeJS.ProcessEnv>): InheritedCpuPool | null {
  const socket = environment[CPU_POOL_ENV];
  const tokens = parseInheritedTokens(environment[CPU_TOKENS_ENV]);
  if (socket === undefined || socket.length === 0 || socket.includes('\0') || tokens === null) {
    return null;
  }
  return { socket, tokens };
}

/**
 * The environment a child of a pool inherits to reach it: its endpoint, the count the pool holds, and its make
 * jobserver, which Cargo and make read from the fifo; `-j` with a jobserver names no count of its own.
 */
export function poolEnvironment(socketPath: string, tokens: number, fifoPath: string): Record<string, string> {
  const jobserverFlags = `-j --jobserver-auth=fifo:${fifoPath}`;
  return {
    [CPU_POOL_ENV]: socketPath,
    [CPU_TOKENS_ENV]: String(tokens),
    CARGO_MAKEFLAGS: jobserverFlags,
    MAKEFLAGS: jobserverFlags,
  };
}
