/**
 * Shared types for ops() experiments
 */

// =============================================================================
// Mock types
// =============================================================================

export interface LogAPI {
  info(msg: string): void;
  error(msg: string): void;
}

export interface TagAPI {
  method(m: string): TagAPI;
  status(s: number): TagAPI;
  url(u: string): TagAPI;
}

export interface Module {
  name: string;
}

// =============================================================================
// Op class
// =============================================================================

export class Op<Args extends unknown[], Result> {
  constructor(
    readonly name: string,
    readonly fn: (ctx: OpContext, ...args: Args) => Promise<Result>,
    readonly module: Module,
  ) {}

  invocationCount = 0;
  errorCount = 0;
}

// =============================================================================
// Context type - what ops receive as first argument
// =============================================================================

export interface OpContext {
  span<R, A extends unknown[]>(name: string, op: Op<A, R>, ...args: A): Promise<R>;
  log: LogAPI;
  tag: TagAPI;
  deps: Record<string, Op<unknown[], unknown>>;
}

// =============================================================================
// Test fixtures
// =============================================================================

export const httpModule: Module = { name: 'http' };

export interface RequestOpts {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  body?: unknown;
}
