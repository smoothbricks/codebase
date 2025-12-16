/**
 * TaskContext - Combines module context with task-specific data.
 *
 * ## Thread ID Access
 *
 * The thread ID is a module-level singleton in threadId.ts.
 * ES modules are instantiated once per JavaScript realm:
 * - Node.js main process: one thread ID per process
 * - Node.js worker_threads: each Worker gets its own thread ID
 * - Browser main thread: one thread ID
 * - Web Workers: each Worker gets its own thread ID
 *
 * This gives us "thread-local" storage without explicit TLS APIs.
 *
 * @module taskContext
 */

import type { ModuleContext } from './moduleContext.js';
import { copyThreadIdTo } from './threadId.js';

/**
 * Task context passed to SpanBuffer constructors.
 */
export class TaskContext {
  constructor(
    public readonly module: ModuleContext,
    public readonly spanNameId: number,
    public readonly lineNumber: number,
  ) {}

  /**
   * Copy this process/worker's thread ID bytes (8 bytes) to destination.
   */
  copyThreadIdTo(dest: Uint8Array, offset: number): void {
    copyThreadIdTo(dest, offset);
  }
}
