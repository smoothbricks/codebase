/**
 * ModuleContext - Shared context for all tasks in a module.
 *
 * @module moduleContext
 */

import { type BufferCapacityStats, DEFAULT_BUFFER_CAPACITY, intern } from '@smoothbricks/arrow-builder';
import type { TagAttributeSchema } from './schema/types.js';

/**
 * Module context shared across all tasks in the same module.
 */
export class ModuleContext {
  /** Self-tuning buffer capacity stats (initialized with defaults) */
  readonly spanBufferCapacityStats: BufferCapacityStats = {
    currentCapacity: DEFAULT_BUFFER_CAPACITY,
    totalWrites: 0,
    overflowWrites: 0,
    totalBuffersCreated: 0,
  };

  /** Pre-encoded UTF-8 bytes for filePath (avoids repeated encoding) */
  readonly utf8FilePath: Uint8Array;

  /** Pre-encoded UTF-8 bytes for gitSha (avoids repeated encoding) */
  readonly utf8GitSha: Uint8Array;

  constructor(
    public readonly moduleId: number, // Keep this for now, we'll remove ID later
    public readonly gitSha: string,
    public readonly filePath: string,
    public readonly tagAttributes: TagAttributeSchema,
  ) {
    this.utf8FilePath = intern(filePath);
    this.utf8GitSha = intern(gitSha);
  }
}
