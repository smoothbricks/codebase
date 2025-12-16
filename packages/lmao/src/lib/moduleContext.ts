/**
 * ModuleContext - Shared context for all tasks in a module.
 *
 * @module moduleContext
 */

import { type BufferCapacityStats, DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
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

  constructor(
    public readonly moduleId: number,
    public readonly gitSha: string,
    public readonly filePath: string,
    public readonly tagAttributes: TagAttributeSchema,
  ) {}
}
