/**
 * ModuleContext - Shared context for all tasks in a module.
 *
 * @module moduleContext
 */

import {
  type BufferCapacityStats,
  DEFAULT_BUFFER_CAPACITY,
  intern,
  PreEncodedEntry,
} from '@smoothbricks/arrow-builder';
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

  /** Pre-encoded entry for package (for Arrow dictionary building) */
  readonly packageEntry: PreEncodedEntry;

  /** Pre-encoded entry for packagePath (for Arrow dictionary building) */
  readonly packagePathEntry: PreEncodedEntry;

  /** Pre-encoded entry for gitSha (for Arrow dictionary building) */
  readonly gitShaEntry: PreEncodedEntry;

  constructor(
    public readonly moduleId: number, // Keep this for now, we'll remove ID later
    public readonly gitSha: string,
    /** npm package name (e.g., '@smoothbricks/lmao') */
    public readonly packageName: string,
    /** Path within package, relative to package.json (e.g., 'src/services/user.ts') */
    public readonly packagePath: string,
    public readonly tagAttributes: TagAttributeSchema,
  ) {
    // Pre-encode and store as reusable entries for Arrow dictionary building
    // Using class instances for V8 optimization (consistent hidden class shape)
    this.packageEntry = new PreEncodedEntry(packageName, intern(packageName));
    this.packagePathEntry = new PreEncodedEntry(packagePath, intern(packagePath));
    this.gitShaEntry = new PreEncodedEntry(gitSha, intern(gitSha));
  }
}
