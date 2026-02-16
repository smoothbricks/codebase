/**
 * Background flush scheduler with adaptive flushing
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Adaptive flush intervals based on capacity, time, memory pressure
 * - Idle detection for opportunistic flushing
 * - Background processing (cold path)
 */

import { Column, type Table, tableFromColumns } from '@uwdata/flechette';
import type { CapacityStatsEntry } from './arrow/capacityStats.js';
import { convertSpanTreeToArrowTable } from './convertToArrow.js';
import type { SpanBufferConstructor } from './spanBuffer.js';
import type { AnySpanBuffer, OpMetadata } from './types.js';

/**
 * Number of flushes before logging capacity stats.
 * Capacity stats are logged periodically to avoid overhead on every flush.
 */
const CAPACITY_STATS_FLUSH_INTERVAL = 100;

function mergeTables(first: Table, rest: Table[]): Table {
  if (rest.length === 0) return first;

  const names = first.names as string[];
  const merged: [string, Column<unknown>][] = names.map((name, index) => {
    const batches = [...(first.getChildAt(index)?.data ?? [])];
    for (const table of rest) {
      const col = table.getChild(name);
      if (!col) continue;
      batches.push(...col.data);
    }
    return [name, new Column(batches as never[])];
  });

  return tableFromColumns(merged);
}

/**
 * Flush handler function type
 * Called when buffers are ready to be flushed
 */
export type FlushHandler = (table: Table, metadata: FlushMetadata) => Promise<void> | void;

/**
 * Metadata about the flush operation
 */
export interface FlushMetadata {
  totalRows: number;
  totalBuffers: number;
  flushReason: 'capacity' | 'time' | 'memory' | 'idle' | 'manual';
  timestamp: number;
}

/**
 * Flush scheduler configuration
 */
export interface FlushSchedulerConfig {
  /**
   * Maximum time between flushes (milliseconds)
   * Default: 10000 (10 seconds)
   */
  maxFlushInterval?: number;

  /**
   * Minimum time between flushes (milliseconds)
   * Default: 1000 (1 second)
   */
  minFlushInterval?: number;

  /**
   * Capacity threshold for automatic flush (0.0 - 1.0)
   * Flush when buffer is X% full
   * Default: 0.8 (80%)
   */
  capacityThreshold?: number;

  /**
   * Enable idle detection for opportunistic flushing
   * Default: true
   */
  idleDetection?: boolean;

  /**
   * Idle timeout (milliseconds) - flush if no activity for this duration
   * Default: 5000 (5 seconds)
   */
  idleTimeout?: number;

  /**
   * Enable memory pressure detection
   * Default: true (Node.js only)
   */
  memoryPressureDetection?: boolean;

  /**
   * Memory pressure threshold (bytes)
   * Flush when available memory drops below this
   * Default: 100MB
   */
  memoryPressureThreshold?: number;
}

/**
 * Background flush scheduler
 * Manages automatic flushing of span buffers to Arrow tables
 */
export class FlushScheduler {
  private config: Required<FlushSchedulerConfig>;
  private handler: FlushHandler;

  private buffers = new Set<AnySpanBuffer>();
  private lastFlushTime = Date.now();
  private lastActivityTime = Date.now();
  private flushTimer?: NodeJS.Timeout;
  private idleTimer?: NodeJS.Timeout;
  private memoryTimer?: NodeJS.Timeout;
  private isRunning = false;

  /** Flush counter for periodic capacity stats logging */
  private _flushCount = 0;

  /**
   * Track unique (SpanBufferConstructor, OpMetadata) pairs that have been flushed since last capacity stats log.
   * We use SpanBufferConstructor as the key (for dedup) and store the OpMetadata for building PreEncodedEntry at conversion time.
   * The constructor has static stats property with the capacity stats we need.
   */
  private _bufferClassesSinceLastStatsLog = new Map<SpanBufferConstructor, OpMetadata>();

  constructor(handler: FlushHandler, config: FlushSchedulerConfig = {}) {
    this.handler = handler;

    // Apply defaults
    this.config = {
      maxFlushInterval: config.maxFlushInterval ?? 10000,
      minFlushInterval: config.minFlushInterval ?? 1000,
      capacityThreshold: config.capacityThreshold ?? 0.8,
      idleDetection: config.idleDetection ?? true,
      idleTimeout: config.idleTimeout ?? 5000,
      memoryPressureDetection: config.memoryPressureDetection ?? true,
      memoryPressureThreshold: config.memoryPressureThreshold ?? 100 * 1024 * 1024, // 100MB
    };
  }

  /**
   * Register a buffer for automatic flushing
   */
  register(buffer: AnySpanBuffer): void {
    this.buffers.add(buffer);
    this.lastActivityTime = Date.now();

    // Start scheduler if not running
    if (!this.isRunning) {
      this.start();
    }
  }

  /**
   * Start the flush scheduler
   */
  start(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    this.lastFlushTime = Date.now();
    this.lastActivityTime = Date.now();

    // Set up periodic flush timer
    this.flushTimer = setInterval(() => {
      this.checkFlushConditions();
    }, 1000); // Check every second

    // Set up idle detection timer
    if (this.config.idleDetection) {
      this.idleTimer = setInterval(() => {
        this.checkIdleFlush();
      }, this.config.idleTimeout);
    }

    // Set up memory pressure timer (Node.js only)
    if (this.config.memoryPressureDetection && typeof process !== 'undefined') {
      this.memoryTimer = setInterval(() => {
        this.checkMemoryPressure();
      }, 2000); // Check every 2 seconds
    }
  }

  /**
   * Stop the flush scheduler
   */
  stop(): void {
    if (!this.isRunning) return;

    this.isRunning = false;

    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = undefined;
    }

    if (this.idleTimer) {
      clearInterval(this.idleTimer);
      this.idleTimer = undefined;
    }

    if (this.memoryTimer) {
      clearInterval(this.memoryTimer);
      this.memoryTimer = undefined;
    }
  }

  /**
   * Manually trigger a flush
   */
  async flush(): Promise<void> {
    await this.doFlush('manual');
  }

  /**
   * Check flush conditions and flush if needed
   */
  private checkFlushConditions(): void {
    const now = Date.now();
    const timeSinceLastFlush = now - this.lastFlushTime;

    // Check max flush interval
    if (timeSinceLastFlush >= this.config.maxFlushInterval) {
      this.doFlush('time').catch(console.error);
      return;
    }

    // Check capacity threshold
    if (this.shouldFlushByCapacity()) {
      this.doFlush('capacity').catch(console.error);
      return;
    }
  }

  /**
   * Check if should flush based on capacity
   */
  private shouldFlushByCapacity(): boolean {
    for (const buffer of this.buffers) {
      const utilizationRatio = buffer._writeIndex / buffer._capacity;
      if (utilizationRatio >= this.config.capacityThreshold) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check idle state and flush if idle
   */
  private checkIdleFlush(): void {
    const now = Date.now();
    const timeSinceActivity = now - this.lastActivityTime;

    if (timeSinceActivity >= this.config.idleTimeout) {
      // Only flush if there's data and min interval has passed
      const timeSinceLastFlush = now - this.lastFlushTime;
      if (this.buffers.size > 0 && timeSinceLastFlush >= this.config.minFlushInterval) {
        this.doFlush('idle').catch(console.error);
      }
    }
  }

  /**
   * Check memory pressure and flush if needed
   * Node.js only
   */
  private checkMemoryPressure(): void {
    if (typeof process === 'undefined') return;

    try {
      const memUsage = process.memoryUsage();
      const availableMemory = memUsage.heapTotal - memUsage.heapUsed;

      if (availableMemory < this.config.memoryPressureThreshold) {
        this.doFlush('memory').catch(console.error);
      }
    } catch (_error) {
      // Ignore errors in memory detection
    }
  }

  /**
   * Perform the actual flush operation
   */
  private async doFlush(reason: FlushMetadata['flushReason']): Promise<void> {
    if (this.buffers.size === 0) return;

    const now = Date.now();
    const timeSinceLastFlush = now - this.lastFlushTime;

    // Respect minimum flush interval
    if (reason !== 'manual' && timeSinceLastFlush < this.config.minFlushInterval) {
      return;
    }

    // Collect all buffers to flush
    const buffersToFlush = Array.from(this.buffers);

    // Collect unique (SpanBufferConstructor, OpMetadata) pairs from buffers being flushed
    // Use SpanBufferConstructor as the key to dedup - multiple buffers may share the same class
    // We pick the first OpMetadata we see for each class (they should be consistent)
    const bufferClassesInThisFlush = new Map<SpanBufferConstructor, OpMetadata>();
    for (const buffer of buffersToFlush) {
      let currentBuffer: AnySpanBuffer | undefined = buffer;
      while (currentBuffer) {
        const bufferClass = currentBuffer.constructor as SpanBufferConstructor;
        // Only add if we haven't seen this buffer class before
        if (!bufferClassesInThisFlush.has(bufferClass)) {
          bufferClassesInThisFlush.set(bufferClass, currentBuffer._opMetadata);
        }
        currentBuffer = currentBuffer._overflow;
      }
    }

    // Add to tracking map (for periodic capacity stats logging)
    for (const [bufferClass, metadata] of bufferClassesInThisFlush) {
      // Only set if not already present (preserve first metadata seen)
      if (!this._bufferClassesSinceLastStatsLog.has(bufferClass)) {
        this._bufferClassesSinceLastStatsLog.set(bufferClass, metadata);
      }
    }

    // Increment flush counter
    this._flushCount++;

    // Check if we should log capacity stats
    const shouldLogCapacityStats = this._flushCount >= CAPACITY_STATS_FLUSH_INTERVAL;
    // Convert Map to CapacityStatsEntry[] for the conversion function
    const modulesToLogStatsForConversion: CapacityStatsEntry[] | undefined = shouldLogCapacityStats
      ? Array.from(this._bufferClassesSinceLastStatsLog.entries()).map(([bufferClass, metadata]) => ({
          bufferClass,
          metadata,
        }))
      : undefined;

    // Count total rows and buffers
    let totalRows = 0;
    let totalBuffers = 0;

    for (const buffer of buffersToFlush) {
      // Count rows in buffer chain
      let currentBuffer: AnySpanBuffer | undefined = buffer;
      while (currentBuffer) {
        totalRows += currentBuffer._writeIndex;
        totalBuffers++;
        currentBuffer = currentBuffer._overflow;
      }
    }

    if (totalRows === 0 && !shouldLogCapacityStats) return;

    // Convert all buffers to Arrow tables and concatenate
    const tables = [];

    for (const buffer of buffersToFlush) {
      try {
        const table = convertSpanTreeToArrowTable(buffer, undefined, modulesToLogStatsForConversion);

        if (table.numRows > 0) {
          tables.push(table);
        }
      } catch (error) {
        console.error('Error converting buffer to Arrow table:', error);
      }
    }

    // Concatenate all tables
    if (tables.length === 0) {
      // No tables but might have capacity stats - create empty table
      // Capacity stats will be in the first table if any buffer had data
      // If no buffers had data, we still need to handle capacity stats
      // For now, return early - capacity stats will be logged on next flush with data
      if (!shouldLogCapacityStats) return;
      // If we should log stats but have no tables, we need to create a table with just capacity stats
      // This is handled by convertSpanTreeToArrowTable returning a table with capacity stats batch
      // But since we have no buffers, we can't call it. So we skip logging stats this time.
      // Stats will be logged when there's actual data to flush.
      return;
    }
    const [firstTable, ...otherTables] = tables;
    const combinedTable = mergeTables(firstTable, otherTables);

    // Call handler
    const metadata: FlushMetadata = {
      totalRows,
      totalBuffers,
      flushReason: reason,
      timestamp: now,
    };

    try {
      await this.handler(combinedTable as unknown as Table, metadata);
      this.lastFlushTime = now;

      // Reset flush counter and module tracking if we logged capacity stats
      if (shouldLogCapacityStats) {
        this._flushCount = 0;
        this._bufferClassesSinceLastStatsLog.clear();
      }

      // Reset buffers after successful flush to avoid duplicate re-processing
      for (const buffer of buffersToFlush) {
        // Reset writeIndex to 0 for the root buffer
        buffer._writeIndex = 0;

        // Clear any linked/chained buffers
        let nextBuffer = buffer._overflow;
        while (nextBuffer) {
          nextBuffer._writeIndex = 0;
          const temp = nextBuffer._overflow;
          nextBuffer._overflow = undefined; // Unlink chained buffer
          nextBuffer = temp;
        }
        buffer._overflow = undefined; // Clear link from root buffer

        // A flushed root buffer is no longer pending; callers must re-register on future writes.
        this.buffers.delete(buffer);
      }
    } catch (error) {
      console.error('Error in flush handler:', error);
      // Do NOT reset buffers on failure to avoid data loss
    }
  }

  /**
   * Record activity (called when buffers are written to)
   */
  recordActivity(): void {
    this.lastActivityTime = Date.now();
  }
}

/**
 * Global flush scheduler instance (optional)
 * Applications can use this or create their own
 */
let globalScheduler: FlushScheduler | null = null;

/**
 * Get or create global flush scheduler
 */
export function getGlobalFlushScheduler(handler: FlushHandler, config?: FlushSchedulerConfig): FlushScheduler {
  if (!globalScheduler) {
    globalScheduler = new FlushScheduler(handler, config);
  }
  return globalScheduler;
}

/**
 * Reset global flush scheduler (for testing)
 */
export function resetGlobalFlushScheduler(): void {
  if (globalScheduler) {
    globalScheduler.stop();
    globalScheduler = null;
  }
}

/**
 * Test utilities for accessing FlushScheduler internals
 * @internal
 */
export const FlushSchedulerTestUtils = {
  /**
   * Get the flush timer for a scheduler instance (for testing)
   */
  getFlushTimer(scheduler: FlushScheduler): NodeJS.Timeout | undefined {
    return (scheduler as unknown as { flushTimer?: NodeJS.Timeout }).flushTimer;
  },

  /**
   * Set the flush timer for a scheduler instance (for testing)
   */
  setFlushTimer(scheduler: FlushScheduler, timer: NodeJS.Timeout | undefined): void {
    (scheduler as unknown as { flushTimer?: NodeJS.Timeout }).flushTimer = timer;
  },

  /**
   * Get the last activity time for a scheduler instance (for testing)
   */
  getLastActivityTime(scheduler: FlushScheduler): number | undefined {
    return (scheduler as unknown as { lastActivityTime?: number }).lastActivityTime;
  },

  /**
   * Set the last activity time for a scheduler instance (for testing)
   */
  setLastActivityTime(scheduler: FlushScheduler, time: number | null | undefined): void {
    (scheduler as unknown as { lastActivityTime?: number | null }).lastActivityTime = time;
  },
};
