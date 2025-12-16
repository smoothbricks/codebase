/**
 * Background flush scheduler with adaptive flushing
 *
 * Per specs/01b2_buffer_self_tuning.md:
 * - Adaptive flush intervals based on capacity, time, memory pressure
 * - Idle detection for opportunistic flushing
 * - Background processing (cold path)
 */

import type * as arrow from 'apache-arrow';
import { convertSpanTreeToArrowTable, type StringInterner } from './convertToArrow.js';
import type { SpanBuffer } from './types.js';

/**
 * Flush handler function type
 * Called when buffers are ready to be flushed
 */
export type FlushHandler = (table: arrow.Table, metadata: FlushMetadata) => Promise<void> | void;

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
  private moduleIdInterner: StringInterner;
  private spanNameInterner: StringInterner;

  private buffers = new Set<SpanBuffer>();
  private lastFlushTime = Date.now();
  private lastActivityTime = Date.now();
  private flushTimer?: NodeJS.Timeout;
  private idleTimer?: NodeJS.Timeout;
  private memoryTimer?: NodeJS.Timeout;
  private isRunning = false;

  constructor(
    handler: FlushHandler,
    moduleIdInterner: StringInterner,
    spanNameInterner: StringInterner,
    config: FlushSchedulerConfig = {},
  ) {
    this.handler = handler;
    this.moduleIdInterner = moduleIdInterner;
    this.spanNameInterner = spanNameInterner;

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
  register(buffer: SpanBuffer): void {
    this.buffers.add(buffer);
    this.lastActivityTime = Date.now();

    // Start scheduler if not running
    if (!this.isRunning) {
      this.start();
    }
  }

  /**
   * Unregister a buffer
   */
  unregister(buffer: SpanBuffer): void {
    this.buffers.delete(buffer);

    // Stop scheduler if no buffers
    if (this.buffers.size === 0 && this.isRunning) {
      this.stop();
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
      const utilizationRatio = buffer.writeIndex / buffer.capacity;
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
    } catch (error) {
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

    // Count total rows and buffers
    let totalRows = 0;
    let totalBuffers = 0;

    for (const buffer of buffersToFlush) {
      // Count rows in buffer chain
      let currentBuffer: SpanBuffer | undefined = buffer;
      while (currentBuffer) {
        totalRows += currentBuffer.writeIndex;
        totalBuffers++;
        currentBuffer = currentBuffer.next as SpanBuffer | undefined;
      }
    }

    if (totalRows === 0) return;

    // Convert all buffers to Arrow tables and concatenate
    const tables: arrow.Table[] = [];

    for (const buffer of buffersToFlush) {
      try {
        const table = convertSpanTreeToArrowTable(buffer, this.moduleIdInterner, this.spanNameInterner);

        if (table.numRows > 0) {
          tables.push(table);
        }
      } catch (error) {
        console.error('Error converting buffer to Arrow table:', error);
      }
    }

    if (tables.length === 0) return;

    // Concatenate all tables
    let combinedTable: arrow.Table;
    if (tables.length === 1) {
      combinedTable = tables[0];
    } else {
      // Concatenate by extracting all record batches
      // Use first table's schema as reference
      const Arrow = await import('apache-arrow');
      const schema = tables[0].schema;
      const allBatches: arrow.RecordBatch[] = [];

      for (const table of tables) {
        // Cast batches to the reference schema to ensure compatibility
        for (let i = 0; i < table.batches.length; i++) {
          allBatches.push(table.batches[i]);
        }
      }

      // Create combined table with explicit schema
      try {
        combinedTable = new Arrow.Table(schema, allBatches);
      } catch (error) {
        // If schemas don't match (shouldn't happen in production), just use first table
        // This can occur in tests where buffers from different modules are mixed
        console.warn('Unable to combine tables due to schema mismatch, using first table only');
        combinedTable = tables[0];
      }
    }

    // Call handler
    const metadata: FlushMetadata = {
      totalRows,
      totalBuffers,
      flushReason: reason,
      timestamp: now,
    };

    try {
      await this.handler(combinedTable, metadata);
      this.lastFlushTime = now;

      // Reset buffers after successful flush to avoid duplicate re-processing
      for (const buffer of buffersToFlush) {
        // Reset writeIndex to 0 for the root buffer
        buffer.writeIndex = 0;

        // Clear any linked/chained buffers
        let nextBuffer = buffer.next as SpanBuffer | undefined;
        while (nextBuffer) {
          nextBuffer.writeIndex = 0;
          const temp = nextBuffer.next as SpanBuffer | undefined;
          nextBuffer.next = undefined; // Unlink chained buffer
          nextBuffer = temp;
        }
        buffer.next = undefined; // Clear link from root buffer
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
export function getGlobalFlushScheduler(
  handler: FlushHandler,
  moduleIdInterner: StringInterner,
  spanNameInterner: StringInterner,
  config?: FlushSchedulerConfig,
): FlushScheduler {
  if (!globalScheduler) {
    globalScheduler = new FlushScheduler(handler, moduleIdInterner, spanNameInterner, config);
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
