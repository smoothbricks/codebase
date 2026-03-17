/**
 * Arrow query engine interface for client-side SQL over Arrow RecordBatches.
 * Backed by DuckDB-WASM for full SQL support with lazy initialization.
 *
 * QueryResult is typed as `unknown` because the concrete return type is
 * `arrow.Table` from apache-arrow (a transitive dep of @duckdb/duckdb-wasm)
 * which is not a direct dependency of this package.
 */
export interface ArrowQueryEngine {
  /** Register an Arrow IPC buffer as a named table in the engine */
  registerArrowBatch(name: string, ipcBytes: Uint8Array): Promise<void>;
  /** Execute a SQL query and return the result */
  query(sql: string): Promise<unknown>;
  /** Cleanup engine resources */
  close(): Promise<void>;
}

// Lazy singleton — engine created on first use, not at import time
let _enginePromise: Promise<ArrowQueryEngine> | null = null;

/**
 * Create or return the singleton DuckDB-WASM query engine.
 * Lazy initialization: the WASM bundle is only fetched on first call,
 * keeping import-time bundle cost zero.
 */
export function createQueryEngine(): Promise<ArrowQueryEngine> {
  if (_enginePromise) return _enginePromise;
  _enginePromise = initEngine();
  return _enginePromise;
}

async function initEngine(): Promise<ArrowQueryEngine> {
  // Dynamic import to avoid bundling DuckDB-WASM at module parse time
  const duckdbModule = await import('@duckdb/duckdb-wasm');

  // CDN-hosted WASM bundles avoid Metro/Vite bundling issues
  const bundles = duckdbModule.getJsDelivrBundles();
  const bundle = await duckdbModule.selectBundle(bundles);

  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdbModule.ConsoleLogger();
  const db = new duckdbModule.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  const conn = await db.connect();

  // Track registered file names for cleanup
  const registeredFiles = new Set<string>();

  return {
    async registerArrowBatch(name: string, ipcBytes: Uint8Array): Promise<void> {
      const fileName = `${name}.arrow`;
      await db.registerFileBuffer(fileName, ipcBytes);
      registeredFiles.add(fileName);
      await conn.query(`CREATE OR REPLACE TABLE "${name}" AS SELECT * FROM arrow_scan('${fileName}')`);
    },

    async query(sql: string): Promise<unknown> {
      return conn.query(sql);
    },

    async close(): Promise<void> {
      await conn.close();
      await db.terminate();
      worker.terminate();
      _enginePromise = null;
    },
  };
}

/**
 * Reset the singleton engine reference. Useful for testing.
 * Does NOT close the existing engine — call close() on the engine first.
 */
export function _resetEngineForTesting(): void {
  _enginePromise = null;
}
