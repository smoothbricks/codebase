/// <reference types="bun" />

/**
 * nx's self-hosted remote cache: `GET /v1/cache/<hash>` returns a task archive, and `PUT /v1/cache/<hash>`
 * publishes one. The cache is per-user and bound to loopback, and it has no authentication: any local
 * caller may read and write it. A task hash names the task's inputs, so an artifact written by any shed on
 * this machine is as trustworthy as the local cache itself. The loopback socket is the trust boundary.
 *
 * Writes never replace. A key that already holds an artifact answers 409 and its bytes are left alone, so a
 * retried upload cannot change what a hash holds. An upload is staged and checked against its declared length
 * before it is published, so a reader never sees a partial artifact.
 *
 * Status codes follow what nx does with them: 200 means stored, and 404 means "not cached" without failing the
 * task. A 403 is the one refusal nx absorbs silently, which is why an artifact this server will not keep is
 * answered 403 rather than failing the build that produced it. Retention is filesystem SIEVE (`gc.ts`).
 */

import { type CacheServerConfig, HOST } from './config.js';
import { sweepDirectory } from './gc.js';
import { logLine, logValue } from './log.js';
import {
  type CacheStore,
  type CacheStoreOpenResult,
  type CompleteUpload,
  describe,
  isCacheKey,
  openCacheStore,
  publishEntry,
  readEntry,
  type StoreOutcome,
  stageEntry,
} from './store.js';

const CACHE_ROUTE = '/v1/cache/:hash';

/** nx sends and expects `application/octet-stream` for artifact bodies. */
const OCTET_STREAM: Readonly<Record<string, string>> = {
  'content-type': 'application/octet-stream',
};
/** Plain-text bodies: exactly `text/plain`, no charset. */
const PLAIN: Readonly<Record<string, string>> = {
  'content-type': 'text/plain',
};

const MISS_BODY = 'No cache entry for that key.\n';

/**
 * Bun answers a body larger than this with a 413 the handler never sees — measured: it compares
 * `content-length` before invoking the route, and it also trips mid-stream on a body that declared no
 * length. Since nx turns any status but 200/409/403 into a failed task, the *policy* limits cannot live
 * here. `handlePut` owns the bounds: a PUT with no declared length is refused 411, a declared length over
 * `maxBytes` is refused 403 before a byte of body is read, and aggregate in-flight staging is reserved
 * against a separate cap. The published archive cap is enforced by sweeps after publication. This
 * ceiling is only a framing tripwire; it does not by itself bound bytes on disk or in flight.
 */
const FRAMING_CEILING = Number.MAX_SAFE_INTEGER;

export interface RunningCacheServer {
  /** The origin to hand nx as `NX_SELF_HOSTED_REMOTE_CACHE_SERVER`. */
  readonly origin: string;
  readonly port: number;
  /** Staged uploads a previous process left behind, removed while opening the store. */
  readonly reclaimedStagingFiles: number;
  stop(): Promise<void>;
}

/** The single stdout readiness record emitted by the foreground executable. */
export interface CacheServerReadyRecord {
  readonly kind: 'ready';
  readonly origin: string;
  readonly port: number;
  readonly reclaimedStagingFiles: number;
}

export type CacheServerStartResult =
  | { readonly ok: true; readonly server: RunningCacheServer }
  | {
      readonly ok: false;
      readonly stage: 'store' | 'gc' | 'listen';
      readonly directory: string;
      readonly reason: string;
    }
  | {
      readonly ok: false;
      readonly stage: 'store-locked';
      readonly directory: string;
      readonly path: string;
      readonly reason: string;
    };

interface UploadAdmission {
  reservedBytes: number;
}

export async function startCacheServer(config: CacheServerConfig): Promise<CacheServerStartResult> {
  let opened: CacheStoreOpenResult;
  try {
    opened = await openCacheStore(config.directory);
  } catch (cause) {
    return { ok: false, stage: 'store', directory: config.directory, reason: describe(cause) };
  }
  if (!opened.ok) {
    return {
      ok: false,
      stage: 'store-locked',
      directory: config.directory,
      path: opened.path,
      reason: `EWOULDBLOCK: another process holds the store lock at ${opened.path}`,
    };
  }
  const store = opened.store;
  const initial = await sweepDirectory(store, config.capBytes);
  if (!initial.ok) {
    await store.close();
    return { ok: false, stage: 'gc', directory: config.directory, reason: initial.reason };
  }
  const admission: UploadAdmission = { reservedBytes: 0 };
  let publishing = Promise.resolve();
  function publish(upload: CompleteUpload): Promise<StoreOutcome> {
    const next = publishing.then(async () => {
      const outcome = await publishEntry(store, upload);
      if (outcome.kind !== 'stored') return outcome;
      const retained = await sweepDirectory(store, config.capBytes, upload.key);
      return retained.ok
        ? outcome
        : ({
            kind: 'failed',
            reason: retained.reason,
            stagingRetained: outcome.stagingRetained,
          } satisfies StoreOutcome);
    });
    publishing = next.then(() => {});
    return next;
  }
  try {
    const server = Bun.serve({
      hostname: HOST,
      port: config.port,
      maxRequestBodySize: FRAMING_CEILING,
      // A paused upload keeps its connection. Bun's default closes a request idle for 10 s: an Nx upload
      // stalled on a loaded host lost its bytes with a 500, and process.test.ts's drain case, whose
      // upload waits on purpose, hung on the staged file that close removed. The listener is
      // loopback-only, so a client that dies closes its socket in the kernel and frees its reservation.
      idleTimeout: 0,
      routes: {
        [CACHE_ROUTE]: {
          GET: (request) => handleGet(store, request),
          PUT: (request) => handlePut(store, config, request, admission, publish),
        },
      },
      fetch(request) {
        const { pathname } = new URL(request.url);
        logLine(`ev=unrouted method=${logValue(request.method)} path=${logValue(pathname)} status=404`);
        return new Response(
          'This server serves /v1/cache/<hash> only (GET, PUT). A path with a doubled slash means NX_SELF_HOSTED_REMOTE_CACHE_SERVER carries a trailing slash.\n',
          { status: 404, headers: PLAIN },
        );
      },
      error(failure) {
        logLine(`ev=server.error status=500 reason=${logValue(String(failure.message ?? failure))}`);
        return new Response('The cache server failed to handle that request; see its log.\n', {
          status: 500,
          headers: PLAIN,
        });
      },
    });

    logLine(
      `ev=listening origin=${server.url.origin} dir=${logValue(config.directory)} max_bytes=${config.maxBytes} cap_bytes=${config.capBytes} reclaimed_staging=${store.reclaimedStagingFiles}`,
    );

    return {
      ok: true,
      server: {
        origin: server.url.origin,
        port: server.port ?? config.port,
        reclaimedStagingFiles: store.reclaimedStagingFiles,
        async stop() {
          await server.stop(false);
          await publishing;
          await store.close();
        },
      },
    };
  } catch (cause) {
    await store.close();
    return { ok: false, stage: 'listen', directory: config.directory, reason: describe(cause) };
  }
}

async function handleGet(store: CacheStore, request: Bun.BunRequest<typeof CACHE_ROUTE>): Promise<Response> {
  const startedNs = Bun.nanoseconds();
  const key = request.params.hash;
  if (!isCacheKey(key)) {
    // A key outside the charset cannot name an entry, so a miss is the honest answer, and nx reads a
    // 404 as "not cached" without failing the task.
    record('get', key, 404, 0, startedNs, 'invalid-key');
    return new Response(MISS_BODY, { status: 404, headers: PLAIN });
  }

  const entry = await readEntry(store, key);
  switch (entry.kind) {
    case 'missing':
      record('get', key, 404, 0, startedNs, 'miss');
      return new Response(MISS_BODY, { status: 404, headers: PLAIN });
    case 'failed':
      record('get', key, 500, 0, startedNs, entry.reason);
      return new Response('The cache server could not read that artifact; see its log.\n', {
        status: 500,
        headers: PLAIN,
      });
    case 'found':
      record('get', key, 200, entry.bytes, startedNs, undefined);
      return new Response(entry.body, { status: 200, headers: OCTET_STREAM });
  }
}

async function handlePut(
  store: CacheStore,
  config: CacheServerConfig,
  request: Bun.BunRequest<typeof CACHE_ROUTE>,
  admission: UploadAdmission,
  publish: (upload: CompleteUpload) => Promise<StoreOutcome>,
): Promise<Response> {
  const startedNs = Bun.nanoseconds();
  const key = request.params.hash;
  if (!isCacheKey(key)) {
    // nx cannot produce such a key, so whatever did is a broken client or an attempt at the filesystem
    // underneath: 400 fails the caller loudly instead of pretending to have stored something.
    record('put', key, 400, 0, startedNs, 'invalid-key');
    return new Response('A cache key is 1-128 characters of [A-Za-z0-9_-].\n', {
      status: 400,
      headers: PLAIN,
    });
  }

  // A declared length is required, and every bound below is derived from it. That is what keeps the
  // size limit total: nothing is streamed whose length is unknown, so no body can outrun the limit,
  // and `stageEntry` can refuse a body that arrives short of what it promised. nx always
  // sends `content-length` (it hands reqwest a fully built buffer), so a request without one is not
  // nx and deserves the loud answer.
  const header = request.headers.get('content-length');
  if (header === null) {
    record('put', key, 411, 0, startedNs, 'no-declared-length');
    return new Response('A cache artifact must arrive with a content-length.\n', { status: 411, headers: PLAIN });
  }
  const declared = Number(header);
  if (!Number.isInteger(declared) || declared < 1) {
    // Zero included: an empty artifact is one nx can never extract, and never-replace would make it a
    // permanent failure for that task hash.
    record('put', key, 400, 0, startedNs, 'bad-declared-length');
    return new Response('content-length must be a positive integer.\n', {
      status: 400,
      headers: PLAIN,
    });
  }
  if (declared > config.maxBytes) {
    // 403 rather than 413 on purpose: an artifact too large to keep must not fail the build that
    // produced it, and 403 is the one refusal nx absorbs silently. The log is where it is not silent.
    record('put', key, 403, declared, startedNs, 'over-max-bytes');
    return new Response(`This artifact exceeds the server's ${config.maxBytes}-byte limit.\n`, {
      status: 403,
      headers: PLAIN,
    });
  }

  if (declared > config.capBytes - admission.reservedBytes) {
    record('put', key, 403, declared, startedNs, 'staging-capacity');
    return new Response('The staging byte budget is full; nothing was stored.\n', {
      status: 403,
      headers: PLAIN,
    });
  }
  admission.reservedBytes += declared;
  logLine(
    `ev=staging.reserved key=${logValue(key)} declared_bytes=${declared} reserved_bytes=${admission.reservedBytes} cap=${config.capBytes}`,
  );
  let stagingRetained = false;
  try {
    const staged = await stageEntry(store, key, request.body, declared);
    if (staged.kind !== 'ready') {
      stagingRetained = staged.stagingRetained;
      if (staged.kind === 'incomplete') {
        record('put', key, 400, staged.bytes, startedNs, 'body-shorter-than-declared');
        return new Response(
          `That body ended after ${staged.bytes} of the ${declared} bytes it declared; nothing was stored.\n`,
          { status: 400, headers: PLAIN },
        );
      }
      record('put', key, 500, 0, startedNs, staged.reason);
      return new Response('The cache server could not stage that artifact; see its log.\n', {
        status: 500,
        headers: PLAIN,
      });
    }
    const outcome = await publish(staged);
    stagingRetained = outcome.stagingRetained;
    if (outcome.kind === 'stored') {
      record('put', key, 200, outcome.bytes, startedNs, undefined);
      return new Response('stored\n', { status: 200, headers: PLAIN });
    }
    if (outcome.kind === 'exists') {
      record('put', key, 409, 0, startedNs, 'already-stored');
      return new Response('That key already holds an artifact; a cache entry is never replaced.\n', {
        status: 409,
        headers: PLAIN,
      });
    }
    record('put', key, 500, 0, startedNs, outcome.reason);
    return new Response('The cache server could not publish or retain that artifact; see its log.\n', {
      status: 500,
      headers: PLAIN,
    });
  } finally {
    if (stagingRetained) {
      logLine(
        `ev=staging.reservation-retained key=${logValue(key)} declared_bytes=${declared} reserved_bytes=${admission.reservedBytes} reason=cleanup-failed`,
      );
    } else {
      admission.reservedBytes -= declared;
      logLine(
        `ev=staging.released key=${logValue(key)} declared_bytes=${declared} reserved_bytes=${admission.reservedBytes}`,
      );
    }
  }
}

function record(
  event: 'get' | 'put',
  key: string,
  status: number,
  bytes: number,
  startedNs: number,
  reason: string | undefined,
): void {
  const microseconds = Math.round((Bun.nanoseconds() - startedNs) / 1000);
  const because = reason === undefined ? '' : ` reason=${logValue(reason)}`;
  logLine(`ev=${event} key=${logValue(key)} status=${status} bytes=${bytes} us=${microseconds}${because}`);
}
