#!/usr/bin/env bun
/// <reference types="bun" />

/**
 * The daemon entry point: read the environment, refuse to start on anything it cannot describe, serve
 * until a signal arrives.
 *
 * stderr carries event logs and guidance; stdout carries one JSON startup record for the supervisor.
 * Exit 2 is configuration refusal; exit 1 is operational startup failure.
 *
 *   NX_CACHE_SERVER_DIR=/srv/nx-cache \
 *   NX_CACHE_SERVER_WRITE_TOKENS=ci:$SECRET \
 *   NX_CACHE_SERVER_READ_TOKENS=shed:$OTHER \
 *   bun src/main.ts
 */

import { cacheServerConfigFromEnv } from './config.js';
import { logLine, logValue } from './log.js';
import { type CacheServerReadyRecord, startCacheServer } from './server.js';
import { describe } from './store.js';

let exitOnStdinEof = false;
const argumentProblems: string[] = [];
for (let position = 2; position < Bun.argv.length; position++) {
  if (Bun.argv[position] === '--exit-on-stdin-eof') {
    exitOnStdinEof = true;
  } else {
    argumentProblems.push(
      `argument ${position - 1} is unsupported; the only supported argument is --exit-on-stdin-eof`,
    );
  }
}
if (argumentProblems.length > 0) {
  console.log(JSON.stringify({ kind: 'failed', stage: 'config', problems: argumentProblems }));
  console.error('nx-cache-server refuses to start:');
  for (const problem of argumentProblems) {
    console.error(`  - ${problem}`);
  }
  process.exit(2);
}

const configured = cacheServerConfigFromEnv(Bun.env);
if (!configured.ok) {
  console.log(JSON.stringify({ kind: 'failed', stage: 'config', problems: configured.problems }));
  console.error('nx-cache-server refuses to start:');
  for (const problem of configured.problems) {
    console.error(`  - ${problem}`);
  }
  process.exit(2);
}

const started = await startCacheServer(configured.config);
if (!started.ok) {
  console.log(JSON.stringify({ kind: 'failed', ...started }));
  console.error(`nx-cache-server ${started.stage} failed in ${started.directory}: ${started.reason}`);
  process.exit(1);
}
const running = started.server;
console.log(
  JSON.stringify({
    kind: 'ready',
    origin: running.origin,
    port: running.port,
    reclaimedStagingFiles: running.reclaimedStagingFiles,
  } satisfies CacheServerReadyRecord),
);

/**
 * Standard mode stops on SIGINT or SIGTERM. With `--exit-on-stdin-eof`, the parent that holds stdin
 * stops the daemon by closing it; signals still stop it in both modes. A stop lets in-flight requests
 * finish. An upload killed mid-body leaves only a staged file, which the next start reclaims — the
 * published entry is never partial, because it only ever comes into existence by `link(2)`.
 */
let stopping = false;
function stop(fields: string, exitCode = 0): void {
  if (stopping) return;
  stopping = true;
  logLine(`ev=stopping ${fields}`);
  void running.stop().then(
    () => process.exit(exitCode),
    (failure: unknown) => {
      logLine(`ev=stop.failed reason=${logValue(describe(failure))}`);
      process.exit(1);
    },
  );
}
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => stop(`signal=${signal}`));
}
if (exitOnStdinEof) {
  process.stdin.once('end', () => stop('reason=supervisor-eof'));
  process.stdin.once('error', (failure) => {
    logLine(`ev=supervisor.error reason=${logValue(describe(failure))}`);
    stop('reason=supervisor-error', 1);
  });
  process.stdin.resume();
}
