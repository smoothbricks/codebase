/// <reference types="bun" />
/// <reference types="node" />

import { describe, expect, it } from 'bun:test';
import { openSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { CowshedError, connectCoordinator, coordinatorEndpoint, type ErrorCode, openProject } from './index.js';

function requireCowshedError(error: unknown, code: ErrorCode): CowshedError {
  expect(error).toBeInstanceOf(CowshedError);
  if (!(error instanceof CowshedError)) {
    // invariant throw: the assertion above proves this branch unreachable.
    throw new Error('expected CowshedError');
  }
  expect(error.code).toBe(code);
  expect(error.hint.length).toBeGreaterThan(0);
  return error;
}

describe('Cowshed Node-API bindings', () => {
  it('rejects invalid inherited descriptors with the stable usage error', () => {
    try {
      coordinatorEndpoint(-1);
      throw new Error('expected coordinatorEndpoint to reject');
    } catch (error) {
      requireCowshedError(error, 'usage');
    }
  });

  it('consumes an inherited endpoint exactly once and preserves handshake errors', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cowshed-napi-'));
    try {
      const path = join(root, 'not-a-socket');
      await writeFile(path, 'fixture');
      const endpoint = coordinatorEndpoint(openSync(path, 'r'));

      try {
        await openProject(endpoint, root);
        throw new Error('expected a regular-file endpoint to fail the controller handshake');
      } catch (error) {
        const handshake = requireCowshedError(error, 'environment-missing');
        expect(handshake.message).toContain('not a stream socket');
      }

      try {
        await openProject(endpoint, root);
        throw new Error('expected a consumed endpoint to reject reuse');
      } catch (error) {
        const consumed = requireCowshedError(error, 'conflict');
        expect(consumed.message).toContain('already been consumed');
      }

      const coordinatorEndpointValue = coordinatorEndpoint(openSync(path, 'r'));
      try {
        await connectCoordinator(coordinatorEndpointValue, root);
        throw new Error('expected a regular-file endpoint to fail the coordinator handshake');
      } catch (error) {
        const handshake = requireCowshedError(error, 'environment-missing');
        expect(handshake.message).toContain('not a stream socket');
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('loads the built addon and preserves its error contract under Node', async () => {
    const moduleUrl = pathToFileURL(join(import.meta.dir, '..', 'dist', 'index.js')).href;
    const script = `
      import { coordinatorEndpoint, CowshedError } from ${JSON.stringify(moduleUrl)};
      try {
        coordinatorEndpoint(-1);
        process.exitCode = 2;
      } catch (error) {
        if (!(error instanceof CowshedError)) throw error;
        if (error.code !== 'usage' || error.hint.length === 0) process.exitCode = 3;
      }
    `;
    const node = Bun.spawn(['node', '--input-type=module', '--eval', script], {
      cwd: join(import.meta.dir, '..'),
      stdin: 'ignore',
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stderr] = await Promise.all([node.exited, new Response(node.stderr).text()]);

    expect(stderr).toBe('');
    expect(exitCode).toBe(0);
  });
});
