import { describe, expect, it, jest } from 'bun:test';
import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { logger } from 'nx/src/devkit-exports.js';
import { standInReservations } from '../../__tests__/stand-in-mbx-reservations.js';
import {
  closeRunReservations,
  RESERVATIONS_ENV,
  RESERVATIONS_UNSERVED_ENV,
  reserveCores,
  serveRunReservations,
} from './mbx-reservations.js';

describe("a leaf's reservation on its run's mbx endpoint", () => {
  it('asks for many reservations over one connection, each granted its cores, and releases each once', async () => {
    const endpoint = await standInReservations((heard) => Math.min(heard.cpus, 4));
    try {
      const wide = reserveCores(endpoint.socket, 6, 'low');
      const narrow = reserveCores(endpoint.socket, 2, 'normal');
      expect(await wide.granted).toMatchObject({ ok: true, cpus: 4 });
      expect(await narrow.granted).toMatchObject({ ok: true, cpus: 2 });
      wide.release();
      narrow.release();
      wide.release();
      await endpoint.released(2);

      expect(endpoint.connections()).toBe(1);
      expect(endpoint.reserves.map(({ cpus, priority }) => ({ cpus, priority }))).toEqual([
        { cpus: 6, priority: 'low' },
        { cpus: 2, priority: 'normal' },
      ]);
      expect(endpoint.releases).toEqual(endpoint.reserves.map(({ id }) => id));
    } finally {
      await endpoint.close();
    }
  });

  it("answers a refusal as its request's error, and the connection serves on", async () => {
    const endpoint = await standInReservations((heard) =>
      heard.cpus === 3 ? { refused: '--memory needs scheduler.memory to be configured' } : 1,
    );
    try {
      const refused = reserveCores(endpoint.socket, 3, 'low');
      expect(await refused.granted).toEqual({ ok: false, error: '--memory needs scheduler.memory to be configured' });
      const next = reserveCores(endpoint.socket, 1, 'low');
      expect(await next.granted).toMatchObject({ ok: true, cpus: 1 });
      next.release();
      await endpoint.released(1);
      expect(endpoint.connections()).toBe(1);
    } finally {
      await endpoint.close();
    }
  });

  it('fails what waits and loses what is held when the endpoint goes, and finds none after', async () => {
    // A full pool keeps the second request waiting.
    const endpoint = await standInReservations((heard) => (heard.cpus === 1 ? 1 : null));
    const held = reserveCores(endpoint.socket, 1, 'low');
    const granted = await held.granted;
    if (!granted.ok) throw new Error(`the stand-in grants one core: ${granted.error}`);
    const waiting = reserveCores(endpoint.socket, 2, 'low');

    await endpoint.close();

    expect(await waiting.granted).toMatchObject({ ok: false });
    expect(await granted.lost).toContain(endpoint.socket);
    const after = await reserveCores(endpoint.socket, 1, 'low').granted;
    expect(after).toMatchObject({ ok: false });
    if (!after.ok) expect(after.error).toContain(endpoint.socket);
  });
});

describe("an Nx run's mbx reservation endpoint", () => {
  /** A stand-in `mbx` at its command line: it records its arguments, then runs `body`. */
  const standInMbx = async (body: string) => {
    const root = await mkdtemp(join(tmpdir(), 'mbx-reservations-hook-'));
    const said = join(root, 'said');
    await writeFile(join(root, 'mbx'), `#!/bin/sh\nprintf '%s\\n' "$*" > '${said}'\n${body}\n`, { mode: 0o755 });
    const env: NodeJS.ProcessEnv = {
      PATH: `${root}:${process.env.PATH ?? ''}`,
      MBX_SCHEDULER: '1',
      MBX_CACHE_DIR: join(root, 'cache'),
    };
    return { root, said, env };
  };

  it('is started once for a run with mbx’s pool, published to its tasks, and ended with the run', async () => {
    // It serves until its standard input closes, as `mbx reservations` does.
    const mbx = await standInMbx('echo listening\nexec cat >/dev/null');
    try {
      await serveRunReservations({ id: 'run-1' }, mbx.env);
      const socket = mbx.env[RESERVATIONS_ENV];
      expect(mbx.env).not.toHaveProperty(RESERVATIONS_UNSERVED_ENV);
      if (socket === undefined) throw new Error('the run published no endpoint');
      expect(await readFile(mbx.said, 'utf8')).toBe(`reservations ${socket}\n`);

      await closeRunReservations({ id: 'run-1' });
      // The endpoint ended, and its socket's directory went with it.
      await expect(access(dirname(socket))).rejects.toThrow();
    } finally {
      await rm(mbx.root, { recursive: true, force: true });
    }
  });

  it('is not started without mbx’s pool, or inside a reservation', async () => {
    const mbx = await standInMbx('exit 97');
    try {
      for (const env of [
        { PATH: mbx.env.PATH },
        { ...mbx.env, MBX_SCHED_DISABLE: '1' },
      ] satisfies NodeJS.ProcessEnv[]) {
        await serveRunReservations({ id: 'run-2' }, env);
        expect(env).not.toHaveProperty(RESERVATIONS_ENV);
      }
      await expect(access(mbx.said)).rejects.toThrow();
    } finally {
      await rm(mbx.root, { recursive: true, force: true });
    }
  });

  it('says why an endpoint did not start, and publishes none', async () => {
    const mbx = await standInMbx("echo 'mbx: reservations need scheduler.enabled' >&2\nexit 1");
    const warn = jest.spyOn(logger, 'warn').mockImplementation(() => {});
    try {
      await serveRunReservations({ id: 'run-3' }, mbx.env);
      expect(mbx.env).not.toHaveProperty(RESERVATIONS_ENV);
      expect(mbx.env[RESERVATIONS_UNSERVED_ENV]).toContain(
        'ended with status 1: mbx: reservations need scheduler.enabled',
      );
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('ended with status 1: mbx: reservations need scheduler.enabled'),
      );
      // Nothing was served, so the run's end has nothing to close.
      await closeRunReservations({ id: 'run-3' });
    } finally {
      warn.mockRestore();
      await rm(mbx.root, { recursive: true, force: true });
    }
  });
});
