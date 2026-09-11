import { describe, expect, it } from 'bun:test';
import {
  type NpmCommandResult,
  type NpmStatusShell,
  npmPackageStatus,
  npmPublishedVersionExists,
  npmPublishedVersionStatus,
} from '../private-npm.js';

const PACKAGE = '@smoothbricks/columine';
const VERSION = '0.1.17';

/**
 * The reset that cost a release: the durable-state probe asked npm whether
 * 0.1.17 was already published, the connection died before npm got an answer,
 * and `--fetch-retries=0` turned one dropped packet into a failed publish run
 * that had spent its whole gate.
 */
const ECONNRESET_STDERR = [
  'npm error code ECONNRESET',
  `npm error network request to https://registry.npmjs.org/${PACKAGE.replace('/', '%2f')} failed, reason: read ECONNRESET`,
].join('\n');

const NOT_FOUND_STDERR = [
  'npm error code E404',
  `npm error 404 Not Found - GET https://registry.npmjs.org/${PACKAGE} - Not found`,
].join('\n');

describe('npm durable-state probe', () => {
  it('reports a published version as exists on the first attempt', async () => {
    const npm = fakeNpm([{ exitCode: 0, stdout: `"${VERSION}"\n` }]);

    await expect(npmPublishedVersionStatus('/root', PACKAGE, VERSION, {}, npm.shell)).resolves.toEqual({
      kind: 'exists',
    });
    // The answering registry pays nothing for the retry bound.
    expect(npm.runs).toHaveLength(1);
    expect(npm.sleeps).toEqual([]);
  });

  it('reports a not-found version as absent without retrying it', async () => {
    const npm = fakeNpm([{ exitCode: 1, stderr: NOT_FOUND_STDERR }]);

    await expect(npmPublishedVersionStatus('/root', PACKAGE, VERSION, {}, npm.shell)).resolves.toEqual({
      kind: 'absent',
    });
    // A 404 is the common answer on the publish path: it stays one request.
    expect(npm.runs).toHaveLength(1);
    expect(npm.sleeps).toEqual([]);
  });

  it('retries a connection reset and reports undetermined when every attempt dies', async () => {
    const npm = fakeNpm([{ exitCode: 1, stderr: ECONNRESET_STDERR }]);

    const probe = await npmPublishedVersionStatus('/root', PACKAGE, VERSION, {}, npm.shell);

    expect(probe.kind).toBe('undetermined');
    expect(npm.runs.length).toBeGreaterThan(1);
    expect(probe).toMatchObject({ attempts: npm.runs.length });
    // One wait per retry, and the waits are real: an immediate re-ask races the
    // same broken connection.
    expect(npm.sleeps).toHaveLength(npm.runs.length - 1);
    expect(npm.sleeps.every((ms) => ms > 0)).toBe(true);
    expect(probe).toMatchObject({ detail: expect.stringContaining('ECONNRESET') });
  });

  it('reports exists when a reset is followed by an answer', async () => {
    const npm = fakeNpm([
      { exitCode: 1, stderr: ECONNRESET_STDERR },
      { exitCode: 0, stdout: `"${VERSION}"\n` },
    ]);

    await expect(npmPublishedVersionStatus('/root', PACKAGE, VERSION, {}, npm.shell)).resolves.toEqual({
      kind: 'exists',
    });
    expect(npm.runs).toHaveLength(2);
  });

  it('refuses immediately when the registry rejects the credential', async () => {
    const npm = fakeNpm([{ exitCode: 1, stderr: 'npm error code E401\nnpm error 401 Unauthorized - GET ...' }]);

    await expect(npmPublishedVersionStatus('/root', PACKAGE, VERSION, {}, npm.shell)).rejects.toThrow('401');
    // The registry gave a verdict; retrying only delays the same one.
    expect(npm.runs).toHaveLength(1);
  });

  it('refuses the release on an undetermined probe, naming the package, version and the network failure', async () => {
    const npm = fakeNpm([{ exitCode: 1, stderr: ECONNRESET_STDERR }]);

    const refusal = await npmPublishedVersionExists('/root', PACKAGE, VERSION, {}, npm.shell).then(
      (value) => `resolved to ${value}`,
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    );

    expect(refusal).toContain(`${PACKAGE}@${VERSION}`);
    expect(refusal).toContain('ECONNRESET');
    expect(refusal).toContain('network failure');
    expect(refusal).toContain('re-dispatching this run is safe');
  });

  it('applies the same three outcomes to the package-level probe', async () => {
    const absent = fakeNpm([{ exitCode: 1, stderr: NOT_FOUND_STDERR }]);
    const reset = fakeNpm([{ exitCode: 1, stderr: ECONNRESET_STDERR }]);

    await expect(npmPackageStatus('/root', PACKAGE, {}, absent.shell)).resolves.toEqual({ kind: 'absent' });
    expect(absent.runs).toHaveLength(1);

    expect((await npmPackageStatus('/root', PACKAGE, {}, reset.shell)).kind).toBe('undetermined');
    expect(reset.runs.length).toBeGreaterThan(1);
  });

  it('asks npm for the exact version with its own retry ladder disabled', async () => {
    const npm = fakeNpm([{ exitCode: 0, stdout: `"${VERSION}"\n` }]);

    await npmPublishedVersionStatus('/root', PACKAGE, VERSION, { registry: 'https://forge.test/npm/' }, npm.shell);

    expect(npm.runs[0]).toEqual([
      'view',
      `${PACKAGE}@${VERSION}`,
      'version',
      '--json',
      '--fetch-retries=0',
      '--registry',
      'https://forge.test/npm/',
    ]);
  });
});

interface FakeNpm {
  shell: NpmStatusShell;
  runs: string[][];
  sleeps: number[];
}

/**
 * npm with a scripted transcript: the last scripted result repeats, so a probe
 * under a sustained outage sees the same failure however many attempts it
 * makes. Sleeps are recorded rather than taken.
 */
function fakeNpm(transcript: Array<Partial<NpmCommandResult> & { exitCode: number }>): FakeNpm {
  const runs: string[][] = [];
  const sleeps: number[] = [];
  return {
    runs,
    sleeps,
    shell: {
      run: async (args) => {
        runs.push(args);
        const scripted = transcript[Math.min(runs.length - 1, transcript.length - 1)];
        return { exitCode: scripted.exitCode, stdout: scripted.stdout ?? '', stderr: scripted.stderr ?? '' };
      },
      sleep: async (ms) => {
        sleeps.push(ms);
      },
    },
  };
}
