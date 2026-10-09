import { afterEach, expect, spyOn, test } from 'bun:test';
import { chmodSync, mkdirSync, writeFileSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ttscPlatformPackage, vendoredVersionCandidates } from './go-toolchain.js';
import { checkPolicyFailures } from './index.js';

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

// A consumer repository ran ttsc 0.30.4 (vendored go1.26.8) under a devenv that provided go1.26.7, and
// `smoo monorepo check` passed: the agreement gate was reachable only from a validate
// path that never enabled it. One root walks the gate's states: no ttsc (nothing to
// check, whatever Go is around), ttsc without a devenv profile (no authoritative Go, so
// the check says to enter the shell rather than compare against PATH), a mismatched
// profile Go (named), and an agreeing one (silent).
test('smoo monorepo check holds the devenv profile Go to the SDK ttsc vendors', async () => {
  const root = await mkdtemp(join(tmpdir(), 'smoo-check-go-'));
  roots.push(root);
  const errors = spyOn(console, 'error').mockImplementation(() => {});
  const goFindings = async (): Promise<string[]> => {
    errors.mockClear();
    await checkPolicyFailures(root, []);
    return errors.mock.calls
      .map((call) => String(call[0]))
      .filter((line) => /devenv profile go|vendors go1\.|`go version`/.test(line));
  };
  const path = process.env.PATH;
  try {
    // No Go on PATH at all for the states with no devenv profile: neither may consult one.
    process.env.PATH = '/usr/bin:/bin';
    expect(await goFindings()).toEqual([]);

    mkdirSync(join(root, 'node_modules', 'ttsc'), { recursive: true });
    writeFileSync(join(root, 'node_modules', 'ttsc', 'package.json'), JSON.stringify({ version: '0.30.4' }));
    const platformPackage = ttscPlatformPackage(process.platform, process.arch);
    const vendoredPath =
      platformPackage === null ? undefined : vendoredVersionCandidates(root, platformPackage, '0.30.4')[1];
    if (vendoredPath === undefined) {
      throw new Error(`no bun store candidate for ${process.platform}/${process.arch}`);
    }
    mkdirSync(join(vendoredPath, '..'), { recursive: true });
    writeFileSync(vendoredPath, 'go1.26.8\ntime 2026-08-28T16:20:06Z\n');
    expect(await goFindings()).toEqual([expect.stringContaining('no devenv profile go')]);
    process.env.PATH = path;

    // The devenv profile's go is what the gate asks; this one reports the shell's release.
    const profileBin = join(root, 'tooling', 'direnv', '.devenv', 'profile', 'bin');
    mkdirSync(profileBin, { recursive: true });
    const devenvGo = (version: string): void => {
      writeFileSync(
        join(profileBin, 'go'),
        `#!/bin/sh\necho "go version ${version} ${process.platform}/${process.arch}"\n`,
      );
      chmodSync(join(profileBin, 'go'), 0o755);
    };
    devenvGo('go1.26.7');
    expect(await goFindings()).toEqual([
      expect.stringContaining('devenv provides go1.26.7 but ttsc 0.30.4 vendors go1.26.8'),
    ]);

    devenvGo('go1.26.8');
    expect(await goFindings()).toEqual([]);
  } finally {
    process.env.PATH = path;
    errors.mockRestore();
  }
});
