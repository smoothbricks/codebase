import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ciPushBranches, readSmooGithub } from './json.js';

describe('ciPushBranches', () => {
  it('reads the configured branches in order, the first being the staging one', () => {
    expect(ciPushBranches({ pushBranches: ['private', 'release'] })).toEqual(['private', 'release']);
  });

  it('falls back to main when no branch is configured', () => {
    expect(ciPushBranches(undefined)).toEqual(['main']);
    expect(ciPushBranches({})).toEqual(['main']);
    expect(ciPushBranches({ pushBranches: [] })).toEqual(['main']);
  });

  it('ignores blank entries, which would render an unusable workflow trigger', () => {
    expect(ciPushBranches({ pushBranches: ['', 'main'] })).toEqual(['main']);
    expect(ciPushBranches({ pushBranches: [''] })).toEqual(['main']);
  });
});

describe('readSmooGithub', () => {
  let root: string;

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'smoo-github-'));
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('returns nothing when there is no package.json or no smoo.github block', async () => {
    expect(readSmooGithub(root)).toBeUndefined();

    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 'repo', smoo: {} }));
    expect(readSmooGithub(root)).toBeUndefined();
  });

  it('reads the block from a manifest that would not pass as a package (no version)', async () => {
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({
        name: 'repo',
        smoo: { github: { pushBranches: ['private'], environments: { staging: 'st' } } },
      }),
    );

    expect(readSmooGithub(root)).toEqual({ pushBranches: ['private'], environments: { staging: 'st' } });
  });

  it('names the offending path instead of silently dropping the whole block', async () => {
    await writeFile(
      join(root, 'package.json'),
      JSON.stringify({ name: 'repo', version: '1.0.0', smoo: { github: { deploySecrets: { API_TOKEN: 1 } } } }),
    );

    expect(() => readSmooGithub(root)).toThrow(
      /^package\.json is invalid: smoo\.github\.deploySecrets\.API_TOKEN: expected string/,
    );
  });

  it('names a wrong value outside the block too, since the manifest is validated as a whole', async () => {
    await writeFile(join(root, 'package.json'), JSON.stringify({ name: 5, smoo: { github: {} } }));

    expect(() => readSmooGithub(root)).toThrow(/^package\.json is invalid: name: expected \(string \| undefined\)/);
  });

  it('names the file when it is not JSON at all', async () => {
    await writeFile(join(root, 'package.json'), '{');

    expect(() => readSmooGithub(root)).toThrow(`${join(root, 'package.json')} is not valid JSON: `);
  });
});
