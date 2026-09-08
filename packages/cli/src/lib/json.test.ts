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
      /^package\.json is invalid: smoo\.github\.deploySecrets\.API_TOKEN: expected /,
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

describe('readSmooGithub value validation', () => {
  let root: string;

  const write = async (manifest: object): Promise<void> => {
    await writeFile(join(root, 'package.json'), JSON.stringify(manifest));
  };

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'smoo-github-values-'));
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('names an explicitly empty pushBranches instead of silently falling back to main', async () => {
    await write({ name: 'repo', smoo: { github: { pushBranches: [] } } });
    expect(() => readSmooGithub(root)).toThrow(/smoo\.github\.pushBranches/);
  });

  it('names a blank branch entry, which would render an unusable workflow trigger', async () => {
    await write({ name: 'repo', smoo: { github: { pushBranches: ['', 'main'] } } });
    expect(() => readSmooGithub(root)).toThrow(/smoo\.github\.pushBranches/);
  });

  it('names a secret reference GitHub could never resolve', async () => {
    await write({
      name: 'repo',
      smoo: { github: { deploySecrets: { API_TOKEN: 'GITHUB_TOKEN' } } },
    });
    expect(() => readSmooGithub(root)).toThrow(/smoo\.github\.deploySecrets\.API_TOKEN/);
  });

  it('accepts a secret reference whose name merely embeds GITHUB_', async () => {
    await write({
      name: 'repo',
      smoo: { github: { deploySecrets: { GITHUB_CLIENT_SECRET: 'SMOO_GITHUB_CLIENT_SECRET' } } },
    });
    expect(readSmooGithub(root)).toEqual({
      deploySecrets: { GITHUB_CLIENT_SECRET: 'SMOO_GITHUB_CLIENT_SECRET' },
    });
  });

  it('names a preview URL template that would publish one URL for every pull request', async () => {
    await write({
      name: 'repo',
      smoo: { github: { previewUrls: ['https://app.example.test'] } },
    });
    expect(() => readSmooGithub(root)).toThrow(/smoo\.github\.previewUrls/);
  });

  it('accepts valid push branches, secret maps, and preview templates together', async () => {
    await write({
      name: 'repo',
      smoo: {
        github: {
          pushBranches: ['private', 'release'],
          deploySecrets: { API_TOKEN: 'SMOO_API_TOKEN' },
          e2eSecrets: { CONTROL_TOKEN: 'SMOO_CONTROL_TOKEN' },
          previewUrls: ['https://app.{stage}.example.test'],
        },
      },
    });
    expect(readSmooGithub(root)).toEqual({
      pushBranches: ['private', 'release'],
      deploySecrets: { API_TOKEN: 'SMOO_API_TOKEN' },
      e2eSecrets: { CONTROL_TOKEN: 'SMOO_CONTROL_TOKEN' },
      previewUrls: ['https://app.{stage}.example.test'],
    });
  });
});
