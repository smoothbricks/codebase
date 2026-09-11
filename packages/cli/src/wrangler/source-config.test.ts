import { afterEach, describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  discoverWranglerSourceConfig,
  parseWranglerConfig,
  readWranglerSourceConfig,
  WRANGLER_SOURCE_CONFIG_FILES,
} from './source-config.js';

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

/** A project directory holding exactly the named config files. */
async function project(files: Record<string, string>): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-wrangler-source-'));
  roots.push(root);
  for (const [name, content] of Object.entries(files)) {
    await writeFile(join(root, name), content);
  }
  return root;
}

describe('discovering the config wrangler itself would read', () => {
  it('follows wrangler precedence for each spelling on its own', async () => {
    for (const file of WRANGLER_SOURCE_CONFIG_FILES) {
      const root = await project({ [file]: file.endsWith('.toml') ? 'name = "acme"\n' : '{ "name": "acme" }' });
      expect(discoverWranglerSourceConfig(root).path).toBe(join(root, file));
    }
  });

  it('refuses two configs, naming every one and the one wrangler would have silently taken', async () => {
    const root = await project({
      'wrangler.jsonc': '{ "name": "acme-from-jsonc" }',
      'wrangler.toml': 'name = "acme-from-toml"\n',
    });

    // Verified against wrangler 4.131.0: with both present it reads the .jsonc and says nothing.
    expect(() => discoverWranglerSourceConfig(root)).toThrow(
      `${root} declares more than one Wrangler configuration: wrangler.jsonc, wrangler.toml. Wrangler would read wrangler.jsonc and ignore the rest without saying so; keep exactly one.`,
    );
  });

  it('refuses a project with no config, naming the spellings it looked for', async () => {
    const root = await project({});

    expect(() => discoverWranglerSourceConfig(root)).toThrow(
      `${root} declares no Wrangler configuration (wrangler.jsonc, wrangler.json, wrangler.toml); pass --config for a build-generated flat configuration.`,
    );
  });

  it('reads and parses the discovered file in one step', async () => {
    const root = await project({ 'wrangler.jsonc': '{\n  // the worker\n  "name": "acme",\n}\n' });

    expect(readWranglerSourceConfig(root)).toEqual({ path: join(root, 'wrangler.jsonc'), document: { name: 'acme' } });
  });
});

describe('parsing a source config', () => {
  it('tolerates comments and trailing commas in wrangler.json too, exactly as wrangler does', () => {
    const document = parseWranglerConfig(
      'wrangler.json',
      '{\n  /* recommended spelling is .jsonc, but wrangler parses both the same way */\n  "name": "acme",\n  "compatibility_flags": ["nodejs_compat"],\n}\n',
      'jsonc',
    );

    expect(document).toEqual({ name: 'acme', compatibility_flags: ['nodejs_compat'] });
  });

  it('names the file when the text is not the format it is spelled as', () => {
    expect(() => parseWranglerConfig('wrangler.jsonc', '{ "name": ', 'jsonc')).toThrow(
      /^wrangler\.jsonc is not valid JSONC: /,
    );
    expect(() => parseWranglerConfig('wrangler.toml', '[env.\nbad', 'toml')).toThrow(
      /^wrangler\.toml is not valid TOML: /,
    );
  });

  it('refuses anything that is not a configuration object', () => {
    expect(() => parseWranglerConfig('wrangler.jsonc', '[1, 2]', 'jsonc')).toThrow(
      'wrangler.jsonc is not a Wrangler configuration document: expected an object, optionally with env blocks.',
    );
  });

  it('refuses a bare TOML date rather than shifting it through JSON', () => {
    // The temporary per-stage config is JSON whatever the source was, and JSON.stringify would
    // turn this into an ISO timestamp in the deploying machine's timezone.
    expect(() => parseWranglerConfig('wrangler.toml', 'compatibility_date = 2026-05-06\n', 'toml')).toThrow(
      'wrangler.toml declares a bare TOML date or time at compatibility_date, which JSON cannot carry; quote it as a string.',
    );
  });

  it('refuses a non-finite TOML number rather than writing null for it', () => {
    expect(() => parseWranglerConfig('wrangler.toml', '[env.staging.vars]\nBUDGET = inf\n', 'toml')).toThrow(
      'wrangler.toml declares Infinity at env.staging.vars.BUDGET, which JSON cannot carry; write a finite number.',
    );
  });
});
