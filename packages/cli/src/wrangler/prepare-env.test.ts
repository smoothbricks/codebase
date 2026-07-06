import { describe, expect, it } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  assertToml,
  blankD1Ids,
  blankEnvVars,
  blankKvIds,
  cloneEnvBlock,
  firstWranglerEnv,
  getD1Id,
  getD1Name,
  getKvId,
  getVar,
  hasEnvBlock,
  isPem,
  looksLikeFileSecret,
  parseDevVarsExample,
  readManifest,
  readPemFile,
  setD1Id,
  setKvId,
  setVar,
} from './prepare-env.js';

// The wrangler shell-outs (listNamespaces / ensureNamespace / listSecretNames /
// putSecret) invoke `bunx wrangler` against a live account and are integration-
// only — deliberately not exercised here. Everything below is a pure primitive.

describe('firstWranglerEnv', () => {
  it('returns the name of the first [env.<name>] header', () => {
    expect(firstWranglerEnv('name = "svc"\n\n[env.staging]\n\n[env.production]\n')).toBe('staging');
  });

  it('derives the env from a nested [env.<name>.vars] sub-table', () => {
    expect(firstWranglerEnv('[env.prod.vars]\nFOO = "bar"\n')).toBe('prod');
  });

  it('returns null when the toml declares no env block', () => {
    expect(firstWranglerEnv('name = "svc"\nmain = "src/index.ts"\n')).toBeNull();
  });
});

describe('hasEnvBlock', () => {
  const toml = 'name = "svc"\n\n[env.production]\nroute = "x"\n';

  it('is true for a declared [env.<env>] block', () => {
    expect(hasEnvBlock(toml, 'production')).toBe(true);
  });

  it('is false for an env the toml never declares', () => {
    expect(hasEnvBlock(toml, 'staging')).toBe(false);
  });
});

describe('getVar / setVar', () => {
  const toml =
    'name = "svc"\n\n[env.staging.vars]\nTOKEN = "staging-token"\n\n[env.production.vars]\nTOKEN = "production-token"\nEMPTY = ""\n';

  it('reads a set value and returns null for empty or absent keys', () => {
    expect(getVar(toml, 'production', 'TOKEN')).toBe('production-token');
    expect(getVar(toml, 'production', 'EMPTY')).toBeNull();
    expect(getVar(toml, 'production', 'MISSING')).toBeNull();
  });

  it('rewrites the target value while preserving a trailing comment', () => {
    const src = '[env.production.vars]\nAPI_URL = "https://old.example.com" # keep this comment\n';
    const out = setVar(src, 'production', 'API_URL', 'https://new.example.com');
    expect(out).toContain('# keep this comment');
    expect(getVar(out, 'production', 'API_URL')).toBe('https://new.example.com');
  });

  it('edits only the target env block, leaving a same-named key under another env untouched', () => {
    const out = setVar(toml, 'production', 'TOKEN', 'rotated-prod');
    expect(getVar(out, 'production', 'TOKEN')).toBe('rotated-prod');
    expect(getVar(out, 'staging', 'TOKEN')).toBe('staging-token');
  });

  it('JSON-encodes a value with $ and quotes so the result still parses', () => {
    const src = '[env.production.vars]\nSECRET_HINT = "placeholder"\n';
    const value = 'a$b"c';
    const out = setVar(src, 'production', 'SECRET_HINT', value);
    expect(() => assertToml(out)).not.toThrow();
    expect(getVar(out, 'production', 'SECRET_HINT')).toBe(value);
  });

  it('throws when the vars key is absent (scaffold-first contract)', () => {
    expect(() => setVar(toml, 'production', 'NOT_THERE', 'x')).toThrow();
  });
});

describe('getKvId / setKvId', () => {
  const toml =
    'name = "svc"\n\n' +
    '[[env.production.kv_namespaces]]\nbinding = "CACHE"\nid = "cache-id-123"\n\n' +
    '[[env.production.kv_namespaces]]\nbinding = "SESSIONS"\nid = ""\n';

  it('reads the id for a matching binding and null for empty/absent bindings', () => {
    expect(getKvId(toml, 'production', 'CACHE')).toBe('cache-id-123');
    expect(getKvId(toml, 'production', 'SESSIONS')).toBeNull();
    expect(getKvId(toml, 'production', 'MISSING')).toBeNull();
  });

  it('writes "<id>" # <title> against the right binding, leaving siblings intact', () => {
    const out = setKvId(toml, 'production', 'SESSIONS', 'sess-id-456', 'svc-sessions-production');
    expect(out).toContain('# svc-sessions-production');
    expect(getKvId(out, 'production', 'SESSIONS')).toBe('sess-id-456');
    expect(getKvId(out, 'production', 'CACHE')).toBe('cache-id-123');
  });

  it('throws when the binding block is absent', () => {
    expect(() => setKvId(toml, 'production', 'MISSING', 'x', 't')).toThrow();
  });
});

describe('cloneEnvBlock', () => {
  const staging =
    'name = "svc"\n\n' +
    '[env.staging]\nroute = "staging.example.com"\n\n' +
    '[env.staging.vars]\nLEVEL = "debug"\n\n' +
    '[[env.staging.kv_namespaces]]\nbinding = "CACHE"\nid = "staging-cache"\n';

  it('appends a rewritten copy while leaving the source block intact', () => {
    const out = cloneEnvBlock(staging, 'staging', 'production');
    expect(() => assertToml(out)).not.toThrow();
    expect(hasEnvBlock(out, 'production')).toBe(true);
    // source untouched
    expect(getVar(out, 'staging', 'LEVEL')).toBe('debug');
    expect(getKvId(out, 'staging', 'CACHE')).toBe('staging-cache');
    // cloned vars + kv tables present under the new env
    expect(getVar(out, 'production', 'LEVEL')).toBe('debug');
    expect(getKvId(out, 'production', 'CACHE')).toBe('staging-cache');
  });

  it('throws when the source env is missing', () => {
    expect(() => cloneEnvBlock(staging, 'qa', 'production')).toThrow();
  });

  it('throws when the target env already exists', () => {
    const withBoth = `${staging}\n[env.production]\nroute = "prod.example.com"\n`;
    expect(() => cloneEnvBlock(withBoth, 'staging', 'production')).toThrow();
  });
});

describe('blankEnvVars / blankKvIds', () => {
  it('blanks the named vars and leaves the others untouched', () => {
    const toml = '[env.production.vars]\nKEEP = "keep-value"\nDROP_A = "a"\nDROP_B = "b"\n';
    const out = blankEnvVars(toml, 'production', ['DROP_A', 'DROP_B']);
    expect(getVar(out, 'production', 'DROP_A')).toBeNull();
    expect(getVar(out, 'production', 'DROP_B')).toBeNull();
    expect(getVar(out, 'production', 'KEEP')).toBe('keep-value');
  });

  it('blanks every kv id under the env while preserving the bindings', () => {
    const toml =
      '[[env.production.kv_namespaces]]\nbinding = "CACHE"\nid = "cache-id"\n\n' +
      '[[env.production.kv_namespaces]]\nbinding = "SESSIONS"\nid = "sess-id"\n';
    const out = blankKvIds(toml, 'production');
    expect(getKvId(out, 'production', 'CACHE')).toBeNull();
    expect(getKvId(out, 'production', 'SESSIONS')).toBeNull();
    // bindings survived: setKvId still resolves them and re-fills an id
    const refilled = setKvId(out, 'production', 'CACHE', 'new-cache', 'title');
    expect(getKvId(refilled, 'production', 'CACHE')).toBe('new-cache');
  });
});

// D1 shell-outs (listD1Databases / ensureD1Database) invoke `bunx wrangler d1`
// against a live account and are integration-only — deliberately not exercised.

describe('getD1Id / getD1Name', () => {
  const toml =
    '[env.staging]\n\n' +
    '[[env.staging.d1_databases]]\nbinding = "DB"\ndatabase_id = "abc123"\ndatabase_name = "app-staging-db"\n\n' +
    '[env.production]\n\n' +
    '[[env.production.d1_databases]]\nbinding = "DB"\ndatabase_id = ""\ndatabase_name = "app-db"\n';

  it('reads the id for a matching binding and null for empty id, absent binding, and absent env', () => {
    expect(getD1Id(toml, 'staging', 'DB')).toBe('abc123');
    expect(getD1Id(toml, 'production', 'DB')).toBeNull(); // production id is ""
    expect(getD1Id(toml, 'staging', 'MISSING')).toBeNull(); // absent binding
    expect(getD1Id(toml, 'qa', 'DB')).toBeNull(); // absent env
  });

  it('reads the database_name for a matching binding and null when the binding or env is absent', () => {
    expect(getD1Name(toml, 'staging', 'DB')).toBe('app-staging-db');
    expect(getD1Name(toml, 'production', 'DB')).toBe('app-db'); // present even though its id is blank
    expect(getD1Name(toml, 'staging', 'MISSING')).toBeNull();
    expect(getD1Name(toml, 'qa', 'DB')).toBeNull();
  });
});

describe('setD1Id', () => {
  const toml =
    '[[env.staging.d1_databases]]\nbinding = "DB"\ndatabase_id = ""\ndatabase_name = "app-staging-db"\n\n' +
    '[[env.staging.d1_databases]]\nbinding = "ANALYTICS"\ndatabase_id = ""\ndatabase_name = "app-staging-analytics"\n\n' +
    '[[env.production.d1_databases]]\nbinding = "DB"\ndatabase_id = "prod-existing"\ndatabase_name = "app-db"\n';

  it('writes "<id>" # <name> against the matching env+binding, leaving siblings and other envs intact', () => {
    const out = setD1Id(toml, 'staging', 'DB', 'new-staging-id', 'app-staging-db');
    expect(out).toContain('"new-staging-id" # app-staging-db');
    expect(getD1Id(out, 'staging', 'DB')).toBe('new-staging-id'); // round-trip
    expect(getD1Id(out, 'staging', 'ANALYTICS')).toBeNull(); // sibling binding untouched
    expect(getD1Id(out, 'production', 'DB')).toBe('prod-existing'); // other env untouched
  });

  it('tolerates database_name preceding database_id and preserves the name', () => {
    const nameFirst =
      '[[env.staging.d1_databases]]\nbinding = "DB"\ndatabase_name = "app-staging-db"\ndatabase_id = "old"\n';
    const out = setD1Id(nameFirst, 'staging', 'DB', 'fresh', 'app-staging-db');
    expect(getD1Id(out, 'staging', 'DB')).toBe('fresh');
    expect(getD1Name(out, 'staging', 'DB')).toBe('app-staging-db'); // name line not clobbered
    expect(() => assertToml(out)).not.toThrow();
  });

  it('throws when the binding or the env is absent', () => {
    expect(() => setD1Id(toml, 'staging', 'MISSING', 'x', 'n')).toThrow();
    expect(() => setD1Id(toml, 'qa', 'DB', 'x', 'n')).toThrow();
  });
});

describe('blankD1Ids', () => {
  const toml =
    '[[env.staging.d1_databases]]\nbinding = "DB"\ndatabase_id = "staging-db-id"\ndatabase_name = "app-staging-db"\n\n' +
    '[[env.staging.d1_databases]]\nbinding = "ANALYTICS"\ndatabase_id = "staging-an-id"\ndatabase_name = "app-staging-analytics"\n\n' +
    '[[env.production.d1_databases]]\nbinding = "DB"\ndatabase_id = "prod-db-id"\ndatabase_name = "app-db"\n';

  it('blanks every d1 id under the env while names, bindings, and the other env survive', () => {
    const out = blankD1Ids(toml, 'staging');
    expect(getD1Id(out, 'staging', 'DB')).toBeNull();
    expect(getD1Id(out, 'staging', 'ANALYTICS')).toBeNull();
    // names survive the blanking
    expect(getD1Name(out, 'staging', 'DB')).toBe('app-staging-db');
    expect(getD1Name(out, 'staging', 'ANALYTICS')).toBe('app-staging-analytics');
    // other env's id is untouched
    expect(getD1Id(out, 'production', 'DB')).toBe('prod-db-id');
    // bindings survived: setD1Id still resolves them and re-fills an id
    const refilled = setD1Id(out, 'staging', 'DB', 're-provisioned', 'app-staging-db');
    expect(getD1Id(refilled, 'staging', 'DB')).toBe('re-provisioned');
  });
});

describe('assertToml', () => {
  it('returns for valid toml', () => {
    expect(() => assertToml('name = "svc"\n[env.production]\n')).not.toThrow();
  });

  it('throws for malformed toml', () => {
    expect(() => assertToml('[env.\nbad')).toThrow();
  });
});

describe('parseDevVarsExample', () => {
  it('extracts KEY names from KEY= and KEY="" lines, ignoring comments and blanks', () => {
    const text = '# secrets\nAPI_KEY=\n\nJWT_SECRET=""\n# trailing note\n';
    expect(parseDevVarsExample(text)).toEqual(['API_KEY', 'JWT_SECRET']);
  });
});

describe('readManifest', () => {
  const toml =
    'name = "svc"\n\n' +
    '[env.staging.vars]\nPUBLIC_URL = "https://staging"\nFEATURE_FLAG = "on"\n\n' +
    '[[env.staging.kv_namespaces]]\nbinding = "CACHE"\nid = "abc"\n\n' +
    '[[env.staging.kv_namespaces]]\nbinding = "SESSIONS"\nid = "def"\n';

  it('splits vars, kv bindings, and secrets for the requested env', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-prepare-env-'));
    try {
      await writeFile(join(root, 'wrangler.toml'), toml);
      await writeFile(join(root, '.dev.vars.example'), '# secrets\nAPI_KEY=\nJWT_SECRET=""\n');
      expect(readManifest(root, 'staging')).toEqual({
        kvBindings: ['CACHE', 'SESSIONS'],
        d1Bindings: [],
        vars: ['PUBLIC_URL', 'FEATURE_FLAG'],
        secrets: ['API_KEY', 'JWT_SECRET'],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('returns an empty secrets list when .dev.vars.example is absent', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-prepare-env-'));
    try {
      await writeFile(join(root, 'wrangler.toml'), toml);
      const manifest = readManifest(root, 'staging');
      expect(manifest.secrets).toEqual([]);
      expect(manifest.vars).toEqual(['PUBLIC_URL', 'FEATURE_FLAG']);
      expect(manifest.kvBindings).toEqual(['CACHE', 'SESSIONS']);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('reports d1 bindings alongside kv bindings and vars without dropping the existing fields', async () => {
    const withD1 =
      'name = "svc"\n\n' +
      '[env.staging.vars]\nPUBLIC_URL = "https://staging"\n\n' +
      '[[env.staging.kv_namespaces]]\nbinding = "CACHE"\nid = "abc"\n\n' +
      '[[env.staging.d1_databases]]\nbinding = "DB"\ndatabase_id = "id-1"\ndatabase_name = "app-staging-db"\n\n' +
      '[[env.staging.d1_databases]]\nbinding = "ANALYTICS"\ndatabase_id = ""\ndatabase_name = "app-analytics-db"\n';
    const root = await mkdtemp(join(tmpdir(), 'smoo-prepare-env-'));
    try {
      await writeFile(join(root, 'wrangler.toml'), withD1);
      expect(readManifest(root, 'staging')).toEqual({
        kvBindings: ['CACHE'],
        d1Bindings: ['DB', 'ANALYTICS'],
        vars: ['PUBLIC_URL'],
        secrets: [],
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe('isPem / readPemFile / looksLikeFileSecret', () => {
  it('recognizes a -----BEGIN block and rejects arbitrary text', () => {
    expect(isPem('-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n')).toBe(true);
    expect(isPem('-----BEGIN RSA PRIVATE KEY-----\n')).toBe(true);
    expect(isPem('just a plain secret')).toBe(false);
  });

  it('reads a PEM file and throws on a non-PEM file', async () => {
    const root = await mkdtemp(join(tmpdir(), 'smoo-prepare-env-'));
    try {
      const pem = '-----BEGIN CERTIFICATE-----\nMIIBcontent\n-----END CERTIFICATE-----\n';
      const pemPath = join(root, 'cert.pem');
      const plainPath = join(root, 'plain.txt');
      await writeFile(pemPath, pem);
      await writeFile(plainPath, 'not a pem at all\n');
      expect(readPemFile(pemPath)).toBe(pem);
      expect(() => readPemFile(plainPath)).toThrow();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('flags _PEM/_KEY/_CERT suffixes as file secrets and nothing else', () => {
    expect(looksLikeFileSecret('SIGNING_PEM')).toBe(true);
    expect(looksLikeFileSecret('SIGNING_KEY')).toBe(true);
    expect(looksLikeFileSecret('SIGNING_CERT')).toBe(true);
    expect(looksLikeFileSecret('API_TOKEN')).toBe(false);
    expect(looksLikeFileSecret('PEM_HEADER')).toBe(false);
  });
});
