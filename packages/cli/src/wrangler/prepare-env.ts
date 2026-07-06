/**
 * Composable primitives for writing a project's own `prepare-env` script — the
 * provisioning half of the wrangler setup flow (the `wrangler` monorepo pack owns
 * the build-time half: the `wrangler-types` target + `.dev.vars.example`).
 *
 * `wrangler types --env-file .dev.vars.example` emits a machine-readable manifest
 * that distinguishes committed public **vars** (literal-typed in `[env.<env>.vars]`)
 * from **secrets** (string-typed, names in `.dev.vars.example`, values never in the
 * repo). `readManifest` reads that split back so a script knows what to prompt for;
 * the toml editors clone/blank/fill env blocks; the wrangler shell-outs provision.
 *
 * Import from a target's `scripts/prepare-env.ts`:
 *   import * as pe from '@smoothbricks/cli/wrangler/prepare-env';
 * Scaffold a starter with:  smoo wrangler scaffold <project>
 *
 * Prompt-agnostic by design: the module does the toml + wrangler work; the script
 * brings its own prompts (e.g. @clack/prompts). Bun-only (uses `bun`'s `$`).
 */
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { $ } from 'bun';
import { parse } from 'smol-toml';

// ── runtime narrowing (wrangler JSON + parsed toml are external data — no casts) ─

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function escapeRe(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ── pure wrangler.toml editors (no I/O — unit-testable) ──────────────────────

/** First `[env.<name>]` header, or `null` when the config declares no envs (top-level bindings only). */
export function firstWranglerEnv(toml: string): string | null {
  const match = toml.match(/^\s*\[env\.([A-Za-z0-9_-]+)/m);
  return match ? match[1] : null;
}

/** True if the toml declares `[env.<env>]`. */
export function hasEnvBlock(toml: string, env: string): boolean {
  return new RegExp(`^\\[env\\.${escapeRe(env)}\\]`, 'm').test(toml);
}

/** A `[env.<env>.vars]` value, or null if unset/empty. Reads committed public config. */
export function getVar(toml: string, env: string, name: string): string | null {
  const block = envRecord(toml, env);
  const vars = isRecord(block?.vars) ? block.vars : {};
  const value = vars[name];
  return typeof value === 'string' && value ? value : null;
}

/** Set a `[env.<env>.vars]` value, preserving any trailing comment. Throws if the key isn't present (scaffold first). */
export function setVar(toml: string, env: string, name: string, value: string): string {
  const re = new RegExp(`(\\[env\\.${escapeRe(env)}\\.vars\\][\\s\\S]*?\\n${escapeRe(name)} = )"[^"]*"`);
  if (!re.test(toml)) {
    throw new Error(`vars key "${name}" not found under [env.${env}.vars]`);
  }
  return toml.replace(re, (_m: string, prefix: string) => `${prefix}${JSON.stringify(value)}`);
}

/** The id written for `[[env.<env>.kv_namespaces]]` binding, or null if absent/empty. */
export function getKvId(toml: string, env: string, binding: string): string | null {
  const block = envRecord(toml, env);
  const rows = isRecord(block) && Array.isArray(block.kv_namespaces) ? block.kv_namespaces : [];
  for (const row of rows) {
    if (isRecord(row) && row.binding === binding && typeof row.id === 'string') {
      return row.id || null;
    }
  }
  return null;
}

/** Rewrite the `id = ...` line of the `[[env.<env>.kv_namespaces]]` table whose binding matches. Throws if absent. */
export function setKvId(toml: string, env: string, binding: string, id: string, title: string): string {
  const re = new RegExp(
    `(\\[\\[env\\.${escapeRe(env)}\\.kv_namespaces\\]\\]\\s*\\n\\s*binding = "${escapeRe(binding)}"\\s*\\n\\s*id = )[^\\n]*`,
  );
  if (!re.test(toml)) {
    throw new Error(`kv_namespaces binding "${binding}" not found under [env.${env}]`);
  }
  return toml.replace(re, `$1"${id}" # ${title}`);
}

/**
 * Append a copy of the whole `[env.<from>]` … block-group (every `[env.<from>...]`
 * table up to the next non-`from` env or EOF) rewritten for `<to>`. Returns the new
 * toml. Throws if `<from>` is absent or `<to>` already exists (blank/fill instead).
 */
export function cloneEnvBlock(toml: string, from: string, to: string): string {
  if (!hasEnvBlock(toml, from)) throw new Error(`source [env.${from}] not found`);
  if (hasEnvBlock(toml, to)) throw new Error(`target [env.${to}] already exists — blank/fill it instead`);
  const lines = toml.split('\n');
  const isFromHeader = (l: string) => new RegExp(`^\\[+env\\.${escapeRe(from)}(\\.|\\])`).test(l);
  const isAnyEnvHeader = (l: string) => /^\[+env\.[A-Za-z0-9_-]+(\.|\])/.test(l);
  const out: string[] = [];
  let capturing = false;
  for (const line of lines) {
    if (isFromHeader(line)) {
      capturing = true;
      out.push(line);
    } else if (capturing && isAnyEnvHeader(line) && !isFromHeader(line)) {
      capturing = false; // a different env began — stop
    } else if (capturing) {
      out.push(line);
    }
  }
  const cloned = out
    .map((l) => l.replace(new RegExp(`(\\[+env\\.)${escapeRe(from)}(\\.|\\])`, 'g'), `$1${to}$2`))
    .join('\n');
  return `${toml.replace(/\s*$/, '')}\n\n${cloned}\n`;
}

/** Blank every `NAME = "..."` under `[env.<env>.vars]` whose key is in `keys` (env-specific values to re-fill). */
export function blankEnvVars(toml: string, env: string, keys: string[]): string {
  let out = toml;
  for (const key of keys) {
    const re = new RegExp(`(\\[env\\.${escapeRe(env)}\\.vars\\][\\s\\S]*?\\n${escapeRe(key)} = )"[^"]*"`);
    if (re.test(out)) out = out.replace(re, '$1""');
  }
  return out;
}

/** Blank every `id = ...` line under the env's `[[env.<env>.kv_namespaces]]` tables (per-env resource ids). */
export function blankKvIds(toml: string, env: string): string {
  const re = new RegExp(
    `(\\[\\[env\\.${escapeRe(env)}\\.kv_namespaces\\]\\]\\s*\\n\\s*binding = "[^"]*"\\s*\\n\\s*id = )[^\\n]*`,
    'g',
  );
  return toml.replace(re, '$1""');
}

/** Parse-guard: throws if `toml` no longer parses, so a bad edit can't be written to disk. */
export function assertToml(toml: string): void {
  parse(toml);
}

/** Persist a toml string after confirming it still parses. */
export function saveToml(path: string, toml: string): void {
  assertToml(toml);
  writeFileSync(path, toml);
}

function envRecord(toml: string, env: string): Record<string, unknown> | null {
  const parsed = parse(toml);
  const envs = isRecord(parsed) && isRecord(parsed.env) ? parsed.env : {};
  const block = envs[env];
  return isRecord(block) ? block : null;
}

// ── manifest: what the worker needs, split into vars vs secrets ──────────────

export interface Manifest {
  /** KV binding names declared under `[[env.<env>.kv_namespaces]]` (resources to provision). */
  kvBindings: string[];
  /** Public var names declared in `[env.<env>.vars]` (committed config). */
  vars: string[];
  /** Secret names declared in `.dev.vars.example` (values live on the worker, never in the repo). */
  secrets: string[];
}

/** Secret NAMES from a `.dev.vars.example` file (KEY=... / KEY="..."), ignoring comments/blanks. */
export function parseDevVarsExample(text: string): string[] {
  const names: string[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=/);
    if (m) names.push(m[1]);
  }
  return names;
}

/**
 * Read the vars/secrets split for `env` from `<root>/wrangler.toml` (public var
 * keys) + `<root>/.dev.vars.example` (secret names). This is the "what to prompt
 * for" manifest a prepare-env script derives its work from.
 */
export function readManifest(root: string, env: string): Manifest {
  const toml = readFileSync(join(root, 'wrangler.toml'), 'utf8');
  const block = envRecord(toml, env);
  const varsRec = isRecord(block?.vars) ? block.vars : {};
  const vars = Object.keys(varsRec);
  const kvRows = isRecord(block) && Array.isArray(block.kv_namespaces) ? block.kv_namespaces : [];
  const kvBindings = kvRows.flatMap((row) => (isRecord(row) && typeof row.binding === 'string' ? [row.binding] : []));
  const examplePath = join(root, '.dev.vars.example');
  const secrets = existsSync(examplePath) ? parseDevVarsExample(readFileSync(examplePath, 'utf8')) : [];
  return { kvBindings, vars, secrets };
}

// ── PEM (a standard, reusable) ───────────────────────────────────────────────

/** True if `value` looks like a PEM block (any `-----BEGIN ...-----`). */
export function isPem(value: string): boolean {
  return /-----BEGIN [A-Z ]+-----/.test(value);
}

/** Read a `.pem` file (expanding a leading `~`) and assert it's a PEM block. Throws otherwise. */
export function readPemFile(path: string): string {
  const resolved = path.replace(/^~/, process.env.HOME ?? '~');
  const content = readFileSync(resolved, 'utf8');
  if (!isPem(content)) throw new Error(`${path} is not a PEM (no -----BEGIN block)`);
  return content;
}

/** Heuristic: does this secret name suggest file input (PEM/key)? Used to preselect a file prompt — never to change semantics. */
export function looksLikeFileSecret(name: string): boolean {
  return /_PEM$|_KEY$|_CERT$/.test(name);
}

// ── wrangler shell-outs (Bun $ + structured JSON), run at `cwd` ───────────────

export interface KvNamespace {
  id: string;
  title: string;
}

function asKvNamespaces(json: unknown): KvNamespace[] {
  if (!Array.isArray(json)) return [];
  const out: KvNamespace[] = [];
  for (const row of json) {
    if (isRecord(row) && typeof row.id === 'string' && typeof row.title === 'string') {
      out.push({ id: row.id, title: row.title });
    }
  }
  return out;
}

function asSecretNames(json: unknown): string[] {
  if (!Array.isArray(json)) return [];
  return json.flatMap((row) => (isRecord(row) && typeof row.name === 'string' ? [row.name] : []));
}

/** Best-effort ERROR line from a Bun ShellError (thrown on non-zero exit). */
export function errText(err: unknown): string {
  if (!isRecord(err)) return 'unknown error';
  const stderr = 'stderr' in err && err.stderr != null ? String(err.stderr) : '';
  const exit = 'exitCode' in err ? String(err.exitCode) : '?';
  return stderr.split('\n').find((l) => l.includes('ERROR')) ?? `exit ${exit}`;
}

/** Live KV namespaces on the account (`kv namespace list` emits a pure JSON array); `[]` when offline/unauthenticated. */
export async function listNamespaces(cwd: string): Promise<KvNamespace[]> {
  try {
    return asKvNamespaces(await $`bunx wrangler kv namespace list`.cwd(cwd).quiet().json());
  } catch {
    return [];
  }
}

/**
 * Reuse the namespace whose title exactly matches `logical`, else create it. The
 * id always comes from the structured `list` JSON (create prints freeform text),
 * so we re-list rather than scrape. Returns the id, or null on failure.
 */
export async function ensureNamespace(logical: string, existing: KvNamespace[], cwd: string): Promise<string | null> {
  const match = existing.find((n) => n.title === logical);
  if (match) return match.id;
  try {
    await $`bunx wrangler kv namespace create ${logical}`.cwd(cwd).quiet();
  } catch (err) {
    return errText(err).includes('already') ? (await reList(logical, cwd)) : null;
  }
  return reList(logical, cwd);
}

async function reList(logical: string, cwd: string): Promise<string | null> {
  return (await listNamespaces(cwd)).find((n) => n.title === logical)?.id ?? null;
}

/** Current secret names on the worker for `env`; `[]` when none/unreachable (Cloudflare allows setting pre-deploy). */
export async function listSecretNames(env: string, cwd: string): Promise<string[]> {
  try {
    return asSecretNames(await $`bunx wrangler secret list --format json --env ${env}`.cwd(cwd).quiet().json());
  } catch {
    return [];
  }
}

/** `wrangler secret put <name> --env <env>` piping `value` on stdin (never a CLI arg). Returns success. */
export async function putSecret(name: string, value: string, env: string, cwd: string): Promise<boolean> {
  try {
    await $`bunx wrangler secret put ${name} --env ${env} < ${Buffer.from(value)}`.cwd(cwd).quiet();
    return true;
  } catch {
    return false;
  }
}
