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
import { type AST, getStaticTOMLValue, parseTOML } from 'toml-eslint-parser';

// ── runtime narrowing (wrangler JSON is external data — no casts) ─────────────

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

// ── wrangler.toml editors (CST: read via getStaticTOMLValue, write via
// node-range splice — comment/format-preserving by construction, no regex) ─────

/** All top-level tables (`[env.x]`, `[env.x.vars]`, `[[env.x.d1_databases]]`, …) — each carries `resolvedKey` (incl. array index). */
function tables(toml: string): AST.TOMLTable[] {
  return parseTOML(toml).body[0].body.filter((n): n is AST.TOMLTable => n.type === 'TOMLTable');
}

/** Whole doc as a JS object (comments/format dropped — reads only). */
function tomlValue(toml: string): Record<string, unknown> {
  const value = getStaticTOMLValue(parseTOML(toml));
  return isRecord(value) ? value : {};
}

function envRecord(toml: string, env: string): Record<string, unknown> | null {
  const envs = tomlValue(toml).env;
  const block = isRecord(envs) ? envs[env] : undefined;
  return isRecord(block) ? block : null;
}

function sameKey(a: (string | number)[], b: (string | number)[]): boolean {
  return a.length === b.length && a.every((x, i) => x === b[i]);
}

/** The `key = value` node named `key` in a table (single-segment keys; matched by source text). */
function kvIn(toml: string, table: AST.TOMLTable, key: string): AST.TOMLKeyValue | null {
  for (const kv of table.body) {
    if (toml.slice(kv.key.range[0], kv.key.range[1]).trim() === key) return kv;
  }
  return null;
}

/** The `<field>` KV of the `[[env.<env>.<arrayKey>]]` table whose `binding` matches, or null. */
function arrayTableKv(toml: string, env: string, arrayKey: string, binding: string, field: string): AST.TOMLKeyValue | null {
  const block = envRecord(toml, env);
  const rows = isRecord(block) && Array.isArray(block[arrayKey]) ? block[arrayKey] : [];
  const index = rows.findIndex((r) => isRecord(r) && r.binding === binding);
  if (index < 0) return null;
  const table = tables(toml).find((t) => t.kind === 'array' && sameKey(t.resolvedKey, ['env', env, arrayKey, index]));
  return table ? kvIn(toml, table, field) : null;
}

function splice(toml: string, start: number, end: number, text: string): string {
  return toml.slice(0, start) + text + toml.slice(end);
}

/** End of the physical line containing `offset` (exclusive of the newline). */
function endOfLine(toml: string, offset: number): number {
  const nl = toml.indexOf('\n', offset);
  return nl === -1 ? toml.length : nl;
}

/** First `[env.<name>]` (source order; also derived from a nested `[env.<name>.*]`), or null when no env declared. */
export function firstWranglerEnv(toml: string): string | null {
  const envs = tomlValue(toml).env;
  if (!isRecord(envs)) return null;
  const [first] = Object.keys(envs);
  return first ?? null;
}

/** True if the toml declares `[env.<env>]` (or any `[env.<env>.*]` sub-table). */
export function hasEnvBlock(toml: string, env: string): boolean {
  const envs = tomlValue(toml).env;
  return isRecord(envs) && env in envs;
}

/** A `[env.<env>.vars]` value, or null if unset/empty. Reads committed public config. */
export function getVar(toml: string, env: string, name: string): string | null {
  const block = envRecord(toml, env);
  const vars = isRecord(block?.vars) ? block.vars : {};
  const value = vars[name];
  return typeof value === 'string' && value ? value : null;
}

/** Set a `[env.<env>.vars]` value (JSON-encoded), preserving any trailing comment. Throws if the key is absent (scaffold first). */
export function setVar(toml: string, env: string, name: string, value: string): string {
  const table = tables(toml).find((t) => sameKey(t.resolvedKey, ['env', env, 'vars']));
  const kv = table ? kvIn(toml, table, name) : null;
  if (!kv) throw new Error(`vars key "${name}" not found under [env.${env}.vars]`);
  return splice(toml, kv.value.range[0], kv.value.range[1], JSON.stringify(value));
}

/** The id written for `[[env.<env>.kv_namespaces]]` binding, or null if absent/empty. */
export function getKvId(toml: string, env: string, binding: string): string | null {
  const block = envRecord(toml, env);
  const rows = isRecord(block) && Array.isArray(block.kv_namespaces) ? block.kv_namespaces : [];
  for (const row of rows) {
    if (isRecord(row) && row.binding === binding && typeof row.id === 'string') return row.id || null;
  }
  return null;
}

/** Rewrite the `id = ...` line (value + trailing comment) of the kv_namespaces table whose binding matches. Throws if absent. */
export function setKvId(toml: string, env: string, binding: string, id: string, title: string): string {
  const kv = arrayTableKv(toml, env, 'kv_namespaces', binding, 'id');
  if (!kv) throw new Error(`kv_namespaces binding "${binding}" not found under [env.${env}]`);
  return splice(toml, kv.value.range[0], endOfLine(toml, kv.value.range[1]), `${JSON.stringify(id)} # ${title}`);
}

/**
 * Append a copy of every `[env.<from>...]` table rewritten for `<to>` (headers
 * only — values copied verbatim). Throws if `<from>` is absent or `<to>` exists.
 */
export function cloneEnvBlock(toml: string, from: string, to: string): string {
  if (!hasEnvBlock(toml, from)) throw new Error(`source [env.${from}] not found`);
  if (hasEnvBlock(toml, to)) throw new Error(`target [env.${to}] already exists — blank/fill it instead`);
  const cloned = tables(toml)
    .filter((t) => t.resolvedKey[0] === 'env' && t.resolvedKey[1] === from)
    .map((t) => {
      const seg = t.key.keys[1].range; // the `<from>` segment of this table's header
      const text = toml.slice(t.range[0], t.range[1]);
      return text.slice(0, seg[0] - t.range[0]) + to + text.slice(seg[1] - t.range[0]);
    });
  return `${toml.replace(/\s*$/, '')}\n\n${cloned.join('\n\n')}\n`;
}

/** Blank the named `[env.<env>.vars]` values to `""` (preserving comments), skipping absent keys. */
export function blankEnvVars(toml: string, env: string, keys: string[]): string {
  const table = tables(toml).find((t) => sameKey(t.resolvedKey, ['env', env, 'vars']));
  if (!table) return toml;
  const edits = keys
    .map((key) => kvIn(toml, table, key))
    .flatMap((kv) => (kv ? [{ start: kv.value.range[0], end: kv.value.range[1] }] : []));
  return applyBlanks(toml, edits);
}

/** Blank every kv_namespaces `id` under the env to `""` (drops trailing comment; binding/name survive). */
export function blankKvIds(toml: string, env: string): string {
  return blankArrayField(toml, env, 'kv_namespaces', 'id');
}

/** The database_id written for `[[env.<env>.d1_databases]]` binding, or null if absent/empty. */
export function getD1Id(toml: string, env: string, binding: string): string | null {
  const block = envRecord(toml, env);
  const rows = isRecord(block) && Array.isArray(block.d1_databases) ? block.d1_databases : [];
  for (const row of rows) {
    if (isRecord(row) && row.binding === binding && typeof row.database_id === 'string') return row.database_id || null;
  }
  return null;
}

/** The `database_name` for `[[env.<env>.d1_databases]]` binding, or null. Source of truth for which DB to create/reuse. */
export function getD1Name(toml: string, env: string, binding: string): string | null {
  const block = envRecord(toml, env);
  const rows = isRecord(block) && Array.isArray(block.d1_databases) ? block.d1_databases : [];
  for (const row of rows) {
    if (isRecord(row) && row.binding === binding && typeof row.database_name === 'string') return row.database_name || null;
  }
  return null;
}

/** Rewrite the `database_id = ...` line (value + trailing comment) of the d1 table whose binding matches; the name line is untouched. Throws if absent. */
export function setD1Id(toml: string, env: string, binding: string, id: string, name: string): string {
  const kv = arrayTableKv(toml, env, 'd1_databases', binding, 'database_id');
  if (!kv) throw new Error(`d1_databases binding "${binding}" not found under [env.${env}]`);
  return splice(toml, kv.value.range[0], endOfLine(toml, kv.value.range[1]), `${JSON.stringify(id)} # ${name}`);
}

/** Blank every d1 `database_id` under the env to `""` (drops trailing comment; binding/name survive). */
export function blankD1Ids(toml: string, env: string): string {
  return blankArrayField(toml, env, 'd1_databases', 'database_id');
}

/** Blank one field across every `[[env.<env>.<arrayKey>]]` table (value..EOL -> `""`). */
function blankArrayField(toml: string, env: string, arrayKey: string, field: string): string {
  const edits = tables(toml)
    .filter((t) => t.kind === 'array' && t.resolvedKey[0] === 'env' && t.resolvedKey[1] === env && t.resolvedKey[2] === arrayKey)
    .map((t) => kvIn(toml, t, field))
    .flatMap((kv) => (kv ? [{ start: kv.value.range[0], end: endOfLine(toml, kv.value.range[1]) }] : []));
  return applyBlanks(toml, edits);
}

/** Apply `""` at each range, descending so earlier offsets stay valid. */
function applyBlanks(toml: string, edits: { start: number; end: number }[]): string {
  let out = toml;
  for (const e of [...edits].sort((a, b) => b.start - a.start)) out = splice(out, e.start, e.end, '""');
  return out;
}

/** Parse-guard: throws if `toml` no longer parses, so a bad edit can't be written to disk. */
export function assertToml(toml: string): void {
  parseTOML(toml);
}

/** Persist a toml string after confirming it still parses. */
export function saveToml(path: string, toml: string): void {
  assertToml(toml);
  writeFileSync(path, toml);
}

// ── manifest: what the worker needs, split into vars vs secrets ──────────────

export interface Manifest {
  /** KV binding names declared under `[[env.<env>.kv_namespaces]]` (resources to provision). */
  kvBindings: string[];
  /** D1 binding names declared under `[[env.<env>.d1_databases]]` (resources to provision). */
  d1Bindings: string[];
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
  const d1Rows = isRecord(block) && Array.isArray(block.d1_databases) ? block.d1_databases : [];
  const d1Bindings = d1Rows.flatMap((row) => (isRecord(row) && typeof row.binding === 'string' ? [row.binding] : []));
  const examplePath = join(root, '.dev.vars.example');
  const secrets = existsSync(examplePath) ? parseDevVarsExample(readFileSync(examplePath, 'utf8')) : [];
  return { kvBindings, d1Bindings, vars, secrets };
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

export interface D1Database {
  uuid: string;
  name: string;
}

function asD1Databases(json: unknown): D1Database[] {
  if (!Array.isArray(json)) return [];
  const out: D1Database[] = [];
  for (const row of json) {
    if (isRecord(row) && typeof row.uuid === 'string' && typeof row.name === 'string') {
      out.push({ uuid: row.uuid, name: row.name });
    }
  }
  return out;
}

/** Live D1 databases on the account (`d1 list --json`); `[]` when offline/unauthenticated. */
export async function listD1Databases(cwd: string): Promise<D1Database[]> {
  try {
    return asD1Databases(await $`bunx wrangler d1 list --json`.cwd(cwd).quiet().json());
  } catch {
    return [];
  }
}

/**
 * Reuse the D1 database whose name matches, else create it. The uuid comes from
 * the structured `list` JSON (create prints freeform text), so we re-list rather
 * than scrape. Returns the uuid, or null on failure.
 */
export async function ensureD1Database(name: string, existing: D1Database[], cwd: string): Promise<string | null> {
  const match = existing.find((d) => d.name === name);
  if (match) return match.uuid;
  try {
    await $`bunx wrangler d1 create ${name}`.cwd(cwd).quiet();
  } catch (err) {
    return errText(err).includes('already') ? reListD1(name, cwd) : null;
  }
  return reListD1(name, cwd);
}

async function reListD1(name: string, cwd: string): Promise<string | null> {
  return (await listD1Databases(cwd)).find((d) => d.name === name)?.uuid ?? null;
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
