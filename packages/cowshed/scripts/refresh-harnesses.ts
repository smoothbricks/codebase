/**
 * Regenerate the agent-harness snapshot in
 * `crates/cowshed-cli/src/skill/generated.rs` from vercel-labs/skills.
 *
 * Upstream keeps the harness table as executable TypeScript whose paths are
 * built from `homedir()` and XDG bases, and whose detection is an arbitrary
 * async function. Rather than execute it — which would bind the snapshot to the
 * generating machine's environment — this reads the source and accepts only
 * entries whose skills directory and detection probe both reduce to a literal
 * path under the home directory. Anything else is skipped and reported, so an
 * unparseable entry is visible at refresh time instead of silently wrong.
 *
 * Run with: `nx run cowshed:refresh-harnesses`
 */

const UPSTREAM_REPO = 'vercel-labs/skills';
const UPSTREAM_PATH = 'src/agents.ts';
const OUTPUT = new URL('../crates/cowshed-cli/src/skill/generated.rs', import.meta.url).pathname;

/** Upstream path bases, as offsets below the home directory. */
const BASES: Record<string, string> = {
  home: '',
  configHome: '.config',
  codexHome: '.codex',
  claudeHome: '.claude',
  vibeHome: '.vibe',
  hermesHome: '.hermes',
  autohandHome: '.autohand',
  grokHome: '.grok',
};

interface Entry {
  name: string;
  globalRoot: string;
  globalSkills: string;
  projectSkills: string;
}

function joinHomeRelative(base: string, rest?: string): string | undefined {
  if (!(base in BASES)) return undefined;
  const prefix = BASES[base];
  const path = rest === undefined ? prefix : prefix ? `${prefix}/${rest}` : rest;
  // A snapshot entry must stay under the home directory: a traversal or an
  // absolute path would install outside the base the installer was given.
  if (!path || path.startsWith('/') || path.split('/').includes('..')) return undefined;
  return path;
}

/** `join(base, 'a', 'b', …)` or a bare base identifier. */
function resolvePathExpression(expression: string): string | undefined {
  const joined = expression.match(/^join\(\s*([A-Za-z]\w*)\s*((?:,\s*'[^']*'\s*)+)\)$/);
  if (joined) {
    const segments = [...joined[2].matchAll(/'([^']*)'/g)].map((match) => match[1]);
    return joinHomeRelative(joined[1], segments.join('/'));
  }
  const bare = expression.match(/^([A-Za-z]\w*)$/);
  if (bare) return joinHomeRelative(bare[1]);
  return undefined;
}

/**
 * Every `existsSync(…)` argument in source order.
 *
 * The pattern is lazy and admits at most one nested call so that
 * `existsSync(a) || existsSync(b)` yields two arguments rather than one run-on
 * match spanning both calls.
 */
function existsSyncArguments(source: string): string[] {
  return [...source.matchAll(/existsSync\(\s*((?:[^()]|\([^()]*\))*?)\s*\)/g)].map((match) => match[1]);
}

/** Split the `agents` object literal into one source block per entry. */
function splitEntries(source: string): Map<string, string> {
  const start = source.indexOf('export const agents');
  if (start < 0) throw new Error('upstream no longer exports `agents`');
  const open = source.indexOf('{', start);
  const blocks = new Map<string, string>();
  let index = open + 1;
  let depth = 1;

  while (index < source.length && depth > 0) {
    const header = source.slice(index).match(/^\s*'?([\w.@/-]+)'?:\s*\{/);
    if (depth === 1 && header) {
      const bodyStart = index + header[0].length;
      let cursor = bodyStart;
      let nested = 1;
      while (cursor < source.length && nested > 0) {
        const character = source[cursor];
        if (character === '{') nested += 1;
        else if (character === '}') nested -= 1;
        cursor += 1;
      }
      blocks.set(header[1], source.slice(bodyStart, cursor - 1));
      index = cursor;
      continue;
    }
    const character = source[index];
    if (character === '{') depth += 1;
    else if (character === '}') depth -= 1;
    index += 1;
  }
  return blocks;
}

function parseEntry(key: string, body: string): Entry | string {
  const name = body.match(/\bname:\s*'([^']+)'/)?.[1] ?? key;

  const projectSkills = body.match(/\bskillsDir:\s*'([^']+)'/)?.[1];
  if (!projectSkills) return 'no literal skillsDir';
  if (projectSkills.startsWith('/') || projectSkills.split('/').includes('..')) {
    return `project skills path escapes the repository (${projectSkills})`;
  }

  const globalExpression = body.match(/\bglobalSkillsDir:\s*([^\n]+?),?\s*$/m)?.[1];
  if (!globalExpression) return 'no globalSkillsDir';
  const globalSkills = resolvePathExpression(globalExpression.trim().replace(/,$/, ''));
  if (!globalSkills) return `globalSkillsDir is not a literal home path (${globalExpression.trim()})`;

  // Detection may test several candidates; take the first that reduces to a
  // home-relative literal and ignore absolute system paths such as /etc/codex.
  const detect = body.slice(body.indexOf('detectInstalled'));
  let globalRoot: string | undefined;
  for (const argument of existsSyncArguments(detect)) {
    const candidate = resolvePathExpression(argument.trim());
    if (candidate) {
      globalRoot = candidate;
      break;
    }
  }
  if (!globalRoot) return 'detectInstalled has no literal home-relative probe';

  return { name, globalRoot, globalSkills, projectSkills };
}

function rustString(value: string): string {
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

const revision =
  Bun.argv[2] ??
  (
    (await (await fetch(`https://api.github.com/repos/${UPSTREAM_REPO}/commits/main?per_page=1`)).json()) as {
      sha: string;
    }
  ).sha;

const sourceUrl = `https://raw.githubusercontent.com/${UPSTREAM_REPO}/${revision}/${UPSTREAM_PATH}`;
const response = await fetch(sourceUrl);
if (!response.ok) throw new Error(`fetch ${sourceUrl}: ${response.status}`);
const source = await response.text();

const entries: Entry[] = [];
const skipped: Array<[string, string]> = [];
for (const [key, body] of splitEntries(source)) {
  const parsed = parseEntry(key, body);
  if (typeof parsed === 'string') skipped.push([key, parsed]);
  else entries.push(parsed);
}
entries.sort((left, right) => left.name.localeCompare(right.name));
skipped.sort(([left], [right]) => left.localeCompare(right));

const lines = [
  '//! Agent-harness skill directories, generated from an upstream snapshot.',
  '//!',
  '//! DO NOT EDIT. Regenerate with `nx run cowshed:refresh-harnesses`.',
  '//!',
  `//! Upstream:  https://github.com/${UPSTREAM_REPO}`,
  `//! Source:    ${UPSTREAM_PATH}`,
  `//! Revision:  ${revision}`,
  '//!',
  '//! Paths are relative to the install base: the home directory for the global',
  '//! scope, the repository root for the project scope. `configHome` is resolved',
  '//! as `.config`, its default when XDG_CONFIG_HOME is unset.',
  '//!',
  '//! Entries whose paths or detection probe do not reduce to a literal home',
  `//! path are skipped rather than guessed (${skipped.length} of ${entries.length + skipped.length}):`,
  ...skipped.map(([key, reason]) => `//!   - ${key}: ${reason}`),
  '',
  'use super::HarnessEntry;',
  '',
  '/// The upstream snapshot. Entries in `VERIFIED_HARNESSES` override these by name.',
  'pub const GENERATED_HARNESSES: &[HarnessEntry] = &[',
  ...entries.flatMap((entry) => [
    '    HarnessEntry {',
    `        name: ${rustString(entry.name)},`,
    `        global_root: ${rustString(entry.globalRoot)},`,
    `        global_skills: ${rustString(entry.globalSkills)},`,
    `        project_skills: ${rustString(entry.projectSkills)},`,
    '    },',
  ]),
  '];',
  '',
];

await Bun.write(OUTPUT, lines.join('\n'));
console.error(`cowshed: wrote ${entries.length} harnesses from ${UPSTREAM_REPO}@${revision.slice(0, 12)}`);
for (const [key, reason] of skipped) console.error(`cowshed: skipped ${key} — ${reason}`);
