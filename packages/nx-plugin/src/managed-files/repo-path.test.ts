import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir, userInfo } from 'node:os';
import { dirname, join } from 'node:path';
import fc from 'fast-check';
import { managedAssetsRoot } from '../managed-assets.js';

interface RepoPathFixture {
  readonly root: string;
  readonly helper: string;
  readonly home: string;
  readonly venv: string;
  readonly installed: string;
  readonly tools: string;
  readonly entries: string[];
}

async function executable(path: string, marker: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `#!/bin/sh\nprintf '%s\\n' '${marker}'\n`, { mode: 0o755 });
}

async function withRepoPath(run: (fixture: RepoPathFixture) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'smoo-repo-path-'));
  try {
    const helper = join(root, 'tooling/direnv/repo-path');
    await mkdir(dirname(helper), { recursive: true });
    await writeFile(helper, await readFile(join(managedAssetsRoot, 'raw/tooling/direnv/repo-path')), { mode: 0o755 });
    const tools = join(root, 'system-bin');
    await mkdir(tools);
    // Resolve the script's tools before scrubbing PATH; /usr/bin:/bin is not
    // a coreutils installation on NixOS.
    for (const tool of ['bash', 'dirname']) {
      const resolved = Bun.which(tool);
      if (resolved === null) throw new Error(`${tool} must be on PATH`);
      await symlink(resolved, join(tools, tool));
    }
    const home = join(root, 'private home');
    const venv = join(root, 'active venv');
    const installed = join(home, 'Library/Application Support/dev.cowshed/bin');
    const entries = [
      join(root, 'tooling/direnv/.devenv/profile/bin'),
      join(root, 'tooling/git-hooks'),
      join(root, 'tooling'),
      join(root, 'tooling/node_modules/.bin'),
      join(root, 'node_modules/.bin'),
    ];
    await executable(join(entries[0], 'python3'), 'profile-python');
    await executable(join(entries[0], 'cargo'), 'profile-cargo');
    await executable(join(venv, 'bin/python3'), 'venv-python');
    await executable(join(venv, 'bin/cargo'), 'venv-cargo');
    await executable(join(installed, 'cargo'), 'installed-cargo');
    await run({ root, helper, home, venv, installed, tools, entries });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function helper(fixture: RepoPathFixture, args: readonly string[], virtualEnv?: string, path = fixture.tools) {
  const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper, ...args], {
    encoding: 'utf8',
    env: { HOME: fixture.home, PATH: path, ...(virtualEnv === undefined ? {} : { VIRTUAL_ENV: virtualEnv }) },
  });
  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  return result.stdout;
}

async function expectModes(fixture: RepoPathFixture, entries: string[], virtualEnv?: string): Promise<void> {
  expect(helper(fixture, ['--print'], virtualEnv)).toBe(`${entries.join('\n')}\n`);
  const githubPath = join(fixture.root, 'github-path');
  await writeFile(githubPath, 'existing-entry\n');
  const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper, '--github-path'], {
    encoding: 'utf8',
    env: {
      HOME: fixture.home,
      PATH: fixture.tools,
      GITHUB_PATH: githubPath,
      ...(virtualEnv === undefined ? {} : { VIRTUAL_ENV: virtualEnv }),
    },
  });
  expect(result.status).toBe(0);
  expect(result.stderr).toBe('');
  expect(await readFile(githubPath, 'utf8')).toBe(`existing-entry\n${[...entries].reverse().join('\n')}\n`);
}

describe('managed repo-path', () => {
  it.each([undefined, ''])('preserves the exact legacy ordering without an active venv (%p)', async (virtualEnv) => {
    await withRepoPath(async (fixture) => {
      await expectModes(fixture, fixture.entries, virtualEnv);
      expect(helper(fixture, [], virtualEnv)).toBe(
        `${[fixture.installed, ...fixture.entries].join(':')}:${fixture.tools}\n`,
      );
    });
  });

  it.each([false, true])(
    'keeps active Python ahead of the profile and installed Cargo first (served=%p)',
    async (served) => {
      await withRepoPath(async (fixture) => {
        if (served) {
          await mkdir(join(fixture.root, '.cowshed'), { recursive: true });
          await writeFile(
            join(fixture.root, '.cowshed/env'),
            `export COWSHED_RUNTIME_LINK=fixture\nexport PATH=${JSON.stringify(fixture.installed)}:$PATH\n`,
          );
        }
        const entries = [join(fixture.venv, 'bin'), ...fixture.entries];
        await expectModes(fixture, entries, fixture.venv);
        const incoming = `${join(fixture.venv, 'bin')}:${fixture.tools}`;
        const path = helper(fixture, [], fixture.venv, incoming).trimEnd();
        expect(path).toBe(`${[fixture.installed, ...entries].join(':')}:${incoming}`);
        const selected = spawnSync(
          join(fixture.tools, 'bash'),
          ['-c', 'command -v python3; python3; command -v cargo; cargo'],
          {
            encoding: 'utf8',
            env: { HOME: fixture.home, PATH: path },
          },
        );
        expect(selected.status).toBe(0);
        expect(selected.stderr).toBe('');
        expect(selected.stdout).toBe(
          `${join(fixture.venv, 'bin/python3')}\nvenv-python\n${join(fixture.installed, 'cargo')}\ninstalled-cargo\n`,
        );
      });
    },
  );

  it("names this checkout's own shell's evaluated profile, never the checkout's re-pointable link", async () => {
    await withRepoPath(async (fixture) => {
      // The link is what any activation of the checkout last pointed it at; the shell evaluated another profile.
      const evaluated = join(fixture.root, 'evaluated-profile');
      await executable(join(evaluated, 'bin/bun'), 'evaluated-bun');
      await executable(join(fixture.entries[0], 'bun'), 'link-bun');
      const run = (args: readonly string[], devenvRoot: string) => {
        const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper, ...args], {
          encoding: 'utf8',
          env: { HOME: fixture.home, PATH: fixture.tools, DEVENV_ROOT: devenvRoot, DEVENV_PROFILE: evaluated },
        });
        expect(result.stderr).toBe('');
        expect(result.status).toBe(0);
        return result.stdout;
      };
      const own = join(fixture.root, 'tooling/direnv');
      expect(run(['--print'], own).split('\n')[0]).toBe(join(evaluated, 'bin'));
      // Another checkout's shell, inherited by a hook run here, does not name this checkout's profile.
      expect(run(['--print'], join(fixture.root, 'system-bin')).split('\n')[0]).toBe(fixture.entries[0]);

      const selected = spawnSync(join(fixture.tools, 'bash'), ['-c', 'bun'], {
        encoding: 'utf8',
        env: { HOME: fixture.home, PATH: run([], own).trimEnd() },
      });
      expect(selected.stderr).toBe('');
      expect(selected.stdout).toBe('evaluated-bun\n');
    });
  });

  it("reaches the installed cowshed through the real uid's passwd home when the caller passes only PATH", async () => {
    await withRepoPath(async (fixture) => {
      // A formatter or hook that passes { PATH } alone leaves HOME unset; it is not the helper's to require.
      const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper], {
        encoding: 'utf8',
        env: { PATH: fixture.tools },
      });
      expect(result.stderr).toBe('');
      expect(result.status).toBe(0);
      const installed = join(userInfo().homedir, 'Library/Application Support/dev.cowshed/bin');
      expect(result.stdout).toBe(`${[installed, ...fixture.entries].join(':')}:${fixture.tools}\n`);
    });
  });
});

// A fork interrupted before its supervisor first started keeps the `.cowshed/env` its mint wrote: no TMPDIR and no
// runtime link, and a build link that may still name its source's volume. Entered from a shell bound to that source,
// every inherited value is the source's. The env files, inherited values, workspace names and what the serve does are
// generated; whether to serve, and whether to enter after it, is repo-path's alone, read from the file and never from
// the caller.
describe('repo-path in a cowshed workspace', () => {
  const absolute = fc.stringMatching(/^\/[a-z]{1,6}(\/[a-z]{1,6}){0,2}$/);
  const value = fc.oneof(absolute, fc.stringMatching(/^[a-z]{1,6}$/), fc.constant(''));
  /** One line of a `.cowshed/env`, and what it leaves TMPDIR or the runtime link as: `null` unsets it. */
  interface EnvLine {
    readonly text: string;
    readonly tmp?: string | null;
    readonly link?: string | null;
  }
  const line: fc.Arbitrary<EnvLine> = fc.oneof(
    value.map((tmp) => ({ text: `export TMPDIR='${tmp}'`, tmp })),
    value.map((link) => ({ text: `export COWSHED_RUNTIME_LINK=${link}`, link })),
    fc.constant({ text: 'unset TMPDIR', tmp: null }),
    fc.constant({ text: 'unset COWSHED_RUNTIME_LINK', link: null }),
    fc.constant({ text: 'export COWSHED_PORT_BASE=37376\nexport COWSHED_PORT_BLOCK_SIZE=64' }),
    fc.constant({ text: 'if [ "${GITHUB_ACTIONS:-}" != true ]; then unset CI; fi' }),
  );
  /** What `cowshed exec <ws> -- true` does: write the file it serves, or fail with a status and its output. */
  type Serve = { readonly served: readonly EnvLine[] } | { readonly status: number; readonly error: string };
  const serve: fc.Arbitrary<Serve> = fc.oneof(
    fc.array(line, { maxLength: 6 }).map((served) => ({ served })),
    fc.record({ status: fc.integer({ min: 1, max: 125 }), error: fc.stringMatching(/^cowshed: [a-z][a-z ]{0,24}$/) }),
  );
  const scenario = fc.record({
    workspace: fc.stringMatching(/^[a-z][a-z0-9-]{0,10}$/),
    file: fc.option(fc.array(line, { maxLength: 6 }), { nil: undefined }),
    inheritedTmp: absolute,
    inheritedLink: absolute,
    serve,
  });

  /** The entries a file leaves unset or relative, in the order repo-path names them. */
  function unserved(file: readonly EnvLine[] | undefined): string[] {
    let tmp: string | null = null;
    let link: string | null = null;
    for (const entry of file ?? []) {
      if (entry.tmp !== undefined) tmp = entry.tmp;
      if (entry.link !== undefined) link = entry.link;
    }
    return [...(tmp?.startsWith('/') ? [] : ['TMPDIR']), ...(link?.startsWith('/') ? [] : ['COWSHED_RUNTIME_LINK'])];
  }

  const quoted = (text: string) => `'${text.replaceAll("'", "'\\''")}'`;

  /**
   * The installed cowshed's stable name, standing in for the supervisor start `cowshed exec` makes: it records where
   * it ran, its arguments, the TMPDIR and runtime link it was given and its stdin, then does what `serve` says.
   */
  async function installCowshed(fixture: RepoPathFixture, log: string, outcome: Serve): Promise<void> {
    const effect =
      'served' in outcome
        ? `printf '%s' ${quoted(outcome.served.map(({ text }) => `${text}\n`).join(''))} > .cowshed/env`
        : `printf '%s\\n' ${quoted(outcome.error)} >&2\nexit ${outcome.status}`;
    await mkdir(fixture.installed, { recursive: true });
    await writeFile(
      join(fixture.installed, 'cowshed'),
      `#!${join(fixture.tools, 'bash')}
printf '%s\\n' "$PWD" "$*" "\${TMPDIR-unset}" "\${COWSHED_RUNTIME_LINK-unset}" >> ${quoted(log)}
while IFS= read -r line; do printf 'stdin %s\\n' "$line" >> ${quoted(log)}; done
${effect}
`,
      { mode: 0o755 },
    );
  }

  it('serves it once through the installed cowshed and enters, refusing only when that serve fails or leaves it unserved', async () => {
    await withRepoPath(async (fixture) => {
      mkdirSync(join(fixture.root, '.cowshed'));
      const env = join(fixture.root, '.cowshed/env');
      const log = join(fixture.root, 'serve.log');
      // The checkout's own launcher runs whatever the checkout last built (06_cli.md, "Launcher"); never this one.
      await executable(join(fixture.root, 'node_modules/.bin/cowshed'), 'launcher');
      await fc.assert(
        fc.asyncProperty(scenario, async ({ workspace, file, inheritedTmp, inheritedLink, serve }) => {
          writeFileSync(
            join(fixture.root, '.cowshed/workspace.json'),
            `${JSON.stringify({ version: 1, workspace, role: 'workspace' }, null, 2)}\n`,
          );
          if (file === undefined) rmSync(env, { force: true });
          else writeFileSync(env, file.map(({ text }) => `${text}\n`).join(''));
          rmSync(log, { force: true });
          await installCowshed(fixture, log, serve);
          const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper], {
            encoding: 'utf8',
            // pre-push reads its refs from the stdin it hands repo-path.
            input: 'refs/heads/x 0000 refs/heads/x 0000\n',
            env: { HOME: fixture.home, PATH: fixture.tools, TMPDIR: inheritedTmp, COWSHED_RUNTIME_LINK: inheritedLink },
          });
          const entered = `${fixture.entries.join(':')}:${fixture.tools}\n`;
          const missing = unserved(file);
          if (missing.length === 0) {
            expect(existsSync(log)).toBe(false);
            expect(result.stderr).toBe('');
            expect(result.status).toBe(0);
            expect(result.stdout).toBe(entered);
            return;
          }
          // Once, from the checkout, with neither the caller's TMPDIR and runtime link nor the file's, and no stdin.
          expect(readFileSync(log, 'utf8')).toBe(`${fixture.root}\nexec ${workspace} -- true\nunset\nunset\n`);
          const serving = `repo-path: cowshed workspace ${workspace} at ${fixture.root} is not served: its .cowshed/env sets no ${missing.join(', ')}. Serving it: cowshed exec ${workspace} -- true\n`;
          if (!('served' in serve)) {
            expect(result.status).toBe(1);
            expect(result.stdout).toBe('');
            expect(result.stderr).toStartWith(
              `${serving}repo-path: serving cowshed workspace ${workspace} at ${fixture.root} failed: cowshed exec ${workspace} -- true exited ${serve.status}: ${serve.error}\n`,
            );
            expect(result.stderr).toEndWith(`\nnext: cowshed exec ${workspace} -- true\n`);
            return;
          }
          const still = unserved(serve.served);
          if (still.length === 0) {
            expect(result.stderr).toBe(serving);
            expect(result.status).toBe(0);
            expect(result.stdout).toBe(entered);
          } else {
            expect(result.stderr).toBe(
              `${serving}repo-path: cowshed exec ${workspace} -- true succeeded, but cowshed workspace ${workspace} at ${fixture.root} is still not served: its .cowshed/env sets no ${still.join(', ')}.\n`,
            );
            expect(result.status).toBe(1);
            expect(result.stdout).toBe('');
          }
        }),
        { numRuns: 64 },
      );
    });
  });

  it('refuses a workspace whose workspace.json names none to serve, serving nothing', async () => {
    await withRepoPath(async (fixture) => {
      mkdirSync(join(fixture.root, '.cowshed'));
      writeFileSync(join(fixture.root, '.cowshed/workspace.json'), '{"version": 1, "role": "workspace"}\n');
      const log = join(fixture.root, 'serve.log');
      await installCowshed(fixture, log, { served: [] });
      const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper], {
        encoding: 'utf8',
        env: { HOME: fixture.home, PATH: fixture.tools },
      });
      expect(result.stderr).toBe(
        `repo-path: cowshed workspace at ${fixture.root} is not served: its .cowshed/env sets no TMPDIR, COWSHED_RUNTIME_LINK, and its .cowshed/workspace.json names no workspace to serve.\n`,
      );
      expect(result.status).toBe(1);
      expect(result.stdout).toBe('');
      expect(existsSync(log)).toBe(false);
    });
  });

  it("refuses, naming the missing installed cowshed, and never runs the checkout's own launcher", async () => {
    await withRepoPath(async (fixture) => {
      mkdirSync(join(fixture.root, '.cowshed'));
      writeFileSync(
        join(fixture.root, '.cowshed/workspace.json'),
        `${JSON.stringify({ version: 1, workspace: 'ws', role: 'workspace' }, null, 2)}\n`,
      );
      // The checkout's launcher is on the PATH repo-path writes. Were it run, it would serve the file and leave a mark.
      const ran = join(fixture.root, 'launcher-ran');
      const launcher = join(fixture.root, 'node_modules/.bin/cowshed');
      await mkdir(dirname(launcher), { recursive: true });
      await writeFile(
        launcher,
        `#!${join(fixture.tools, 'bash')}\n: > ${quoted(ran)}\nprintf '%s\\n' 'export TMPDIR=/t' 'export COWSHED_RUNTIME_LINK=/l' > .cowshed/env\n`,
        { mode: 0o755 },
      );
      const result = spawnSync(join(fixture.tools, 'bash'), [fixture.helper], {
        encoding: 'utf8',
        env: { HOME: fixture.home, PATH: fixture.tools },
      });
      expect(result.status).toBe(1);
      expect(result.stdout).toBe('');
      expect(result.stderr).toContain(
        `repo-path: serving cowshed workspace ws at ${fixture.root} failed: cowshed exec ws -- true exited 127: `,
      );
      expect(result.stderr).toContain(`${join(fixture.installed, 'cowshed')}: No such file or directory`);
      expect(result.stderr).toEndWith('\nnext: cowshed exec ws -- true\n');
      expect(existsSync(ran)).toBe(false);
    });
  });
});
