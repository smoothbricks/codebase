import { afterEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { mergeEnv, runResult } from '../lib/run.js';
import { validateGitHubActions } from './github-actions.js';
import { packsForTest, runValidatePacks } from './packs/index.js';

const workflow = `name: Validate
on: push
permissions: {}
concurrency:
  group: validate-\${{ github.ref }}
  cancel-in-progress: true
jobs:
  check:
    runs-on: ubuntu-24.04
    timeout-minutes: 5
    steps:
      - run: printf '%s\\n' valid
`;
const action = `name: Fixture
description: A local composite action
runs:
  using: composite
  steps:
    - shell: bash
      run: printf '%s\\n' valid
`;
const dependabot = `version: 2
updates:
  - package-ecosystem: npm
    directory: /
    cooldown:
      default-days: 7
    schedule:
      interval: weekly
`;
const roots: string[] = [];
const originalPath = process.env.PATH;
const originalXdgConfigHome = process.env.XDG_CONFIG_HOME;

afterEach(async () => {
  if (originalPath === undefined) delete process.env.PATH;
  else process.env.PATH = originalPath;
  if (originalXdgConfigHome === undefined) delete process.env.XDG_CONFIG_HOME;
  else process.env.XDG_CONFIG_HOME = originalXdgConfigHome;
  mock.restore();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

async function fixture(files: Record<string, string>): Promise<string> {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'smoo-jactionlint-')));
  roots.push(root);
  const init = Bun.spawnSync(['git', 'init', '--quiet'], {
    cwd: root,
    env: mergeEnv(undefined, ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR']),
  });
  expect(init.exitCode).toBe(0);
  for (const [path, content] of Object.entries(files)) {
    await mkdir(dirname(join(root, path)), { recursive: true });
    await writeFile(join(root, path), content);
  }
  return root;
}

describe('GitHub Actions validation with the real jactionlint executable', () => {
  it('accepts a real valid workflow without changing its bytes', async () => {
    const root = await fixture({ '.github/workflows/validate.yml': workflow });
    const direct = await runResult('jactionlint', ['--no-online', '--config-file', '/dev/null'], root);
    expect(direct.exitCode).toBe(0);
    expect(await validateGitHubActions(root)).toBe(0);
    expect(await readFile(join(root, '.github/workflows/validate.yml'), 'utf8')).toBe(workflow);
  });

  it.each([
    ['expression', workflow.replace("printf '%s\\n' valid", `printf '%s\\n' '\${{ github. }}'`)],
    ['syntax', workflow.replace('runs-on:', 'runs-on-typo:')],
  ])('rejects real workflow %s errors and retains checker diagnostics', async (_name, invalid) => {
    const root = await fixture({ '.github/workflows/validate.yml': invalid });
    const direct = await runResult('jactionlint', ['--no-online', '--config-file', '/dev/null'], root);
    expect(direct.exitCode).toBe(1);
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    expect(logs.mock.calls.flat().join('\n')).toContain('.github/workflows/validate.yml:');
    expect(errors.mock.calls.flat().join('\n')).toContain('exit code 1');
    expect(await readFile(join(root, '.github/workflows/validate.yml'), 'utf8')).toBe(invalid);
  });

  it('keeps default security checks and honors an explicit repository profile', async () => {
    const root = await fixture({
      '.github/workflows/validate.yml': workflow.replace(
        "      - run: printf '%s\\n' valid",
        '      - uses: actions/checkout@v4',
      ),
    });
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    expect(logs.mock.calls.flat().join('\n')).toContain('[unpinned-uses]');
    await writeFile(join(root, '.github/jactionlint.yaml'), 'profile: correctness\nonline: true\n');
    expect(await validateGitHubActions(root)).toBe(0);
  });

  it('ignores a hostile user-global jactionlint config and honors explicit repository config', async () => {
    const root = await fixture({
      '.github/workflows/validate.yml': workflow.replace(
        "      - run: printf '%s\\n' valid",
        '      - uses: actions/checkout@v4',
      ),
      'user-config/jactionlint/jactionlint.yaml': 'profile: correctness\n',
    });
    process.env.XDG_CONFIG_HOME = join(root, 'user-config');
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    expect(logs.mock.calls.flat().join('\n')).toContain('[unpinned-uses]');
    await writeFile(join(root, '.github/jactionlint.yaml'), 'profile: correctness\n');
    expect(await validateGitHubActions(root)).toBe(0);
  });

  it('reports real local shellcheck findings for unquoted run expansions', async () => {
    const root = await fixture({
      '.github/workflows/validate.yml': workflow.replace(
        "      - run: printf '%s\\n' valid",
        '      - run: echo $UNSET',
      ),
    });
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    expect([...logs.mock.calls, ...errors.mock.calls].flat().join('\n')).toContain('SC2086');
  });

  it('reports real local pyflakes findings for undefined names', async () => {
    const pythonStep = '      - shell: python\n        run: |\n          print(undefined_name)';
    const root = await fixture({
      '.github/workflows/validate.yml': workflow.replace("      - run: printf '%s\\n' valid", pythonStep),
    });
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    const output = [...logs.mock.calls, ...errors.mock.calls].flat().join('\n');
    expect(output).toContain('[pyflakes]');
    expect(output).toContain('undefined_name');
  });

  it.each(['.github/actions/fixture/action.yml', 'automation/fixture/action.yaml', 'action.yml'])(
    'checks valid and invalid composite action metadata at %s without a workflow',
    async (path) => {
      const root = await fixture({ [path]: action });
      expect(await validateGitHubActions(root)).toBe(0);
      const invalid = action.replace('shell: bash', 'shell-typo: bash');
      await writeFile(join(root, path), invalid);
      const logs = spyOn(console, 'log').mockImplementation(() => {});
      spyOn(console, 'error').mockImplementation(() => {});
      expect(await validateGitHubActions(root)).toBe(1);
      expect(logs.mock.calls.flat().join('\n')).toContain(path);
      expect(await readFile(join(root, path), 'utf8')).toBe(invalid);
    },
  );

  it('checks standalone Dependabot inputs and uses their repository configuration', async () => {
    const root = await fixture({ '.github/dependabot.yaml': dependabot });
    expect(await validateGitHubActions(root)).toBe(0);
    await writeFile(join(root, '.github/dependabot.yaml'), dependabot.replace('version: 2', 'version: 9'));
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(root)).toBe(1);
    expect(logs.mock.calls.flat().join('\n')).toContain('[dependabot-syntax]');
    await writeFile(join(root, '.github/dependabot.yaml'), dependabot);
    await writeFile(join(root, '.github/jactionlint.yaml'), 'profile: invalid-profile\n');
    expect(await validateGitHubActions(root)).toBe(1);
  });

  it('skips only absent applicable inputs, with a verbose reason', async () => {
    const root = await fixture({
      '.github/workflows/nested/not-a-workflow.yml': 'not a workflow: [',
      'node_modules/dependency/action.yml': 'not an action: [',
      'fixtures/action.yaml': 'not an action: [',
    });
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    process.env.PATH = root;
    expect(await validateGitHubActions(root, true)).toBe(0);
    expect(logs.mock.calls.flat().join('\n')).toContain('no workflows, actions, or Dependabot configuration');
  });

  it('does not turn an unreadable workflow directory or missing executable into a pass', async () => {
    const malformed = await fixture({ '.github/workflows': 'not a directory' });
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    expect(await validateGitHubActions(malformed)).toBe(1);
    expect(errors.mock.calls.flat().join('\n')).toContain('input discovery failed');
    const root = await fixture({ '.github/workflows/validate.yml': workflow });
    const shellcheck = Bun.which('shellcheck');
    const pyflakes = Bun.which('pyflakes');
    if (!shellcheck) throw new Error('test prerequisite shellcheck is missing from PATH');
    if (!pyflakes) throw new Error('test prerequisite pyflakes is missing from PATH');
    await symlink(shellcheck, join(root, 'shellcheck'));
    await symlink(pyflakes, join(root, 'pyflakes'));
    process.env.PATH = root;
    expect(await validateGitHubActions(root)).toBe(1);
    expect(errors.mock.calls.flat().join('\n')).toContain('requires jactionlint on the managed shell/CI PATH');
  });

  it.each(['shellcheck', 'pyflakes'])(
    'fails when required helper %s is missing from the managed PATH',
    async (missing) => {
      const other = missing === 'shellcheck' ? 'pyflakes' : 'shellcheck';
      const root = await fixture({ '.github/workflows/validate.yml': workflow });
      const jactionlint = Bun.which('jactionlint');
      const otherPath = Bun.which(other);
      if (!jactionlint) throw new Error('test prerequisite jactionlint is missing from PATH');
      if (!otherPath) throw new Error(`test prerequisite ${other} is missing from PATH`);
      await symlink(jactionlint, join(root, 'jactionlint'));
      await symlink(otherPath, join(root, other));
      process.env.PATH = root;
      const errors = spyOn(console, 'error').mockImplementation(() => {});
      expect(await validateGitHubActions(root)).toBe(1);
      const output = errors.mock.calls.flat().join('\n');
      expect(output).toContain(`requires ${missing}`);
      expect(output).toContain('managed shell/CI PATH');
    },
  );

  it('runs the production pack before builds, remains repository-wide, and counts fail-fast failures', async () => {
    const invalid = workflow.replace('runs-on:', 'runs-on-typo:');
    const root = await fixture({ '.github/workflows/validate.yml': invalid });
    const pack = packsForTest.find((candidate) => candidate.name === 'github-actions');
    if (!pack) throw new Error('production github-actions pack is missing');
    let builds = 0;
    const hooks = {
      packs: [pack],
      runBuild() {
        builds++;
        return 0;
      },
    };
    const ctx = { root, syncRuntime: false, projects: ['unrelated-package'] };
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    spyOn(console, 'error').mockImplementation(() => {});
    expect(await runValidatePacks(ctx, { failFast: true }, hooks)).toEqual({ failures: 1, failedChecks: 1 });
    expect(builds).toBe(0);
    expect(logs.mock.calls.flat().join('\n')).toContain('.github/workflows/validate.yml:');
    expect(await runValidatePacks(ctx, { fix: true }, hooks)).toEqual({ failures: 1, failedChecks: 1 });
    expect(builds).toBe(1);
    expect(await readFile(join(root, '.github/workflows/validate.yml'), 'utf8')).toBe(invalid);
    await writeFile(join(root, '.github/workflows/validate.yml'), workflow);
    expect(await runValidatePacks(ctx, {}, hooks)).toEqual({ failures: 0, failedChecks: 0 });
    expect(builds).toBe(2);
  });
});
