import { describe, expect, it } from 'bun:test';
import { spawnSync } from 'node:child_process';
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { connect, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { managedAssetsRoot } from '../managed-assets.js';
import { ownedSocketDirectory } from '../testing.js';

/** The managed files a commit in the fork runs before anything of its own: its hooks and their PATH writer. */
const HOOK_FILES = [
  'tooling/direnv/repo-path',
  'tooling/direnv/ttsc-env.sh',
  'tooling/git-hooks/pre-commit.sh',
  'tooling/git-hooks/commit-msg.sh',
];

function tool(name: string): string {
  const resolved = Bun.which(name);
  if (resolved === null) throw new Error(`${name} must be on PATH`);
  return resolved;
}

function git(cwd: string, args: readonly string[], env: Record<string, string>) {
  return spawnSync('git', args, { cwd, encoding: 'utf8', env });
}

function connects(socket: string): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  const client = connect(socket, () => {
    client.end();
    resolve();
  });
  client.on('error', reject);
  return promise;
}

// The defect this guards: a `cowshed fork` interrupted before its first supervisor start left a shed whose
// `.cowshed/env` was the mint's (no TMPDIR, no runtime link) and whose `.cowshed/build` still named main's build
// volume, so `.nx/workspace-data` was main's. A commit there from a shell bound to main ran the shed's hooks with
// main's TMPDIR and Nx socket dir; the shed's Nx wrote its daemon record into main's state, and every Nx client in
// main then failed with "different workspace". Real git runs the real managed hooks here, whose PATH writer serves
// such a shed first. The installed cowshed's stable name stands in for that supervisor start, since no test attaches a
// real image: it writes what the start writes, or fails as one can. No Nx is on PATH, so a hook that got past its PATH
// writer fails on its first program, which names how far it got.
interface Commit {
  readonly fork: string;
  readonly status: number | null;
  readonly stderr: string;
  /** The commits the fork has. */
  readonly commits: string;
  /** What each `cowshed` the commit ran was asked, one line each. */
  readonly serves: string;
}

/**
 * Commit in an interrupted fork beside main's live Nx state, from a shell bound to main, with `serve(fork, root)` as
 * the installed cowshed's effect, and prove main's daemon record bytes and its live `d.sock` untouched afterwards.
 */
async function commitInInterruptedFork(serve: (fork: string, root: string) => string): Promise<Commit> {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'smoo-unserved-')));
  // Main's runtime directory: a socket below the private TMPDIR could exceed Darwin's sun_path bound.
  const run = await ownedSocketDirectory('smoo-unserved-', 'nx/d.sock');
  const server = createServer((socket) => socket.end());
  try {
    const main = join(root, 'main');
    const fork = join(root, 'fork');
    const volume = join(root, 'main-build');
    const tools = join(root, 'bin');
    const installed = join(root, 'Library/Application Support/dev.cowshed/bin');
    for (const directory of [main, fork, join(volume, 'nx/workspace-data/d'), join(run, 'nx'), tools, installed]) {
      mkdirSync(directory, { recursive: true });
    }
    for (const name of ['bash', 'dirname', 'git', 'ln', 'mkdir']) symlinkSync(tool(name), join(tools, name));

    // Main's live Nx state: its daemon record on its build volume, and the daemon listening on its socket.
    const socket = join(run, 'nx/d.sock');
    const listening = Promise.withResolvers<void>();
    server.listen(socket, listening.resolve);
    await listening.promise;
    let connections = 0;
    server.on('connection', () => {
      connections += 1;
    });
    const record = join(volume, 'nx/workspace-data/d/server-process.json');
    writeFileSync(
      record,
      `${JSON.stringify({ processId: process.pid, socketPath: socket, nxVersion: '23.2.1', workspaceRoot: main })}\n`,
    );
    const recorded = readFileSync(record);
    const socketIdentity = statSync(socket).ino;

    // The interrupted fork: a checkout of its own, the mint's env, and main's build link and Nx state links.
    const env = { HOME: root, PATH: tools, GIT_CONFIG_NOSYSTEM: '1' };
    expect(git(fork, ['init', '--quiet'], env).status).toBe(0);
    for (const file of HOOK_FILES) {
      mkdirSync(dirname(join(fork, file)), { recursive: true });
      copyFileSync(join(managedAssetsRoot, 'raw', file), join(fork, file));
    }
    for (const hook of ['pre-commit', 'commit-msg']) {
      symlinkSync(`../../tooling/git-hooks/${hook}.sh`, join(fork, '.git/hooks', hook));
    }
    mkdirSync(join(fork, '.cowshed'));
    writeFileSync(
      join(fork, '.cowshed/workspace.json'),
      `${JSON.stringify({ version: 1, workspace: 'false-ready', role: 'workspace', forkedFrom: 'main' }, null, 2)}\n`,
    );
    writeFileSync(
      join(fork, '.cowshed/env'),
      'if [ "${GITHUB_ACTIONS:-}" != true ]; then unset CI; fi\nexport COWSHED_PORT_BASE=39616\nexport COWSHED_PORT_BLOCK_SIZE=64\nunset COWSHED_RUNTIME_LINK\n',
    );
    symlinkSync(volume, join(fork, '.cowshed/build'));
    mkdirSync(join(fork, '.nx'));
    symlinkSync('../.cowshed/build/nx/workspace-data', join(fork, '.nx/workspace-data'));

    const serves = join(root, 'serves');
    writeFileSync(
      join(installed, 'cowshed'),
      `#!${tool('bash')}\nprintf '%s\\n' "$*" >> '${serves}'\n${serve(fork, root)}\n`,
      { mode: 0o755 },
    );

    const committed = git(
      fork,
      [
        '-c',
        'user.name=Fixture',
        '-c',
        'user.email=fixture@example.invalid',
        'commit',
        '--allow-empty',
        '-m',
        'fix: x',
      ],
      {
        ...env,
        // The shell the commit ran from was bound to main.
        TMPDIR: join(root, 'main-tmp'),
        COWSHED_RUNTIME_LINK: run,
        NX_SOCKET_DIR: join(run, 'nx'),
        NX_WORKSPACE_ROOT_PATH: main,
      },
    );

    expect(readFileSync(record)).toEqual(recorded);
    expect(statSync(socket).ino).toBe(socketIdentity);
    expect(connections).toBe(0);
    // Main's daemon still answers on its socket.
    await connects(socket);
    return {
      fork,
      status: committed.status,
      stderr: committed.stderr,
      commits: git(fork, ['rev-list', '--all', '--count'], env).stdout,
      serves: readFileSync(serves, 'utf8'),
    };
  } finally {
    const closed = Promise.withResolvers<void>();
    server.close(() => closed.resolve());
    await closed.promise;
    rmSync(run, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
  }
}

/** What repo-path says before it serves the fork. */
function serving(fork: string): string {
  return `repo-path: cowshed workspace false-ready at ${fork} is not served: its .cowshed/env sets no TMPDIR, COWSHED_RUNTIME_LINK. Serving it: cowshed exec false-ready -- true\n`;
}

describe('git hooks in an unserved cowshed workspace', () => {
  it("serve it once, then run on the file that serve wrote, never on main's Nx state", async () => {
    // What the start does: its mount settles the checkout's own build link, then it serves the file.
    const commit = await commitInInterruptedFork((fork, root) =>
      [
        `mkdir -p '${root}/fork-build' '${root}/fork-tmp'`,
        `ln -sfn '${root}/fork-build' '${fork}/.cowshed/build'`,
        `printf '%s\\n' "export TMPDIR='${root}/fork-tmp'" 'export COWSHED_RUNTIME_LINK=${root}/fork-run' >> '${fork}/.cowshed/env'`,
      ].join('\n'),
    );
    expect(commit.serves).toBe('exec false-ready -- true\n');
    expect(commit.stderr).toStartWith(serving(commit.fork));
    // Past its PATH writer, pre-commit reached its first program, which this fixture does not provide.
    expect(commit.stderr).toContain('git-format-staged: command not found');
    expect(commit.stderr).not.toContain('failed: cowshed exec');
    expect(commit.status).toBe(1);
    expect(commit.commits).toBe('0\n');
  });

  it("refuse by name when that serve fails, before anything reaches main's Nx state", async () => {
    const commit = await commitInInterruptedFork(
      () => `printf '%s\\n' 'cowshed: workspace false-ready: image lock held by fork' >&2\nexit 7`,
    );
    expect(commit.serves).toBe('exec false-ready -- true\n');
    expect(commit.status).toBe(1);
    expect(commit.stderr).toStartWith(
      `${serving(commit.fork)}repo-path: serving cowshed workspace false-ready at ${commit.fork} failed: cowshed exec false-ready -- true exited 7: cowshed: workspace false-ready: image lock held by fork\n`,
    );
    expect(commit.stderr).toEndWith('\nnext: cowshed exec false-ready -- true\n');
    expect(commit.commits).toBe('0\n');
  });
});
