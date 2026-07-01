import { existsSync, mkdirSync, readlinkSync, rmSync, symlinkSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { $ } from 'bun';
import { decode } from '../lib/run.js';

export async function applyWorkspaceGitConfig(root: string): Promise<void> {
  const gitDirResult = await $`git rev-parse --git-dir`.cwd(root).quiet().nothrow();
  if (gitDirResult.exitCode !== 0) {
    throw new Error('Not in a git repository');
  }

  const gitDir = resolve(root, decode(gitDirResult.stdout).trim());
  const tooling = join(root, 'tooling');

  await $`git config --local include.path ${join(tooling, 'workspace.gitconfig')}`.cwd(root);

  // Keep the newer runtime version pins on any merge (nvfetcher overlay +
  // devenv.lock) so a mirror sync's `git am --3way` never stalls on a version
  // conflict. Mapped by the managed .gitattributes (merge=smoo-newer-pins);
  // implemented in tooling/direnv/merge-newer-pins.sh. The next `devenv shell`
  // regenerates package.json fields from the resulting runtime.
  await $`git config --local merge.smoo-newer-pins.name ${'keep the newer devenv/nvfetcher runtime pins'}`.cwd(root);
  await $`git config --local merge.smoo-newer-pins.driver ${'bash tooling/direnv/merge-newer-pins.sh %O %A %B %P'}`.cwd(
    root,
  );
  linkHook(gitDir, tooling, 'pre-commit');
  linkHook(gitDir, tooling, 'commit-msg');
}

function linkHook(gitDir: string, tooling: string, name: string): void {
  const source = join(tooling, 'git-hooks', `${name}.sh`);
  if (!existsSync(source)) {
    throw new Error(`Missing ${name} hook source: ${source}`);
  }

  const target = join(gitDir, 'hooks', name);
  if (readLinkOrNull(target) === source) {
    return;
  }

  console.log(`[!] Linking ${name} hook in ${gitDir}`);
  mkdirSync(dirname(target), { recursive: true });
  rmSync(target, { force: true });
  symlinkSync(source, target);
}

function readLinkOrNull(path: string): string | null {
  try {
    return readlinkSync(path);
  } catch {
    return null;
  }
}
