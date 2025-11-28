/**
 * Git operations helpers
 */

import { execa } from 'execa';
import type { DepUpdaterConfig } from './config.js';
import type { CommandExecutor } from './types.js';

/**
 * Default executor - execa cast to CommandExecutor type
 * The cast is safe because execa's Result extends our ExecutorResult interface
 */
const defaultExecutor = execa as unknown as CommandExecutor;

/**
 * Get the current git branch
 */
export async function getCurrentBranch(repoRoot: string, executor: CommandExecutor = defaultExecutor): Promise<string> {
  const { stdout } = await executor('git', ['rev-parse', '--abbrev-ref', 'HEAD'], {
    cwd: repoRoot,
  });
  return stdout.trim();
}

/**
 * Check if working directory is clean
 */
export async function isWorkingDirectoryClean(
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<boolean> {
  const { stdout } = await executor('git', ['status', '--porcelain'], {
    cwd: repoRoot,
  });
  return stdout.trim() === '';
}

/**
 * Get list of changed files
 */
export async function getChangedFiles(
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<string[]> {
  const { stdout } = await executor('git', ['status', '--porcelain'], {
    cwd: repoRoot,
  });

  // Git porcelain format: XY filename (X=index, Y=worktree, both single chars, then space, then filename)
  // Don't trim lines before parsing to preserve the status format
  return stdout
    .split('\n')
    .filter((line: string) => line.trim().length > 0)
    .map((line: string) => line.substring(3).trim());
}

/**
 * Create a new git branch
 */
export async function createBranch(
  repoRoot: string,
  branchName: string,
  baseBranch?: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  if (baseBranch) {
    await executor('git', ['checkout', '-b', branchName, baseBranch], {
      cwd: repoRoot,
    });
  } else {
    await executor('git', ['checkout', '-b', branchName], {
      cwd: repoRoot,
    });
  }
}

/**
 * Switch to an existing branch
 */
export async function switchBranch(
  repoRoot: string,
  branchName: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  await executor('git', ['checkout', branchName], {
    cwd: repoRoot,
  });
}

/**
 * Check if a branch exists (locally or remotely)
 */
export async function branchExists(
  repoRoot: string,
  branchName: string,
  checkRemote = false,
  executor: CommandExecutor = defaultExecutor,
): Promise<boolean> {
  try {
    if (checkRemote) {
      await executor('git', ['rev-parse', '--verify', `refs/remotes/origin/${branchName}`], {
        cwd: repoRoot,
      });
    } else {
      await executor('git', ['rev-parse', '--verify', `refs/heads/${branchName}`], {
        cwd: repoRoot,
      });
    }
    return true;
  } catch (error) {
    const location = checkRemote ? 'remote' : 'local';
    console.warn(
      `Branch "${branchName}" does not exist (${location}):`,
      error instanceof Error ? error.message : String(error),
    );
    return false;
  }
}

/**
 * Stage files for commit
 */
export async function stageFiles(
  repoRoot: string,
  files: string[],
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  await executor('git', ['add', ...files], {
    cwd: repoRoot,
  });
}

/**
 * Stage all changes
 */
export async function stageAll(repoRoot: string, executor: CommandExecutor = defaultExecutor): Promise<void> {
  await executor('git', ['add', '-A'], {
    cwd: repoRoot,
  });
}

/**
 * Create a git commit
 */
export async function commit(
  repoRoot: string,
  message: string,
  body?: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  const commitMessage = body ? `${message}\n\n${body}` : message;

  await executor('git', ['commit', '-m', commitMessage], {
    cwd: repoRoot,
  });
}

/**
 * Push to remote
 */
export async function push(
  repoRoot: string,
  remote: string,
  branch: string,
  force = false,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  const args = ['push', remote, branch];
  if (force) {
    args.push('--force');
  }

  await executor('git', args, {
    cwd: repoRoot,
  });
}

/**
 * Push with upstream tracking
 */
export async function pushWithUpstream(
  repoRoot: string,
  remote: string,
  branch: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  await executor('git', ['push', '-u', remote, branch], {
    cwd: repoRoot,
  });
}

/**
 * Delete a remote branch
 */
export async function deleteRemoteBranch(
  repoRoot: string,
  remote: string,
  branch: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  await executor('git', ['push', remote, '--delete', branch], {
    cwd: repoRoot,
  });
}

/**
 * Get the last commit message
 */
export async function getLastCommitMessage(
  repoRoot: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<string> {
  const { stdout } = await executor('git', ['log', '-1', '--pretty=%B'], {
    cwd: repoRoot,
  });
  return stdout.trim();
}

/**
 * Check if branch has conflicts with another branch
 */
export async function hasConflicts(
  repoRoot: string,
  sourceBranch: string,
  targetBranch: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<boolean> {
  try {
    // Try a test merge without committing
    await executor('git', ['merge-tree', targetBranch, sourceBranch], {
      cwd: repoRoot,
    });
    return false;
  } catch {
    return true;
  }
}

/**
 * Get repository root directory
 */
export async function getRepoRoot(cwd = process.cwd(), executor: CommandExecutor = defaultExecutor): Promise<string> {
  const { stdout } = await executor('git', ['rev-parse', '--show-toplevel'], {
    cwd,
  });
  const root = stdout.trim();

  // Validate the repository root path
  // Ensure it's an absolute path and doesn't contain suspicious characters
  if (!root) {
    throw new Error('Failed to determine repository root: git returned empty string');
  }

  // Check for absolute path (Unix: starts with /, Windows: C:\)
  if (!root.startsWith('/') && !/^[A-Z]:[/\\]/i.test(root)) {
    throw new Error(`Invalid repository root path: ${root}. Expected absolute path.`);
  }

  return root;
}

/**
 * Fetch from remote
 */
export async function fetch(
  repoRoot: string,
  remote: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  await executor('git', ['fetch', remote], {
    cwd: repoRoot,
  });
}

/**
 * Get diff between two branches/commits
 */
export async function getDiff(
  repoRoot: string,
  from: string,
  to: string,
  files?: string[],
  executor: CommandExecutor = defaultExecutor,
): Promise<string> {
  const args = ['diff', from, to];
  if (files && files.length > 0) {
    args.push('--', ...files);
  }

  const { stdout } = await executor('git', args, {
    cwd: repoRoot,
  });
  return stdout;
}

/**
 * Helper to create a complete update commit
 */
export async function createUpdateCommit(
  config: Partial<DepUpdaterConfig>,
  commitMessage: string,
  commitBody?: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot(undefined, executor));

  // Stage all changes
  await stageAll(repoRoot, executor);

  // Commit with message
  await commit(repoRoot, commitMessage, commitBody, executor);

  config.logger?.info('✓ Created commit:', commitMessage);
}

/**
 * Helper to create and push update branch
 */
export async function createUpdateBranch(
  config: Partial<DepUpdaterConfig>,
  branchName: string,
  baseBranch?: string,
  executor: CommandExecutor = defaultExecutor,
): Promise<void> {
  const repoRoot = config.repoRoot || (await getRepoRoot(undefined, executor));
  const remote = config.git?.remote || 'origin';

  // Create branch
  await createBranch(repoRoot, branchName, baseBranch, executor);
  config.logger?.info('✓ Created branch:', branchName);

  // Push with upstream
  await pushWithUpstream(repoRoot, remote, branchName, executor);
  config.logger?.info('✓ Pushed to remote:', `${remote}/${branchName}`);
}
