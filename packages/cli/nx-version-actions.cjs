const nxJsVersionActions = require('@nx/js/src/release/version-actions');

module.exports = nxJsVersionActions.default ?? nxJsVersionActions;

module.exports.afterAllProjectsVersioned = async (cwd, options) => {
  const result = await nxJsVersionActions.afterAllProjectsVersioned(cwd, options);

  // Temporary Bun workaround. Remove this hook only after all three issues are
  // fixed in supported Bun versions:
  // - https://github.com/oven-sh/bun/issues/18906
  // - https://github.com/oven-sh/bun/issues/20477
  // - https://github.com/oven-sh/bun/issues/20829
  // Nx runs `bun install --lockfile-only`, but Bun currently leaves workspace
  // versions stale in bun.lock. `bun pm pack` then rewrites `workspace:*` using
  // those stale lockfile versions instead of the current package.json versions.
  const { syncBunLockfileVersions } = await import('./dist/monorepo/lockfile.js');
  const updated = syncBunLockfileVersions(cwd);
  if (updated === 0) {
    return result;
  }

  return {
    changedFiles: Array.from(new Set([...result.changedFiles, 'bun.lock'])),
    deletedFiles: result.deletedFiles,
  };
};
