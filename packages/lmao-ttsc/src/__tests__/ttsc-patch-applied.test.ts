import { describe, expect, it } from 'bun:test';

/**
 * `@ttsc/unplugin` is carried with a repo-local patch (see
 * `patches/@ttsc%2Funplugin@0.28.3.patch`) that scopes its directory-stability
 * terms to the inputs the compiler declared. Without it, a concurrently written
 * sibling directory — cargo's `target/`, this package's `plugin/host`, a
 * `dist-test/` — rejects a transform generation that nothing in it contributed
 * to, and `lmao-ttsc:test` fails in CI with `TtscUnstableGenerationError`.
 *
 * This is guarded because `patchedDependencies` keys pin an exact version and
 * bun DROPS a patch whose key no longer matches the installed version, silently
 * and with exit code 0. A bump of `@ttsc/unplugin` therefore reintroduces the
 * flake with no diagnostic at all.
 */
const SCOPING_MARKERS = ['inputBearingDirectories', 'scopedDirectorySignature'] as const;

async function loadedTransformSource(): Promise<{ path: string; source: string }> {
  const adapter = import.meta.resolve('@ttsc/unplugin/bun');
  const path = new URL('./core/transform.mjs', adapter).pathname;
  return { path, source: await Bun.file(path).text() };
}

describe('ttsc unplugin patch', () => {
  it('scopes directory-stability terms in the module that actually loads', async () => {
    const { path, source } = await loadedTransformSource();
    const missing = SCOPING_MARKERS.filter((marker) => !source.includes(marker));

    expect(
      missing,
      `@ttsc/unplugin is loading WITHOUT the directory-scoping patch from ${path}. ` +
        'Transform generations will be rejected by churn in directories nothing compiled, which fails ' +
        'this package in CI nondeterministically. Either the patch stopped applying (bun drops a ' +
        'patchedDependencies entry whose pinned version no longer matches, silently and with rc=0 — ' +
        're-run `bun patch @ttsc/unplugin@<version>` and regenerate the patch), or upstream ' +
        '(https://github.com/samchon/ttsc) has scoped these terms itself, in which case drop the patch ' +
        'and delete this test.',
    ).toEqual([]);
  });

  it('still rejects a generation when a declared input moves', async () => {
    const { source } = await loadedTransformSource();

    // The patch must not weaken rejection in the other direction: a generation
    // built from half-written sources would be cached and would ship silently
    // wrong transform output. Every FILE in a compared directory stays counted;
    // only subdirectories bearing no declared input are dropped.
    expect(
      source.includes('!member.endsWith("/")'),
      'the patched directory comparison no longer counts every file in a compared directory, so a ' +
        'source file arriving beside a declared input would stop rejecting the generation',
    ).toBe(true);
    // Tear detection during `readdir` is preserved, so a directory that moved
    // mid-enumeration can never compare equal.
    expect(
      source.includes('unstable:'),
      'the patched walk no longer marks a directory torn mid-enumeration as unstable',
    ).toBe(true);
  });
});
