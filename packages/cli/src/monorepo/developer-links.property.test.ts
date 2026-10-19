import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import fc from 'fast-check';
import { hasStaleDeveloperLinks } from '../../../nx-plugin/managed/raw/tooling/direnv/developer-links.ts';

// CI's cache once restored the install stamp without the links it vouched for, and the stamp alone said the install
// was current. A stamp cannot see whether a link still resolves, so install currency also reads the links
// (bunInstaller.isCurrent): a matching stamp is stale while a developer link dangles, or while a live one names a
// package the install must relink. These trees are generated: each is real symlinks in a fresh directory.
describe('a stamped install is stale exactly while its developer links say so', () => {
  type Target = 'inside' | 'inside-missing' | 'outside' | 'outside-missing';
  const NAMES = ['nx', 'left-pad', '@scope/nx', '@scope/util'];
  const link = fc.record({
    tree: fc.constantFrom('', 'packages/member'),
    name: fc.constantFrom(...NAMES),
    target: fc.constantFrom<Target>('inside', 'inside-missing', 'outside', 'outside-missing'),
  });
  const links = fc.uniqueArray(link, { selector: ({ tree, name }) => `${tree}/${name}`, maxLength: 8 });

  /** A checkout whose root and member trees hold `planted`, beside the sibling checkouts they may point into. */
  function plant(planted: readonly { tree: string; name: string; target: Target }[]): string {
    const base = realpathSync(mkdtempSync(join(tmpdir(), 'developer-links-property-')));
    const root = join(base, 'root');
    mkdirSync(root);
    writeFileSync(
      join(root, 'bun.lock'),
      JSON.stringify({ lockfileVersion: 1, workspaces: { '': {}, 'packages/member': {} } }),
    );
    planted.forEach(({ tree, name, target }, index) => {
      const at = join(root, tree, 'node_modules', name);
      const destinations: Record<Target, string> = {
        inside: join(root, 'node_modules', '.bun', `entry-${index}`),
        'inside-missing': join(root, 'node_modules', '.bun', `gone-${index}`),
        outside: join(base, 'checkouts', `provider-${index}`),
        'outside-missing': join(base, 'checkouts', `removed-${index}`),
      };
      if (target === 'inside' || target === 'outside') mkdirSync(destinations[target], { recursive: true });
      mkdirSync(dirname(at), { recursive: true });
      symlinkSync(destinations[target], at);
    });
    return base;
  }

  it('is stale while a link out of the checkout dangles or a live one names a relinked package, and only then', () => {
    fc.assert(
      fc.property(links, fc.subarray(NAMES), (planted, relink) => {
        const base = plant(planted);
        try {
          expect(hasStaleDeveloperLinks(join(base, 'root'), relink)).toBe(
            planted.some(({ target }) => target === 'outside-missing') ||
              planted.some(({ name, target }) => target === 'outside' && relink.includes(name)),
          );
        } finally {
          rmSync(base, { recursive: true, force: true });
        }
      }),
    );
  });
});
