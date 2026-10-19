import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';

import { type CompiledInclusion, indexedCompiledFiles } from './cargo-closure-input.js';

// The pre-push key once moved with the disk rather than the tree: a compiled-in file was named whether or not Nx's
// file index held it, so a build output an `include_str!` reads keyed whatever run last wrote it. These inclusions and
// index answers are generated, with no workspace, Cargo or Nx in them.
describe('the compiled-in files a cargo closure key names', () => {
  /** Under a directory no generated tree path names, so the index never holds what lies there. */
  const OUTPUT = 'z-out/';
  const path = fc
    .array(fc.stringMatching(/^[a-y]{1,4}$/), { minLength: 1, maxLength: 3 })
    .map((segments) => segments.join('/'));
  const inclusion = fc.record({ declaring: path, file: path });
  // A compiled-in file the index does not hold, or one only an unindexed source includes.
  const offIndex = fc.oneof(
    fc.record({ declaring: path, file: path.map((file) => `${OUTPUT}${file}`) }),
    fc.record({ declaring: path.map((source) => `${OUTPUT}${source}`), file: path }),
  );
  const scenario = fc
    .record({
      inclusions: fc.array(inclusion, { maxLength: 8 }),
      offIndex: fc.array(offIndex, { minLength: 1, maxLength: 4 }),
    })
    .chain(({ inclusions, offIndex }) => {
      const tree = [...new Set([...inclusions, ...offIndex].flatMap(({ declaring, file }) => [declaring, file]))];
      return fc
        .subarray(tree.filter((candidate) => !candidate.startsWith(OUTPUT)))
        .map((held) => ({ inclusions, offIndex, held: new Set(held) }));
    });
  const bothHeld =
    (held: ReadonlySet<string>) =>
    ({ declaring, file }: CompiledInclusion): boolean =>
      held.has(declaring) && held.has(file);

  it('names each held file a held source includes, nothing else, and no off-index include_str! target moves it', () => {
    fc.assert(
      fc.property(scenario, ({ inclusions, offIndex, held }) => {
        const named = indexedCompiledFiles(inclusions, held);
        for (const file of named) {
          expect(inclusions.some((included) => included.file === file && bothHeld(held)(included))).toBe(true);
        }
        for (const included of inclusions.filter(bothHeld(held))) expect(named).toContain(included.file);
        expect(indexedCompiledFiles([...offIndex, ...inclusions], held)).toEqual(named);
        expect(indexedCompiledFiles([...inclusions, ...offIndex], held)).toEqual(named);
      }),
    );
  });
});
