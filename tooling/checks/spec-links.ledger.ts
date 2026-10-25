// The broken spec references spec-links.ts checks the tree against: every one present when the check landed,
// counted per spec, reference and symbol. A row only shrinks: a fixed reference lowers its count or deletes its
// row in the same commit, and a new broken reference is fixed, never added here.
import type { SpecLinkRow } from './spec-links.ts';

/** Owns every row below: the findings and the fix each waits for. */
const DRIFT = 'agents-todo/spec-link-drift-20261025.md';

const UPSTREAM =
  'Names the Apple iokit sources the passage cites as upstream evidence, not this checkout; qualify the citation with the tree it names.';

export const SPEC_LINKS: readonly SpecLinkRow[] = [
  {
    spec: 'specs/cowshed/01a_apfs.md',
    kind: 'path-line',
    raw: 'iokit/Kernel/IOUserClient.cpp:809',
    count: 1,
    reason: UPSTREAM,
    defect: DRIFT,
  },
];
