# Spec references that do not resolve

SPEC-LINK (tooling/checks/spec-links.ts, run by the root lint target through its spec-links target) resolves every code
reference in specs/: a backticked path/to/file.ext:LINE, a Markdown link out of specs/, and a Code: entry with its
symbols. The reference below does not resolve in this tree. It is the one row in tooling/checks/spec-links.ledger.ts;
this brief is the defect that row names. A row leaves the ledger only with its fix in the same commit: rewrite the
reference so it names code that exists here, or qualify it with the tree it names so it stops reading as a path in this
repo, then delete or lower the row. New broken references are fixed, never ledgered.

## Citations of upstream sources

- specs/cowshed/01a_apfs.md:915 — iokit/Kernel/IOUserClient.cpp:809 (names the Apple iokit sources; qualify the citation
  with the tree it names)
