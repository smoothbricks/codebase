# The root helper links to a different LC_UUID when only its build tree differs

Measured on 2026-11-22. The 380915d756 and 01be1c2173 releases changed no helper source, yet their `cowshed-helper`
binaries have different sha256s (8d06e4f2… and 33211eb3…). The binaries have the same size and identical `nm` symbols.
Exactly 48 bytes differ: the 16-byte `LC_UUID` (8576261D… and 58B77631…) and the 32-byte code-signature hash of page 0,
which holds it. A release build of 01be1c2173 in another shed, `zfs-helper-key`, gave a third UUID, 3A2CA2D7…. Two
builds in the same tree, one after the other, gave the same UUID.

fb59b27981 narrowed the helper's Nx key to its own crate, so an unchanged helper replays from the cache and no longer
relinks on every land. A cold cache still relinks, though, and so does a build in another tree. Each such relink gives a
new digest, and setup then reinstalls the helper behind an administrator dialog (09_substrates.md, "Install and
privilege").

Change:

- Find what feeds ld's UUID beyond the output's content. Candidates:
  - an input path that mbx's link spelling (`-oso_prefix`, physical target root) leaves absolute;
  - a temporary object such as rustc's `symbols.o`;
  - Apple ld's own UUID rule.
- Make the release link of identical sources give an identical `LC_UUID` in every tree.
- Do not drop `LC_UUID`: macOS dyld needs it.

Proof: build the helper's production target at one commit in two sheds. Compare the binaries byte for byte, and compare
their `otool -l | grep uuid`. Use each shed's own provided target directory; never redirect one.
