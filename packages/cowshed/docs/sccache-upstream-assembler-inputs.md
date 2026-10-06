# Draft upstream report: files no preprocessor line marker names

Status of mozilla/sccache, read at `v0.17.0` and at `main` `c0a78c4` (2026-09-26):

- **`.incbin` / `.include` in assembler source: affected in both modes.** The only handling is in
  `process_preprocessed_file` (`src/compiler/c.rs`), which turns preprocessor cache mode off when the preprocessed text
  holds `.incbin "`, `.incbin  "` or `.incbin \"`. The object key is still the preprocessed text and the arguments,
  which name the file and hold none of its bytes, so a changed file is served the object of its old bytes. Reproduced
  with nixpkgs' unpatched `sccache 0.17.0` and Apple clang 21 with `SCCACHE_DIRECT` true and false. Already reported:
  open PR [#2795](https://github.com/mozilla/sccache/pull/2795) (Linux kernel module signing keys,
  `certs/system_certificates.S`), which hashes a file found against the cwd and refuses the rest. Issue
  [#2700](https://github.com/mozilla/sccache/issues/2700) is the distributed-compile face of the same directive.
- **`#embed` and `__has_embed` under preprocessor cache mode: affected, not reported.** Found nothing for either in the
  tracker. Without preprocessor cache mode both are keyed correctly: clang and gcc expand `#embed` under `-E`, so the
  bytes are in the preprocessed text, and `__has_embed`'s answer changes it.

The comment and issue below are written to be posted as they stand.

## Comment for PR #2795

Thanks for this. We hit the same bug building Bun, whose `InternalModuleRegistryConstants.S` `.incbin`s a 2.6 MB blob of
every builtin JavaScript module, and fixed it in a patched build. One thing differs from this PR, and it decides whether
that file gets cached at all.

Bun compiles from a build directory, and the blob is in a subdirectory the assembler reaches through `-I`, not in the
cwd. With cwd-only resolution that translation unit is never cached, and it is one of the larger ones. gas and LLVM
resolve these operands the same way, and a hasher can follow it exactly:

1. The operand as written, opened against the cwd (absolute paths included). gas: `search_and_open` in `gas/read.c`.
   LLVM: `SourceMgr::OpenIncludeFile`.
2. If that fails to open and the path is relative: each assembler include directory in order. A path that fails to open
   is skipped, not an error. For an absolute path that fails, LLVM tries `<dir>/<absolute>` and gas does not, so that
   case has to be refused.
3. The include directories are every driver `-I`, then every `-I` given through `-Wa,` or `-Xassembler`, whichever order
   they were written in. gcc's `asm_options` spec emits `%{I*}` ahead of `%Y`. clang's assembler job adds `OPT_I_Group`
   ahead of `CollectArgsForIntegratedAssembler`. Neither forwards `-iquote`, `-isystem`, `-idirafter` or `CPATH`.
   Checked empirically with clang 21 (`-Ia -Wa,-Ib`, `-Wa,-Ib -Ia` and `-Xassembler -Ib -Ia` all resolve to `a/`).

Cases a hasher cannot reproduce, and should refuse:

- `-I-`; `-I=dir` and `-I$SYSROOT/dir`, which name the sysroot to the preprocessor but reach the assembler verbatim; an
  empty directory, which is the root to gas and the cwd to LLVM;
- a directory as a candidate, which LLVM skips and gas opens and fails on;
- an operand with a `\` (an escape or a macro parameter) or a `$` (a Darwin macro argument), and any directive in a file
  that uses `.altmacro`, which substitutes parameters inside strings without a `\`.

In C/C++ inline assembly, clang searches every `-I`, `-iquote` and `-isystem` entry, prefixed with the sysroot unless
the entry ignores it (`MCTargetOptions::IASSearchPaths`, filled in `clang/lib/CodeGen/BackendUtil.cpp`). gcc hands its
assembler the `-I` list. The two disagree, so refusing there is simplest.

Two smaller points:

- Old entries. A directive-free compile keeps its key, so nothing else is invalidated. Entries written for a translation
  unit with directives were stored under keys without the file's bytes, and a fix has to keep a lookup from reaching
  them. Keyed compiles get a new key once the digests are added. A refused compile should skip the lookup, not just the
  store.
- Preprocessor cache mode. A translation unit that reads such a file must never record a preprocessor cache entry. The
  entry would replay the key against the source and headers alone.

Our implementation is a public patch against 0.17.0:
[`packages/cowshed/nix/sccache/sccache-embedded-inputs.patch`](../nix/sccache/sccache-embedded-inputs.patch). The
real-compiler tests are in [`real-compiler.rs`](../nix/sccache/real-compiler.rs)
(`incbin_keys_the_file_the_assembler_reads`, `incbin_the_hasher_cannot_resolve_is_not_cached`). Happy to port any of it
onto this PR.

## New issue: preprocessor cache mode serves stale objects for `#embed` and `__has_embed`

**Summary.** With preprocessor cache mode on (the default for the local disk cache), a C23 translation unit that
`#embed`s a file gets a cache hit with the old object after the embedded file changes. The same happens for the answer
to `__has_embed` after the named file appears or disappears. Preprocessor cache mode records the source and every file
the preprocessed text's line markers name. Neither directive produces a line marker for the resource.

**Repro** (sccache 0.17.0, verified with Apple clang 21; any TU with at least one `#include`, so an entry is recorded):

```sh
sccache --stop-server; export SCCACHE_DIRECT=true SCCACHE_DIR=$(mktemp -d)
printf 'typedef int unit_t;\n' > unit.h
printf '#include "unit.h"\nconst unsigned char data[] = {\n#embed "data.bin"\n};\n' > unit.c
printf 'first-bytes' > data.bin
touch -t 202001010000 unit.h unit.c data.bin      # older than the compile, so the entry is recorded
sccache clang -std=c23 -c unit.c -o 1.o
printf 'other-bytes' > data.bin; touch -t 202001010001 data.bin
sccache clang -std=c23 -c unit.c -o 2.o           # cache hit
strings 2.o | grep -- -bytes                      # prints first-bytes
```

The same holds with `#embed` in a header, and for `#if __has_embed("optional.bin")` when `optional.bin` is created
between the compiles.

**Cause.** `process_preprocessed_file` collects the files to record from line markers. `#embed` resources and
`__has_embed` probes appear in none: clang's `-E` writes the bytes inline with no marker, and its depfile lists the
resource (`clang -MD`) but preprocessor cache mode does not read the depfile.

**Suggested fix.** Treat a translation unit that uses either construct like one that uses `__TIME__`, and turn
preprocessor cache mode off for it. The source and every remembered header are already read in chunks to hash them, and
`TimeMacroFinder` scans them for time macros in the same pass. An `EmbedFinder` in that pass can match the identifier
`embed` (which covers `#embed`, `# embed` and `#/**/embed`) and `__has_embed`. Also check `-D` arguments for
`__has_embed`. Over-matching (a variable named `embed`) only costs preprocessor cache mode for that unit. The regular
key is already correct, because the preprocessed text carries the bytes. The preprocessor cache entry format version
needs a bump so that entries recorded before the fix stop being consulted.

**Related, distributed compiles.** With `rewrite_includes_only`, clang's `-frewrite-includes` output keeps the
`#embed "file"` line verbatim, so a key computed from that text lacks the resource's bytes.

**Known gap in the suggested fix.** With `skip_system_headers`, system headers are neither hashed nor scanned, so an
`#embed` in a system header is not seen.

Our patch does the above (`EmbedFinder`, `FORMAT_VERSION` 1). The tests are `embed_in_the_source_defeats_direct_mode`,
`embed_in_a_header_defeats_direct_mode` and `has_embed_defeats_direct_mode` in the same `real-compiler.rs`.
