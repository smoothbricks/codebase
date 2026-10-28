/**
 * What the emit changes about the project's own options: JavaScript and declarations from one non-incremental program.
 * They ride the command line onto the project's tsconfig. A synthesized config would have to sit beside that tsconfig,
 * because `${configDir}` and `paths` anchor to the directory of the config that consumes them, and a transient file there
 * changes the directory listing a concurrent ttsc transform of another project proves while it compiles; that
 * transform then refuses with `graph/accessible-entries-changed`.
 *
 * The one definition of how a package's TypeScript dist is emitted: the typescript-emit executor passes these, and so
 * does every bootstrap that emits a dist before Nx can run (tooling/ttsc-emit.ts, the shell entry). A bootstrap that
 * emitted incrementally wrote the tsconfig's tsBuildInfoFile into dist, a file no Nx output produces, and a release
 * refused the verified dist at its merge boundary for it.
 */
export const EMIT_OVERRIDES: readonly string[] = [
  '--composite',
  'false',
  '--declaration',
  'true',
  '--declarationMap',
  'true',
  '--emitDeclarationOnly',
  'false',
  '--incremental',
  'false',
];
