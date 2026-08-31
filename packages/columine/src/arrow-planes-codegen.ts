import { ARROW_PLANES, type ArrowPlane } from './arrow-planes.js';

/**
 * Renders the Rust plane declarations from {@link ARROW_PLANES}.
 *
 * Only Rust is generated. The TypeScript surface is DERIVED by type mapping in
 * `arrow-planes.ts`, so there is no generated TypeScript to format, review or
 * drift — Rust needs generation only because it cannot read TypeScript types.
 *
 * Output is committed rather than produced during a build, so `cargo check`
 * needs no Bun and `tsc` needs no cargo, and a plane rename lands as a
 * reviewable diff instead of vanishing into a build step.
 */

/**
 * Rewrites only the declaration lines inside `arrow_planes! { … }`.
 *
 * Per-plane doc comments stay hand-written in Rust: they explain buffer budgets
 * and why nested types are excluded, which is Rust-facing reasoning rather than
 * table data. Only `Variant = tag => "kind",` is generated.
 */
export function renderRustPlaneDeclarations(
  schemaSource: string,
  planes: readonly ArrowPlane[] = ARROW_PLANES,
): string {
  const opener = 'arrow_planes! {';
  const start = schemaSource.indexOf(opener);
  if (start < 0) {
    throw new Error('arrow_planes! invocation not found in schema.rs');
  }
  const bodyStart = start + opener.length;
  const bodyEnd = schemaSource.indexOf('\n}', bodyStart);
  if (bodyEnd < 0) {
    throw new Error('arrow_planes! invocation is not closed');
  }
  const byVariant = new Map(planes.map((plane) => [plane.variant, plane]));
  const seen = new Set<string>();
  const rewritten = schemaSource
    .slice(bodyStart, bodyEnd)
    .split('\n')
    .map((line) => {
      const declaration = /^(\s+)(\w+) = \d+ => "[^"]+",$/.exec(line);
      if (!declaration) {
        return line;
      }
      const plane = byVariant.get(declaration[2] ?? '');
      if (!plane) {
        throw new Error(`${declaration[2]} is declared in schema.rs but absent from arrow-planes.ts`);
      }
      seen.add(plane.variant);
      return `${declaration[1]}${plane.variant} = ${plane.tag} => "${plane.kind}",`;
    })
    .join('\n');
  const missing = planes.filter((plane) => !seen.has(plane.variant));
  if (missing.length > 0) {
    throw new Error(`planes absent from schema.rs: ${missing.map((plane) => plane.variant).join(', ')}`);
  }
  return schemaSource.slice(0, bodyStart) + rewritten + schemaSource.slice(bodyEnd);
}
