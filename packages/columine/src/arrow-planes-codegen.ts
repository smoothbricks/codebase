import { ARROW_PLANES, type ArrowPlane } from './arrow-planes.js';

/**
 * Renders both language surfaces from {@link ARROW_PLANES}.
 *
 * Output is committed rather than produced during a build, so `cargo check`
 * needs no Bun and `tsc` needs no cargo, and a plane rename shows up as a
 * reviewable diff instead of vanishing into a build step. `arrow-planes.test.ts`
 * fails when a committed surface drifts from what this renders.
 */

const GENERATED_BY = 'UPDATE_GENERATED=1 nx test columine';

function unionMember(plane: ArrowPlane): string {
  const fields = [`readonly kind: '${plane.kind}'`];
  if (plane.offsets) {
    fields.push(`readonly offsets: ${plane.offsets}`);
  }
  if (plane.data) {
    fields.push(`readonly data: ${plane.data}`);
  }
  if (plane.validity) {
    fields.push(`readonly validity?: ${plane.validity}`);
  }
  const single = `  | { ${fields.join('; ')} }`;
  if (single.length <= 120) {
    return single;
  }
  // Biome wraps a member past the line limit, so render it pre-wrapped rather
  // than leaving the formatter to rewrite generated output into drift.
  return ['  | {', ...fields.map((field) => `      ${field};`), '    }'].join('\n');
}

/** The `CompactColumn` union and the tag table the host decodes with. */
export function renderCompactColumnModule(planes: readonly ArrowPlane[] = ARROW_PLANES): string {
  const maxTag = Math.max(...planes.map((plane) => plane.tag));
  return `${[
    `// @generated from arrow-planes.ts — DO NOT EDIT. Regenerate: \`${GENERATED_BY}\`.`,
    '',
    '/**',
    ' * One compact column: a physical plane plus its buffers.',
    ' *',
    ' * The discriminant, its wire tag and its carrier are declared together in',
    ' * `arrow-planes.ts`, so a plane cannot be named on one side of the ABI and',
    ' * carried by a different-signedness array on the other.',
    ' */',
    'export type CompactColumn =',
    // A type alias ends at its semicolon; without it the formatter rewrites the
    // generated file and every run reports drift.
    ...planes.map(unionMember).map((member, index) => (index === planes.length - 1 ? `${member};` : member)),
    '',
    '/** Physical plane tags — the `ArrowType` enum in columine-arrow. */',
    'export const COMPACT_KIND_TAG = {',
    ...planes.map((plane) => `  ${plane.kind}: ${plane.tag},`),
    "} as const satisfies Record<CompactColumn['kind'], number>;",
    '',
    '/**',
    ' * Highest valid plane tag, DERIVED. A bounds check naming one plane’s tag',
    ' * silently rejected every plane appended after it.',
    ' */',
    `export const COMPACT_MAX_KIND_TAG = ${maxTag};`,
  ].join('\n')}\n`;
}

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
