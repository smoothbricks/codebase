# arrow-builder: appending an enum variant re-types its field

`specs/lmao/01a_trace_schema_system.md` says both "An enum's variant list is append-only" (the `S.enum` bullet) and that
a published field "is never removed, reordered or re-typed" (§Field numbers). But `enumKind`'s digest is its whole
variant list (`packages/arrow-builder/src/lib/schema/field.ts`, `enumKind.digest: JSON.stringify(variants)`), so
appending a variant changes the field's type digest and `checkPublishedFields` refuses the field as `type-changed`: the
append the first sentence allows is the re-type the second forbids.

The ruling (the owner's ruling, 2026-11-14): a variant list is not part of a field's type identity; its own published
list guards append-only. `S.currency` already follows it (its digest names no currency).

- Red first: a library publishes `S.enum(['a', 'b'])`, then declares `S.enum(['a', 'b', 'c'])`; `publishFields` accepts
  it and the field keeps its number. Reordering or removing a variant is refused, by name, as an enum change.
- The enum's digest stops naming its variants; the published field list records the variants and checks append-only
  beside the type digest.
- The spec states the rule once, in §Field numbers, and the `S.enum` bullet points to it.
