# Can a scriptc emit report a diagnostic that tsc-js does not?

Found 2026-11-26 while removing duplicate type checks. Owner: nx-plugin.

`scriptc-<arch>-<family>` (`packages/nx-plugin/src/index.ts`, `scriptcPlatformTargets`) emits `tsconfig.lib.json` again
in private staging for each platform; its comment at :1573 says "the emit type-checks against its dependencies' emitted
JavaScript, as tsc-js does". A scriptc package's lib sources are therefore type-checked by tsc-js and once more by each
scriptc target the host builds. With `typecheck` deferring to tsc-js, these emits are the remaining place a lib source
is checked twice. Skipping their check is safe only if a scriptc emit can never report a diagnostic tsc-js would not.

## Evidence needed before anything changes

1. The compiler options each scriptc emit uses (lib, target, module, types, the scriptc runtime pack's declarations,
   `SCRIPTC_TARGET`), compared with tsc-js's for the same `tsconfig.lib.json`.
2. Whether a platform's runtime-pack types or FFI declarations (`scriptcFfiInputs`) make the scriptc program differ from
   tsc-js's.
3. A probe on each scriptc package (found by its `scriptc` devDependency): a type error tsc-js accepts but a scriptc
   emit refuses, or a proof that none can exist.

## Outcome

If none can exist, the scriptc emit drops its check and tsc-js stays the one checker. If one can, the scriptc check
stays, and the README states why.
