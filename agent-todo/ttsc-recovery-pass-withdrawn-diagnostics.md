# ttsc's plugin-failure recovery pass reports diagnostics a DiagnosticPlugin withdrew

## Gap

When a native plugin host's build fails, ttsc collects TypeScript diagnostics through a second pass of the stock
compiler with no plugins (`node_modules/ttsc/src/compiler/internal/build/BuildExecution.ts`,
`appendTypeScriptDiagnosticsAfterPluginFailure`, which runs `runTsgo(execution, ["--noEmit"], …)` and appends what the
host did not print). That pass loads no linked plugin, so a diagnostic a linked `driver.DiagnosticPlugin` withdrew (the
managed ttsc patch's seam, `LoweredDiagnostics`) comes back in the failure report: a dialect loop a plugin lowers is
reported as TS1103 beside the host's real errors.

Observed in a consumer: a test project whose plugin host failed on three unrelated type errors also listed
`tests/decide-for-await-parts.test.ts(34,9): error TS1103`, a loop the plugin admitted and the host had withdrawn.

## Impact

Only the failure path is affected. A build that passes has no recovery pass and reports no withdrawn diagnostic. A
failing build's report names one diagnostic that is not a defect, beside the defects that are.

## Fix at the owner

The recovery pass should drop what a `DiagnosticPlugin` would withdraw. Either run the pass through the host with its
linked plugins and an analysis-only option, or have the host report its withdrawn diagnostics (file, code, span) in its
failure output so the filter removes them alongside the ones it already printed. Both are changes to the managed ttsc
patch (`packages/nx-plugin/managed/raw/patches/ttsc@0.36.3.patch` and `patches/ttsc@0.36.3.patch`, byte-identical), and
the next nx-plugin publish carries them; upstream samchon/ttsc has neither the seam nor the pass change.
