# A land leaves no developer-linked package's build stale

Twice on 2026-11-23 a consumer repository's land gate failed because a smoothbricks land changed `packages/cli/src` and
the live checkout's `packages/cli/dist` still held the previous build: the consumer's `smoo:developer-links` refused the
link ("refusing developer link(s) to a live checkout whose build does not describe its sources … packages/cli/dist is
stale"), devenv entry failed, and every task of the consumer's gate that enters the shell failed with it. It cleared
only after `nx run cli:tsc-js` was run in the smoothbricks checkout by hand.

The land already gates exactly the tree main will hold and adopts its build volume, but the package `dist` outputs a
consumer links live in the checkout, and "cache-only output restoration hit 0 Nx task(s)" after the adoption.

Owed: after a land adopts, every package another checkout developer-links (the `smoo.developerLinks` targets) holds the
landed build of its sources: the land restores those outputs from the gate's cache, or rebuilds them, before it reports
done. Red: a land that changes a linked package's source leaves its `.build-inputs.json` matching the landed sources.
