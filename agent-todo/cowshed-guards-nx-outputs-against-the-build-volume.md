# cowshed guards every consumer's Nx outputs against its build volume

specs/cowshed/16_build_volumes.md says Nx outputs never live in the build volume: `target`, `.nx/cache`,
`.nx/workspace-data`, `.codegraph` and the other build-state paths are symlinks onto the shed's build volume, and an Nx
cache restore of an output beneath one replaces the link with a real directory and silently detaches the checkout from
its volume. The validator already lives here (`@smoothbricks/nx-plugin/build-state-output-policy`), but only one
consumer runs it, as a hand-written root check, and that check asked the consumer's pinned cowshed CLI for the paths:
when the host's newer cowshed wrote the build-state record with a field the pinned one did not know, the consumer's
whole lint stopped running.

The rule is cowshed's, so cowshed (or nx-plugin) runs it for every consumer with a build volume:

- the paths come from the record in the volume (`.cowshed/build/cowshed-build-state.json`, its `paths`), read leniently
  by any cowshed version, or from the managing cowshed, never from a consumer's pinned CLI;
- it runs where every Nx graph of a shed is judged, for example an inferred target nx-plugin adds to each workspace, or
  the land gate's preflight, so no consumer writes its own check.

Red: a consumer workspace with no check of its own that declares an output under `target/` is refused, and a record
written by a newer cowshed still judges.
