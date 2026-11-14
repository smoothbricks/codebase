# time and duration: release the nanosecond brands

- Need: a consumer that keeps event times as nanoseconds converts through the published packages, not through a
  microsecond detour.
- Observed on main `d9e8e8da0c`: `@smoothbricks/time` exports `EpochNanos` (`packages/time/src/index.ts:43`) and
  `computeTimeBoundaryNanos` (`:20`), and `@smoothbricks/duration` exports `durationToNanos` and `addDurationNanos`
  (`packages/duration/src/index.ts:24`), from `9f925b9882` and `d7acb489f8`. Both packages' `package.json` say 0.1.9,
  and the `time@0.1.9` and `duration@0.1.9` tags (`8bfbd86007`) hold none of them.
- Work: publish both packages with a version bump (AGENTS.md, Commands, "Publish").
- Red first: an install of the published `@smoothbricks/time` and `@smoothbricks/duration` resolves those four exports.
