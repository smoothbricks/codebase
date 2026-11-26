# A sandbox's fresh TMPDIR makes every Xcode shim scan Xcode's plug-ins

Measured on 2026-11-26 on the host, at load 30 to 160, in workspaces of two repositories.

## What was measured

`/usr/bin/clang`, `/usr/bin/xcrun` and the other Xcode shims keep their answer in `$TMPDIR/xcrun_db`. A process whose
TMPDIR holds no warm db runs `xcodebuild -sdk <sdk> -version`, which scans every Xcode plug-in
(`IDEInitialize → DVTPlugInManager _scanForPlugIns`, stat'ing each plug-in's Info.plist).

| case                                                            | wall                                  |
| --------------------------------------------------------------- | ------------------------------------- |
| `xcrun --sdk macosx --show-sdk-version`, warm db, sandbox       | 2.5 to 16.7 ms                        |
| the same on the host                                            | 11.7 to 13.8 ms                       |
| `clang -### -x c -c -`, warm db, sandbox / host                 | 72 ms / 47 ms                         |
| a `go env` whose `clang -###` child found no warm db            | 2:43 and 3:09, 0.00 s CPU, in `wait4` |
| `platform-identity.sh`'s `xcrun --show-sdk-version`, no warm db | 3:44 and still in `xcodebuild`        |

A sandbox's TMPDIR is the workspace's own store directory (`/private/cowshed/store/<repo>/tmp/<workspace>`), so each new
workspace starts cold, and so does every test that points TMPDIR at a fresh directory. Several such probes at once under
load hold `bun test` processes past the 180 s idle bound (nx-plugin's seven test tasks were idle-killed twice).
`xcodebuild` also showed in the vnode alerts.

The two callers found are being taken off the shims at their source: ttsc's per-process `go env` (which asked Go's
costly keys and so probed the C compiler) and a downstream repository's platform-identity runtime input. Any other tool
that runs a shim in a sandbox still pays it.

## What to find

- Whether xcrun's db can be shared host-wide read-only into every sandbox, or seeded warm into each workspace's TMPDIR
  at provision, and what xcrun does when the db is read-only or stale (an Xcode update must still be seen).
- Where xcrun reads the db from (`confstr(_CS_DARWIN_USER_TEMP_DIR)` or `$TMPDIR`): a test that sets its own TMPDIR
  bypasses a seeded workspace db either way.

## Done when

A shim run in a fresh workspace's sandbox, and in a test with its own TMPDIR under it, starts no `xcodebuild` (counted
with a recording shim or `fs_usage`) while an Xcode update still changes the answer.
