# The inferred `bun lint` warns on the files its config ignores

Found 2026-11-27 while gating sb-batch-9. Owner: nx-plugin.

The inferred lint (`packages/nx-plugin/src/index.ts`, `projectLintCommands`) runs the lint Bun as
`"${SMOO_LINT_BUN:?…}" lint --no-script --type-aware -f stylish {eslintSources}`. `{eslintSources}` expands to the
project's sources, and `eslint.config.ts`'s global `ignores` (`**/*.cjs`, `**/*.js`, `**/*.mjs`) excludes some of them.
`bun lint` then prints `0:0 warning File ignored because of a matching ignore pattern` once for each such file: four in
nx-plugin's lint, among them `src/__tests__/dialect-plugin/plugin.cjs`, `src/__tests__/side-output-plugin/plugin.cjs`
and `src/executors/bounded-exec/__tests__/go-link-barrier-toolexec.cjs`. The run still exits 0. The typed ESLint pass
beside it already passes `--no-warn-ignored`.

## Change

Add `--no-warn-ignored` to the lint Bun's command, as the typed pass has it. bun lint accepts the flag
(`bun lint --help-all`, "Compatibility with ESLint"). Update the command constant in `index.test.ts` and the README
sentence that quotes the command. The lint's key does not move: the command is not an input.
