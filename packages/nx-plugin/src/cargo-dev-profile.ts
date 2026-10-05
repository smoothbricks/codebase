/**
 * The two local profiles every Cargo workspace root declares, shared by the
 * two things that must agree about them: the package generator, which writes
 * them into a new workspace, and the monorepo policy, which refuses a root
 * that lacks one. Both render from {@link CARGO_DEV_PROFILE_REQUIREMENTS}, so
 * the TOML a failure tells you to add is the TOML a new workspace starts with.
 *
 * `dev` is the edit-compile-test loop: workspace crates get file:line and stay
 * incremental, dependencies carry no debuginfo. `debugging` is the same build
 * with full DWARF everywhere, in its own `target/debugging` directory, so
 * attaching a debugger never invalidates a dev unit or its cache.
 */

/** A profile this policy owns. */
export type CargoLocalProfile = 'dev' | 'debugging';

/**
 * Where in a profile a key lives: the profile's own table (workspace members),
 * `package."*"` (every non-member dependency), or `build-override` (build
 * scripts, proc macros, and their dependencies).
 */
export type CargoProfileScope = 'members' | 'dependencies' | 'build-override';

export type CargoProfileKey = 'inherits' | 'debug' | 'split-debuginfo';

export interface CargoProfileRequirement {
  readonly profile: CargoLocalProfile;
  readonly scope: CargoProfileScope;
  readonly key: CargoProfileKey;
  /** The canonical TOML value; `debug` compares by level, so `0`, `false` and `"none"` agree. */
  readonly value: string | number;
  readonly why: string;
}

const SAME_UNIT =
  'A crate that both build scripts or proc macros and ordinary code use is one unit only while both see the same ' +
  'debug level; otherwise Cargo compiles it, and everything above it, twice.';

export const CARGO_DEV_PROFILE_REQUIREMENTS: readonly CargoProfileRequirement[] = [
  {
    profile: 'dev',
    scope: 'members',
    key: 'debug',
    value: 'line-tables-only',
    why: 'Workspace crates keep file:line in backtraces and profiles without paying for full DWARF.',
  },
  {
    profile: 'dev',
    scope: 'members',
    key: 'split-debuginfo',
    value: 'unpacked',
    why: 'Debuginfo stays in the object files instead of being relinked into every binary, so links stay fast.',
  },
  {
    profile: 'dev',
    scope: 'dependencies',
    key: 'debug',
    value: 0,
    why: 'Dependencies carry no debuginfo: measured -57% target bytes at flat CPU.',
  },
  {
    profile: 'dev',
    scope: 'build-override',
    key: 'debug',
    value: 0,
    why: `It must equal [profile.dev.package."*"] debug. ${SAME_UNIT}`,
  },
  {
    profile: 'debugging',
    scope: 'members',
    key: 'inherits',
    value: 'dev',
    why:
      'Debug with `cargo build --profile debugging` or `cargo test --profile debugging`: it is the dev profile with ' +
      'full debuginfo, built into target/debugging, so it never invalidates the dev units or their cache.',
  },
  {
    profile: 'debugging',
    scope: 'members',
    key: 'debug',
    value: 2,
    why: 'Workspace crates carry full DWARF for the debugger.',
  },
  {
    profile: 'debugging',
    scope: 'dependencies',
    key: 'debug',
    value: 2,
    why: 'A custom profile inherits dev\'s [profile.dev.package."*"] debug = 0, which leaves dependencies opaque to the debugger.',
  },
  {
    profile: 'debugging',
    scope: 'build-override',
    key: 'debug',
    value: 2,
    why: `It must equal [profile.debugging.package."*"] debug. ${SAME_UNIT}`,
  },
];

/** The TOML table header a requirement lives under, without brackets. */
export function cargoProfileTable(profile: CargoLocalProfile, scope: CargoProfileScope): string {
  switch (scope) {
    case 'members':
      return `profile.${profile}`;
    case 'dependencies':
      return `profile.${profile}.package."*"`;
    case 'build-override':
      return `profile.${profile}.build-override`;
  }
}

/** One TOML key line, `debug = 0` or `inherits = "dev"`. */
export function cargoProfileAssignment(requirement: CargoProfileRequirement): string {
  return `${requirement.key} = ${JSON.stringify(requirement.value)}`;
}

/** Every required table, each key commented with its reason, as a new workspace root carries them. */
export function renderCargoDevProfiles(): string {
  const tables = new Map<string, CargoProfileRequirement[]>();
  for (const requirement of CARGO_DEV_PROFILE_REQUIREMENTS) {
    const table = cargoProfileTable(requirement.profile, requirement.scope);
    const requirements = tables.get(table);
    if (requirements === undefined) {
      tables.set(table, [requirement]);
    } else {
      requirements.push(requirement);
    }
  }
  return [...tables]
    .map(([table, requirements]) =>
      [
        `[${table}]`,
        ...requirements.flatMap((requirement) => [`# ${requirement.why}`, cargoProfileAssignment(requirement)]),
      ].join('\n'),
    )
    .join('\n\n');
}
