/** Shared artifact target naming conventions; no graph construction or filesystem access. */
export const MACOS_PLATFORM_TARGET_GLOBS = ['*-macos', '*-ios'] as const;
export const LINUX_PLATFORM_TARGET_GLOBS = ['*-linux'] as const;
export const PLATFORM_TARGET_GLOBS = [...MACOS_PLATFORM_TARGET_GLOBS, ...LINUX_PLATFORM_TARGET_GLOBS] as const;

/**
 * The Nx configuration that builds what ships. A cargo build target's default
 * options compile cargo's `dev` profile, which every local build, test and
 * gate shares; this configuration adds `--release`. Only release tooling
 * selects it, so a shipped artifact is the only thing compiled twice.
 */
export const RELEASE_CONFIGURATION = 'production';
