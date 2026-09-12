/** Shared artifact target naming conventions; no graph construction or filesystem access. */
export const MACOS_PLATFORM_TARGET_GLOBS = ['*-macos', '*-ios'] as const;
export const LINUX_PLATFORM_TARGET_GLOBS = ['*-linux'] as const;
export const PLATFORM_TARGET_GLOBS = [...MACOS_PLATFORM_TARGET_GLOBS, ...LINUX_PLATFORM_TARGET_GLOBS] as const;
