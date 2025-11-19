/**
 * Path validation utilities to prevent security vulnerabilities
 */

import { normalize, relative, resolve } from 'node:path';

/**
 * Validates that a resolved path is within a given base directory
 * Prevents path traversal attacks (e.g., ../../../etc/passwd)
 *
 * @param basePath - The base directory path (e.g., repository root)
 * @param targetPath - The path to validate (already resolved)
 * @throws {Error} If the target path is outside the base directory
 *
 * @example
 * ```typescript
 * const repoRoot = '/home/user/repo';
 * const configPath = resolve(repoRoot, userInput);
 * validatePathWithinBase(repoRoot, configPath);
 * // Throws if configPath is outside repoRoot
 * ```
 */
export function validatePathWithinBase(basePath: string, targetPath: string): void {
  // Normalize both paths to ensure consistent comparison
  const normalizedBase = normalize(basePath);
  const normalizedTarget = normalize(targetPath);

  // Get the relative path from base to target
  const relativePath = relative(normalizedBase, normalizedTarget);

  // Security check: if relative path starts with '..', target is outside base
  if (relativePath.startsWith('..')) {
    throw new Error(`Security: Path traversal detected. Path "${targetPath}" is outside base directory "${basePath}"`);
  }

  // Additional check: if relative path is absolute, it's definitely outside
  // This catches cases where targetPath is on a different drive (Windows)
  if (
    relative(normalizedBase, normalizedTarget).startsWith('/') ||
    /^[A-Z]:/i.test(relative(normalizedBase, normalizedTarget))
  ) {
    throw new Error(`Security: Path traversal detected. Path "${targetPath}" is outside base directory "${basePath}"`);
  }
}

/**
 * Safely resolves a user-provided path relative to a base directory
 * Ensures the resulting path stays within the base directory
 *
 * @param basePath - The base directory path
 * @param userPath - User-provided relative path
 * @returns Absolute path within base directory
 * @throws {Error} If resolved path would be outside base directory
 *
 * @example
 * ```typescript
 * const safe = safeResolve('/home/user/repo', './config.json');
 * // Returns: '/home/user/repo/config.json'
 *
 * const unsafe = safeResolve('/home/user/repo', '../../../etc/passwd');
 * // Throws error
 * ```
 */
export function safeResolve(basePath: string, userPath: string): string {
  const resolvedPath = resolve(basePath, userPath);
  validatePathWithinBase(basePath, resolvedPath);
  return resolvedPath;
}
