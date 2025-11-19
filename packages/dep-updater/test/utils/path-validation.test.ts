/**
 * Security tests for path validation utilities
 */

import { describe, expect, test } from 'bun:test';
import { safeResolve, validatePathWithinBase } from '../../src/utils/path-validation.js';

describe('validatePathWithinBase', () => {
  const baseDir = '/home/user/repo';

  test('should allow paths within base directory', () => {
    const targetPath = '/home/user/repo/config.json';
    expect(() => validatePathWithinBase(baseDir, targetPath)).not.toThrow();
  });

  test('should allow nested paths within base directory', () => {
    const targetPath = '/home/user/repo/src/utils/config.json';
    expect(() => validatePathWithinBase(baseDir, targetPath)).not.toThrow();
  });

  test('should block path traversal with ../', () => {
    const targetPath = '/home/user/repo/../../../etc/passwd';
    expect(() => validatePathWithinBase(baseDir, targetPath)).toThrow(/Security: Path traversal detected/);
  });

  test('should block absolute paths outside base', () => {
    const targetPath = '/etc/passwd';
    expect(() => validatePathWithinBase(baseDir, targetPath)).toThrow(/Security: Path traversal detected/);
  });

  test('should block paths that resolve outside base', () => {
    const targetPath = '/home/user/other-repo/file.txt';
    expect(() => validatePathWithinBase(baseDir, targetPath)).toThrow(/Security: Path traversal detected/);
  });

  test('should allow base directory itself', () => {
    expect(() => validatePathWithinBase(baseDir, baseDir)).not.toThrow();
  });

  test('should block parent directory access', () => {
    const targetPath = '/home/user';
    expect(() => validatePathWithinBase(baseDir, targetPath)).toThrow(/Security: Path traversal detected/);
  });

  test('should block sibling directory access', () => {
    const targetPath = '/home/user/other-dir/file.txt';
    expect(() => validatePathWithinBase(baseDir, targetPath)).toThrow(/Security: Path traversal detected/);
  });

  test('should handle paths with trailing slashes', () => {
    const targetPath = '/home/user/repo/src/';
    expect(() => validatePathWithinBase(baseDir, targetPath)).not.toThrow();
  });
});

describe('safeResolve', () => {
  const baseDir = '/home/user/repo';

  test('should resolve safe relative paths', () => {
    const result = safeResolve(baseDir, './config.json');
    expect(result).toBe('/home/user/repo/config.json');
  });

  test('should resolve nested relative paths', () => {
    const result = safeResolve(baseDir, 'src/utils/config.json');
    expect(result).toBe('/home/user/repo/src/utils/config.json');
  });

  test('should block path traversal attempts', () => {
    expect(() => safeResolve(baseDir, '../../../etc/passwd')).toThrow(/Security: Path traversal detected/);
  });

  test('should block absolute paths outside base', () => {
    expect(() => safeResolve(baseDir, '/etc/passwd')).toThrow(/Security: Path traversal detected/);
  });

  test('should handle current directory reference', () => {
    const result = safeResolve(baseDir, '.');
    expect(result).toBe('/home/user/repo');
  });

  test('should handle multiple traversal attempts', () => {
    expect(() => safeResolve(baseDir, '../../../../../../etc/passwd')).toThrow(/Security: Path traversal detected/);
  });

  test('should block encoded path traversal (URL encoding)', () => {
    // Some systems might try %2e%2e for ..
    // After resolve, this becomes a normal path
    const malicious = '..%2F..%2Fetc%2Fpasswd';
    // This test verifies that even if someone tries encoding, after resolve it still gets caught
    expect(() => safeResolve(baseDir, malicious)).toThrow(/Security/);
  });

  test('should handle special characters in filenames', () => {
    // Null bytes and other special characters in paths are typically rejected by the OS
    // Node.js resolve() will normalize them, making them safe paths within the base
    // As long as they don't traverse outside, they're allowed
    const result = safeResolve(baseDir, 'config-v1.0.json');
    expect(result).toBe('/home/user/repo/config-v1.0.json');
  });

  test('should handle empty string', () => {
    // Empty string should resolve to base directory
    const result = safeResolve(baseDir, '');
    expect(result).toBe(baseDir);
  });

  test('should work with real-world package.json path', () => {
    const result = safeResolve(baseDir, './package.json');
    expect(result).toBe('/home/user/repo/package.json');
  });

  test('should work with real-world syncpack config path', () => {
    const result = safeResolve(baseDir, './.syncpackrc.json');
    expect(result).toBe('/home/user/repo/.syncpackrc.json');
  });

  test('should block traversal even with legitimate-looking file', () => {
    expect(() => safeResolve(baseDir, '../other-repo/package.json')).toThrow(/Security: Path traversal detected/);
  });
});

describe('Cross-platform path handling', () => {
  test('should handle Windows-style paths (when on Windows)', () => {
    // Skip this test if not on Windows
    if (process.platform !== 'win32') {
      return;
    }

    const baseDir = 'C:\\Users\\user\\repo';
    const result = safeResolve(baseDir, 'config.json');
    expect(result).toBe('C:\\Users\\user\\repo\\config.json');
  });

  test('should block Windows-style path traversal', () => {
    // Skip this test if not on Windows
    if (process.platform !== 'win32') {
      return;
    }

    const baseDir = 'C:\\Users\\user\\repo';
    expect(() => safeResolve(baseDir, '..\\..\\..\\Windows\\System32\\config.txt')).toThrow(/Security/);
  });
});

describe('Edge cases and attack vectors', () => {
  const baseDir = '/home/user/repo';

  test('should handle paths with spaces', () => {
    const result = safeResolve(baseDir, 'my config/file.json');
    expect(result).toBe('/home/user/repo/my config/file.json');
  });

  test('should handle paths with special characters', () => {
    const result = safeResolve(baseDir, 'config-v1.2.3.json');
    expect(result).toBe('/home/user/repo/config-v1.2.3.json');
  });

  test('should block symlink-style traversal patterns', () => {
    // While we can't test actual symlinks without filesystem access,
    // we can test that path patterns that might be used in symlink attacks are blocked
    expect(() => safeResolve(baseDir, '../symlink-to-root/etc/passwd')).toThrow(/Security/);
  });

  test('should handle deeply nested valid paths', () => {
    const result = safeResolve(baseDir, 'a/b/c/d/e/f/g/file.json');
    expect(result).toBe('/home/user/repo/a/b/c/d/e/f/g/file.json');
  });

  test('should block mixed traversal patterns', () => {
    expect(() => safeResolve(baseDir, './src/../../etc/passwd')).toThrow(/Security/);
  });

  test('should block traversal after valid path', () => {
    expect(() => safeResolve(baseDir, 'src/utils/../../../../../../etc/passwd')).toThrow(/Security/);
  });
});

describe('Real-world use cases from dep-updater', () => {
  test('should work with update-expo.ts packageJsonPath', () => {
    const repoRoot = '/Users/niko/Developer/private';
    const userInput = './package.json';
    const result = safeResolve(repoRoot, userInput);
    expect(result).toBe('/Users/niko/Developer/private/package.json');
  });

  test('should work with update-expo.ts syncpackPath', () => {
    const repoRoot = '/Users/niko/Developer/private';
    const userInput = './.syncpackrc.json';
    const result = safeResolve(repoRoot, userInput);
    expect(result).toBe('/Users/niko/Developer/private/.syncpackrc.json');
  });

  test('should block malicious packageJsonPath', () => {
    const repoRoot = '/Users/niko/Developer/private';
    const maliciousInput = '../../../etc/passwd';
    expect(() => safeResolve(repoRoot, maliciousInput)).toThrow(/Security/);
  });

  test('should work with nested package.json in workspace', () => {
    const repoRoot = '/Users/niko/Developer/private';
    const packagePath = 'packages/dep-updater/package.json';
    const result = safeResolve(repoRoot, packagePath);
    expect(result).toBe('/Users/niko/Developer/private/packages/dep-updater/package.json');
  });
});
