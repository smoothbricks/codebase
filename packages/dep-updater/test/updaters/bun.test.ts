import { describe, expect, test } from 'bun:test';
import { parseBunUpdateOutput, parsePackageJsonDiff } from '../../src/updaters/bun.js';

describe('Bun Updater', () => {
  describe('parsePackageJsonDiff', () => {
    test('parses single package version change', () => {
      const diff = `diff --git a/package.json b/package.json
-    "vite": "^7.1.12",
+    "vite": "^7.2.2",`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates).toHaveLength(1);
      expect(updates[0]).toEqual({
        name: 'vite',
        fromVersion: '7.1.12',
        toVersion: '7.2.2',
        updateType: 'minor',
        ecosystem: 'npm',
      });
    });

    test('parses multiple package changes', () => {
      const diff = `diff --git a/package.json b/package.json
-    "react-router-dom": "^7.9.5"
+    "react-router-dom": "^7.9.6"
-    "@biomejs/biome": "^2.3.2",
+    "@biomejs/biome": "^2.3.5",
-    "vite": "^7.1.12",
+    "vite": "^7.2.2",`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates).toHaveLength(3);
      expect(updates[0].name).toBe('react-router-dom');
      expect(updates[1].name).toBe('@biomejs/biome');
      expect(updates[2].name).toBe('vite');
    });

    test('handles scoped packages', () => {
      const diff = `-    "@vitest/ui": "^4.0.5",
+    "@vitest/ui": "^4.0.9",`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates).toHaveLength(1);
      expect(updates[0].name).toBe('@vitest/ui');
    });

    test('strips semver prefixes', () => {
      const diff = `-    "package": "^1.0.0"
+    "package": "~1.0.1"`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates[0].fromVersion).toBe('1.0.0');
      expect(updates[0].toVersion).toBe('1.0.1');
    });

    test('classifies update types correctly', () => {
      const diff = `-    "major-pkg": "1.0.0"
+    "major-pkg": "2.0.0"
-    "minor-pkg": "1.0.0"
+    "minor-pkg": "1.1.0"
-    "patch-pkg": "1.0.0"
+    "patch-pkg": "1.0.1"`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates[0].updateType).toBe('major');
      expect(updates[1].updateType).toBe('minor');
      expect(updates[2].updateType).toBe('patch');
    });

    test('ignores non-version changes', () => {
      const diff = `-    "name": "old-name"
+    "name": "new-name"
-    "description": "old"
+    "description": "new"`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates).toHaveLength(0);
    });

    test('returns empty array for no changes', () => {
      const diff = `diff --git a/package.json b/package.json
+++ b/package.json`;

      const updates = parsePackageJsonDiff(diff);

      expect(updates).toHaveLength(0);
    });
  });

  describe('parseBunUpdateOutput', () => {
    test('parses single package update', () => {
      const output = '↑ @biomejs/biome 2.3.3 → 2.3.5';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0]).toEqual({
        name: '@biomejs/biome',
        fromVersion: '2.3.3',
        toVersion: '2.3.5',
        updateType: 'patch',
        ecosystem: 'npm',
      });
    });

    test('parses multiple package updates', () => {
      const output = `↑ @biomejs/biome 2.3.3 → 2.3.5
↑ vite 7.2.0 → 7.2.2
↑ react 19.1.0 → 19.2.0`;

      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(3);
      expect(updates[0].name).toBe('@biomejs/biome');
      expect(updates[1].name).toBe('vite');
      expect(updates[2].name).toBe('react');
    });

    test('handles scoped package names', () => {
      const output = '↑ @testing-library/react 16.2.0 → 16.3.0';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].name).toBe('@testing-library/react');
    });

    test('handles packages with hyphens and slashes', () => {
      const output = '↑ @nx/react 22.0.1 → 22.0.2';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].name).toBe('@nx/react');
    });

    test('handles prerelease versions', () => {
      const output = '↑ typescript 5.9.0-beta → 5.9.0-rc.1';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].fromVersion).toBe('5.9.0-beta');
      expect(updates[0].toVersion).toBe('5.9.0-rc.1');
    });

    test('classifies update types correctly', () => {
      const output = `↑ react 19.1.0 → 20.0.0
↑ vite 7.2.0 → 7.3.0
↑ typescript 5.9.0 → 5.9.1`;

      const updates = parseBunUpdateOutput(output);

      expect(updates[0].updateType).toBe('major'); // 19 → 20
      expect(updates[1].updateType).toBe('minor'); // 7.2 → 7.3
      expect(updates[2].updateType).toBe('patch'); // 5.9.0 → 5.9.1
    });

    test('ignores non-update lines', () => {
      const output = `bun update v1.3.1 (89fa0f34)
Checked 2057 installs across 2251 packages (no changes) [3.08s]
↑ vite 7.2.0 → 7.2.2
Some other output`;

      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].name).toBe('vite');
    });

    test('returns empty array when no updates found', () => {
      const output = `bun update v1.3.1
Checked 2057 installs across 2251 packages (no changes)`;

      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(0);
    });

    test('handles downgrade arrows', () => {
      const output = '↓ react 19.2.0 → 19.1.0';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].fromVersion).toBe('19.2.0');
      expect(updates[0].toVersion).toBe('19.1.0');
    });

    test('handles add symbol for new packages', () => {
      const output = '+ lodash 4.17.21 → 4.17.21';
      const updates = parseBunUpdateOutput(output);

      expect(updates).toHaveLength(1);
      expect(updates[0].name).toBe('lodash');
    });
  });
});
