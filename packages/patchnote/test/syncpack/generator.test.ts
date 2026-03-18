import { describe, expect, test } from 'bun:test';
import {
  generateExpoVersionGroups,
  generateWorkspaceRules,
  mergeSyncpackConfig,
} from '../../src/syncpack/generator.js';
import type { ExpoPackageVersions } from '../../src/types.js';

describe('Syncpack Generator', () => {
  describe('generateExpoVersionGroups', () => {
    test('generates version groups from Expo packages', () => {
      const expoVersions: ExpoPackageVersions = {
        expo: '52.0.0',
        react: '19.1.0',
        'react-native': '0.77.0',
        sdkVersion: '52',
        packages: {
          react: '19.1.0',
          'react-native': '0.77.0',
        },
      } as ExpoPackageVersions;

      const versionGroups = generateExpoVersionGroups(expoVersions);

      // Should create separate groups for each package
      expect(versionGroups?.length).toBeGreaterThan(0);

      const reactGroup = versionGroups?.find((g) => g.dependencies?.includes('react'));
      expect(reactGroup).toBeDefined();
      expect(reactGroup?.pinVersion).toBe('19.1.0');
      expect(reactGroup?.label).toContain('Expo SDK');
      expect(reactGroup?.dependencyTypes).toEqual(['prod', 'dev', 'peer']);

      const rnGroup = versionGroups?.find((g) => g.dependencies?.includes('react-native'));
      expect(rnGroup).toBeDefined();
      expect(rnGroup?.pinVersion).toBe('0.77.0');
    });

    test('creates separate version groups for each package', () => {
      const expoVersions: ExpoPackageVersions = {
        expo: '52.0.0',
        react: '19.1.0',
        'react-native': '0.77.0',
        sdkVersion: '52',
        packages: {
          react: '19.1.0',
          'react-native': '0.77.0',
        },
      } as ExpoPackageVersions;

      const versionGroups = generateExpoVersionGroups(expoVersions);

      // Each package gets its own group
      expect(versionGroups?.length).toBeGreaterThanOrEqual(2);

      // React group
      const reactGroup = versionGroups?.find((g) => g.dependencies?.includes('react'));
      expect(reactGroup?.dependencies).toEqual(['react']);
    });

    test('handles empty packages', () => {
      const expoVersions: ExpoPackageVersions = {
        expo: '52.0.0',
        react: '19.1.0',
        'react-native': '0.77.0',
        sdkVersion: '52',
        packages: {},
      } as ExpoPackageVersions;

      const versionGroups = generateExpoVersionGroups(expoVersions);

      expect(versionGroups).toHaveLength(0);
    });
  });

  describe('generateWorkspaceRules', () => {
    test('generates workspace protocol rules for multiple scopes', () => {
      const workspaceScopes = ['@company', '@example'];

      const rules = generateWorkspaceRules(workspaceScopes);

      expect(rules).toHaveLength(1);
      expect(rules?.[0]).toMatchObject({
        packages: ['**'],
        dependencies: ['@company/*', '@example/*'],
        pinVersion: 'workspace:*',
        dependencyTypes: ['prod', 'dev', 'peer'],
      });
      expect(rules?.[0]?.label).toContain('workspace protocol');
      expect(rules?.[0]?.label).toContain('@company');
      expect(rules?.[0]?.label).toContain('@example');
    });

    test('generates rules for single scope', () => {
      const workspaceScopes = ['@company'];

      const rules = generateWorkspaceRules(workspaceScopes);

      expect(rules).toHaveLength(1);
      expect(rules?.[0]?.dependencies).toEqual(['@company/*']);
    });

    test('returns empty array when no scopes provided', () => {
      const rules = generateWorkspaceRules([]);

      expect(rules).toHaveLength(0);
    });

    test('creates glob patterns with wildcard', () => {
      const workspaceScopes = ['@test', '@example'];

      const rules = generateWorkspaceRules(workspaceScopes);

      expect(rules?.[0]?.dependencies).toEqual(['@test/*', '@example/*']);
    });
  });

  describe('mergeSyncpackConfig', () => {
    test('merges Expo version groups with existing config', () => {
      const existingConfig = {
        versionGroups: [
          {
            packages: ['**'],
            dependencies: ['lodash'],
            pinVersion: '4.17.21',
          },
        ],
        semverGroups: [
          {
            packages: ['**'],
            dependencies: ['@company/*'],
            range: 'workspace:*',
          },
        ],
      };

      const expoVersionGroups = [
        {
          packages: ['**'],
          dependencies: ['react'],
          pinVersion: '19.1.0',
        },
      ];

      const merged = mergeSyncpackConfig(existingConfig, expoVersionGroups, true);

      expect(merged.versionGroups).toHaveLength(2);
      expect(merged.versionGroups).toContainEqual(existingConfig.versionGroups[0]);
      expect(merged.versionGroups).toContainEqual(expoVersionGroups[0]);
      expect(merged.semverGroups).toEqual(existingConfig.semverGroups);
    });

    test('preserves custom rules when flag is true', () => {
      const existingConfig = {
        versionGroups: [
          {
            packages: ['**'],
            dependencies: ['custom-package'],
            pinVersion: '1.0.0',
          },
        ],
      };

      const expoVersionGroups = [
        {
          packages: ['**'],
          dependencies: ['react'],
          pinVersion: '19.1.0',
        },
      ];

      const merged = mergeSyncpackConfig(existingConfig, expoVersionGroups, true);

      expect(merged.versionGroups).toContainEqual(existingConfig.versionGroups[0]);
    });

    test('replaces all rules when preserveCustomRules is false', () => {
      const existingConfig = {
        versionGroups: [
          {
            packages: ['**'],
            dependencies: ['custom-package'],
            pinVersion: '1.0.0',
          },
        ],
      };

      const expoVersionGroups = [
        {
          packages: ['**'],
          dependencies: ['react'],
          pinVersion: '19.1.0',
        },
      ];

      const merged = mergeSyncpackConfig(existingConfig, expoVersionGroups, false);

      expect(merged.versionGroups).toEqual(expoVersionGroups);
      expect(merged.versionGroups).not.toContainEqual(existingConfig.versionGroups[0]);
    });

    test('handles empty existing config', () => {
      const existingConfig = {};

      const expoVersionGroups = [
        {
          packages: ['**'],
          dependencies: ['react'],
          pinVersion: '19.1.0',
        },
      ];

      const merged = mergeSyncpackConfig(existingConfig, expoVersionGroups, true);

      expect(merged.versionGroups).toEqual(expoVersionGroups);
    });

    test('preserves other syncpack config properties', () => {
      const existingConfig = {
        versionGroups: [],
        dependencyTypes: ['dev', 'peer', 'prod'],
        semverGroups: [],
        customField: 'custom-value',
      };

      const expoVersionGroups = [
        {
          packages: ['**'],
          dependencies: ['react'],
          pinVersion: '19.1.0',
        },
      ];

      const merged = mergeSyncpackConfig(existingConfig, expoVersionGroups, true);

      expect(merged.dependencyTypes).toEqual(['dev', 'peer', 'prod']);
      expect(merged).toHaveProperty('customField', 'custom-value');
    });
  });
});
