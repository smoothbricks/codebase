import { describe, expect, test } from 'bun:test';
import { buildOnboardingPRBody, ONBOARD_BRANCH, ONBOARD_PR_TITLE } from '../../src/commands/onboard.js';
import type { PackageUpdate, ProjectSetup } from '../../src/types.js';

describe('onboard', () => {
  describe('constants', () => {
    test('ONBOARD_BRANCH should be chore/configure-patchnote', () => {
      expect(ONBOARD_BRANCH).toBe('chore/configure-patchnote');
    });

    test('ONBOARD_PR_TITLE should be Configure Patchnote', () => {
      expect(ONBOARD_PR_TITLE).toBe('Configure Patchnote');
    });
  });

  describe('buildOnboardingPRBody', () => {
    const defaultSetup: ProjectSetup = {
      hasExpo: false,
      hasNix: false,
      hasSyncpack: false,
      packageManager: 'bun',
    };

    const sampleConfig = JSON.stringify({ expo: { enabled: false } }, null, 2);

    test('should include welcome header', () => {
      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, []);
      expect(body).toContain('# Configure Patchnote');
    });

    test('should include detected project setup section', () => {
      const setup: ProjectSetup = {
        hasExpo: true,
        hasNix: true,
        hasSyncpack: false,
        packageManager: 'pnpm',
      };
      const body = buildOnboardingPRBody(setup, sampleConfig, []);
      expect(body).toContain('## Detected Project Setup');
      expect(body).toContain('pnpm');
      expect(body).toContain('Expo');
      expect(body).toContain('Nix');
    });

    test('should include proposed configuration code block', () => {
      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, []);
      expect(body).toContain('## Proposed Configuration');
      expect(body).toContain('```json');
      expect(body).toContain(sampleConfig);
    });

    test('should include next steps section', () => {
      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, []);
      expect(body).toContain('## Next Steps');
      expect(body).toContain('Merge this PR');
    });

    test('should show "No outdated dependencies" when updates array is empty', () => {
      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, []);
      expect(body).toContain('No outdated dependencies detected');
    });

    test('should show update preview table when updates exist', () => {
      const updates: PackageUpdate[] = [
        {
          name: 'lodash',
          fromVersion: '4.17.20',
          toVersion: '4.17.21',
          updateType: 'patch',
          ecosystem: 'npm',
        },
        {
          name: 'react',
          fromVersion: '18.2.0',
          toVersion: '19.0.0',
          updateType: 'major',
          ecosystem: 'npm',
        },
      ];

      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, updates);
      expect(body).toContain('## Dependency Update Preview');
      expect(body).toContain('| Package |');
      expect(body).toContain('lodash');
      expect(body).toContain('4.17.20');
      expect(body).toContain('4.17.21');
      expect(body).toContain('patch');
      expect(body).toContain('react');
      expect(body).toContain('major');
    });

    test('should cap preview at 50 packages with overflow message', () => {
      const updates: PackageUpdate[] = Array.from({ length: 60 }, (_, i) => ({
        name: `package-${i}`,
        fromVersion: '1.0.0',
        toVersion: '2.0.0',
        updateType: 'major' as const,
        ecosystem: 'npm' as const,
      }));

      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, updates);
      expect(body).toContain('and 10 more');
      // Count table rows (lines starting with |, excluding header and separator)
      const tableRows = body.split('\n').filter((line) => line.startsWith('| package-'));
      expect(tableRows).toHaveLength(50);
    });

    test('should show Expo as Yes when detected', () => {
      const setup: ProjectSetup = {
        hasExpo: true,
        hasNix: false,
        hasSyncpack: false,
        packageManager: 'bun',
      };
      const body = buildOnboardingPRBody(setup, sampleConfig, []);
      expect(body).toMatch(/Expo.*Yes/i);
    });

    test('should show Nix as No when not detected', () => {
      const body = buildOnboardingPRBody(defaultSetup, sampleConfig, []);
      expect(body).toMatch(/Nix.*No/i);
    });
  });
});
