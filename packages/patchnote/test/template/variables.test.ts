import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import type { PatchnoteConfig } from '../../src/config.js';
import { buildTemplateVariables } from '../../src/template/variables.js';
import type { PackageUpdate } from '../../src/types.js';

const mockConfig: PatchnoteConfig = {
  expo: { enabled: false, autoDetect: true, projects: [] },
  syncpack: { configPath: './.syncpackrc.json', preserveCustomRules: true, fixScriptName: 'syncpack:fix' },
  prStrategy: {
    stackingEnabled: true,
    maxStackDepth: 5,
    autoCloseOldPRs: true,
    resetOnMerge: true,
    stopOnConflicts: true,
    branchPrefix: 'chore/update-deps',
    prTitlePrefix: 'chore: update dependencies',
  },
  autoMerge: { enabled: false, mode: 'none', requireTests: true },
  ai: { provider: 'zai' }, // No API key = fallback
  git: { remote: 'origin', baseBranch: 'main' },
  logger: {
    info: mock(() => {}),
    warn: mock(() => {}),
    error: mock(() => {}),
    debug: mock(() => {}),
  },
};

const sampleUpdates: PackageUpdate[] = [
  { name: 'react', fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major', ecosystem: 'npm' },
  { name: 'vite', fromVersion: '5.0.0', toVersion: '5.1.0', updateType: 'minor', ecosystem: 'npm' },
  { name: 'lodash', fromVersion: '4.17.20', toVersion: '4.17.21', updateType: 'patch', ecosystem: 'npm' },
];

describe('buildTemplateVariables', () => {
  beforeEach(() => {
    delete process.env.ZAI_API_KEY;
  });

  afterEach(() => {
    delete process.env.ZAI_API_KEY;
  });

  test('returns all expected keys', async () => {
    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result).toHaveProperty('header');
    expect(result).toHaveProperty('table');
    expect(result).toHaveProperty('aiSummary');
    expect(result).toHaveProperty('releaseNotes');
    expect(result).toHaveProperty('nixUpdates');
    expect(result).toHaveProperty('warnings');
    expect(result).toHaveProperty('provenanceWarnings');
    expect(result).toHaveProperty('deprecationWarnings');
    expect(result).toHaveProperty('downgrades');
    expect(result).toHaveProperty('updateCount');
    expect(result).toHaveProperty('majorCount');
    expect(result).toHaveProperty('minorCount');
    expect(result).toHaveProperty('patchCount');
  });

  test('computes counts correctly', async () => {
    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.updateCount).toBe(3);
    expect(result.majorCount).toBe(1);
    expect(result.minorCount).toBe(1);
    expect(result.patchCount).toBe(1);
  });

  test('produces empty strings for sections with no content', async () => {
    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    // No nix updates in sampleUpdates
    expect(result.nixUpdates).toBe('');
    // No downgrades
    expect(result.downgrades).toBe('');
    // No provenance issues
    expect(result.provenanceWarnings).toBe('');
    // No deprecation issues
    expect(result.deprecationWarnings).toBe('');
    // AI skipped
    expect(result.aiSummary).toBe('');
  });

  test('populates table when AI is skipped', async () => {
    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.table).toContain('## Dependency Updates');
    expect(result.table).toContain('react: 18.0.0');
    expect(result.table).toContain('Total updates: 3');
  });

  test('populates provenanceWarnings when updates have provenanceDowngraded flag', async () => {
    const updatesWithProvenance: PackageUpdate[] = [
      {
        name: 'bad-pkg',
        fromVersion: '1.0.0',
        toVersion: '2.0.0',
        updateType: 'major',
        ecosystem: 'npm',
        provenanceDowngraded: true,
      },
    ];

    const result = await buildTemplateVariables({
      updates: updatesWithProvenance,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.provenanceWarnings).toContain('Supply Chain Warning');
    expect(result.provenanceWarnings).toContain('bad-pkg');
  });

  test('populates deprecationWarnings when updates have deprecatedMessage', async () => {
    const updatesWithDeprecation: PackageUpdate[] = [
      {
        name: 'old-pkg',
        fromVersion: '1.0.0',
        toVersion: '2.0.0',
        updateType: 'major',
        ecosystem: 'npm',
        deprecatedMessage: 'Use new-pkg instead',
      },
    ];

    const result = await buildTemplateVariables({
      updates: updatesWithDeprecation,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.deprecationWarnings).toContain('Deprecated Packages');
    expect(result.deprecationWarnings).toContain('old-pkg');
  });

  test('populates downgrades when provided', async () => {
    const downgrades: PackageUpdate[] = [
      { name: 'python3', fromVersion: '3.13.0', toVersion: '3.12.0', updateType: 'unknown', ecosystem: 'nix' },
    ];

    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades,
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.downgrades).toContain('Downgrades & Removals');
    expect(result.downgrades).toContain('python3');
  });

  test('populates nixUpdates when nix packages present', async () => {
    const updatesWithNix: PackageUpdate[] = [
      ...sampleUpdates,
      { name: 'nodejs', fromVersion: '20.0.0', toVersion: '22.0.0', updateType: 'unknown', ecosystem: 'nix' },
    ];

    const result = await buildTemplateVariables({
      updates: updatesWithNix,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    expect(result.nixUpdates).toContain('Nix Updates');
    expect(result.nixUpdates).toContain('nodejs');
  });

  test('sets header to commitTitle', async () => {
    const result = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore(deps): update dependencies',
    });

    expect(result.header).toBe('chore(deps): update dependencies');
  });
});
