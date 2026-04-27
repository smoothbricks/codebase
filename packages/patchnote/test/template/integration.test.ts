import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import type { PatchnoteConfig } from '../../src/config.js';
import { DEFAULT_PR_BODY_TEMPLATE } from '../../src/template/defaults.js';
import { collapseBlankLines, renderTemplate } from '../../src/template/renderer.js';
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
  {
    name: 'react',
    fromVersion: '18.0.0',
    toVersion: '19.0.0',
    updateType: 'major',
    ecosystem: 'npm',
    changelogUrl: 'https://github.com/facebook/react/releases',
  },
  { name: 'vite', fromVersion: '5.0.0', toVersion: '5.1.0', updateType: 'minor', ecosystem: 'npm' },
  { name: 'lodash', fromVersion: '4.17.20', toVersion: '4.17.21', updateType: 'patch', ecosystem: 'npm' },
];

describe('Integration: template rendering pipeline', () => {
  beforeEach(() => {
    delete process.env.ZAI_API_KEY;
  });

  afterEach(() => {
    delete process.env.ZAI_API_KEY;
  });

  test('default template with fallback (AI disabled) contains key sections', async () => {
    const variables = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    const rendered = renderTemplate(DEFAULT_PR_BODY_TEMPLATE, variables);
    const result = collapseBlankLines(rendered);

    // Should contain the update table
    expect(result).toContain('## Dependency Updates');
    expect(result).toContain('### ! Major Updates');
    expect(result).toContain('react: 18.0.0');
    expect(result).toContain('### Minor Updates');
    expect(result).toContain('vite: 5.0.0');
    expect(result).toContain('### Patch Updates');
    expect(result).toContain('lodash: 4.17.20');
    expect(result).toContain('Total updates: 3');
  });

  test('custom template only includes requested sections', async () => {
    const customTemplate = '# Custom\n{{table}}';

    const variables = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    const rendered = renderTemplate(customTemplate, variables);
    const result = collapseBlankLines(rendered);

    expect(result).toContain('# Custom');
    expect(result).toContain('## Dependency Updates');
    // Should NOT contain sections not in the custom template
    expect(result).not.toContain('Supply Chain Warning');
    expect(result).not.toContain('Deprecated Packages');
  });

  test('empty variables do not produce triple-newlines in final output', async () => {
    const variables = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    const rendered = renderTemplate(DEFAULT_PR_BODY_TEMPLATE, variables);
    const result = collapseBlankLines(rendered);

    // collapseBlankLines should have removed any 3+ consecutive newlines
    expect(result).not.toContain('\n\n\n');
  });

  test('all sections present when every variable is populated', async () => {
    const updatesWithAll: PackageUpdate[] = [
      {
        name: 'react',
        fromVersion: '18.0.0',
        toVersion: '19.0.0',
        updateType: 'major',
        ecosystem: 'npm',
        provenanceDowngraded: true,
        deprecatedMessage: 'Use react-next instead',
      },
      {
        name: 'nodejs',
        fromVersion: '20.0.0',
        toVersion: '22.0.0',
        updateType: 'unknown',
        ecosystem: 'nix',
      },
    ];
    const downgrades: PackageUpdate[] = [
      { name: 'python3', fromVersion: '3.13.0', toVersion: '3.12.0', updateType: 'unknown', ecosystem: 'nix' },
    ];
    const changelogs = new Map([['react', '## 19.0.0\n- Breaking: New rendering model']]);

    const variables = await buildTemplateVariables({
      updates: updatesWithAll,
      downgrades,
      changelogs,
      config: mockConfig,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    const rendered = renderTemplate(DEFAULT_PR_BODY_TEMPLATE, variables);
    const result = collapseBlankLines(rendered);

    expect(result).toContain('Supply Chain Warning');
    expect(result).toContain('## Dependency Updates');
    expect(result).toContain('Nix Updates');
    expect(result).toContain('Downgrades & Removals');
    expect(result).toContain('Release Notes');
    expect(result).toContain('Deprecated Packages');
  });

  test('config prBodyTemplate overrides default', async () => {
    const configWithCustomTemplate: PatchnoteConfig = {
      ...mockConfig,
      prStrategy: {
        ...mockConfig.prStrategy,
        prBodyTemplate: '# {{header}}\n\nUpdates: {{updateCount}}\n\n{{table}}',
      },
    };

    const variables = await buildTemplateVariables({
      updates: sampleUpdates,
      downgrades: [],
      changelogs: new Map(),
      config: configWithCustomTemplate,
      skipAI: true,
      commitTitle: 'chore: update dependencies',
    });

    const template = configWithCustomTemplate.prStrategy.prBodyTemplate ?? DEFAULT_PR_BODY_TEMPLATE;
    const rendered = renderTemplate(template, variables);
    const result = collapseBlankLines(rendered);

    expect(result).toContain('# chore: update dependencies');
    expect(result).toContain('Updates: 3');
    expect(result).toContain('## Dependency Updates');
    // Default-only sections should NOT appear
    expect(result).not.toContain('{{provenanceWarnings}}');
  });
});
