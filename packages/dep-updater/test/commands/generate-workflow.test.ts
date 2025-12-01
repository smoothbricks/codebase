/**
 * Tests for generate-workflow command (unified template)
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Type definitions for test assertions
interface WorkflowStep {
  name: string;
  if?: string;
  uses?: string;
  with?: Record<string, unknown>;
  env?: Record<string, string>;
  run?: string;
}

describe('Unified Workflow Template Generation', () => {
  const packageRoot = join(__dirname, '../..');
  const templatesDir = join(packageRoot, 'templates/workflows');

  describe('Template File', () => {
    it('should have exactly 1 unified template file', async () => {
      const { readdirSync } = await import('node:fs');
      const files = readdirSync(templatesDir)
        .filter((f) => f.endsWith('.yml'))
        .sort();
      expect(files).toEqual(['unified.yml']);
    });

    it('should have valid template placeholders', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');

      // Check for expected AI-related placeholders
      expect(template).toContain('{{AI_HEADER_SUFFIX}}');
      expect(template).toContain('{{AI_SETUP_NOTE}}');
      expect(template).toContain('{{AI_STEP_SUFFIX}}');
      expect(template).toContain('{{AI_ENV_VAR}}');

      // Should NOT have old auth-specific placeholders
      expect(template).not.toContain('{{STEP_VAR}}');
      expect(template).not.toContain('{{STEP_SECRETS}}');
      expect(template).not.toContain('{{STEP_COPY}}');
      expect(template).not.toContain('{{STEP_COMMIT}}');
      expect(template).not.toContain('{{STEP_VALIDATE}}');
      expect(template).not.toContain('{{AI_SECRET_COMMAND}}');
      expect(template).not.toContain('{{AI_SECRET_LIST}}');
    });

    it('should have runtime auth detection in template', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');

      // GitHub App conditional step
      expect(template).toContain("if: ${{ vars.DEP_UPDATER_APP_ID != '' }}");
      expect(template).toContain('actions/create-github-app-token@v2');

      // Token fallback expression
      expect(template).toContain('${{ steps.app-token.outputs.token || secrets.DEP_UPDATER_TOKEN }}');
      expect(template).toContain('${{ steps.app-token.outputs.token || github.token }}');
    });

    it('should have conditional AI skip logic', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');

      // OpenCode CLI installation conditional on skip flag
      expect(template).toContain("if: ${{ vars.DEP_UPDATER_SKIP_AI != 'true' }}");
      expect(template).toContain('DEP_UPDATER_SKIP_AI');
      expect(template).toContain('--skip-ai');
    });
  });

  describe('Generated Workflows', () => {
    const cliPath = join(packageRoot, 'dist/cli.js');

    async function generateWorkflow(args: string[]): Promise<string> {
      const { $ } = await import('bun');
      const result = await $`node ${cliPath} generate-workflow --dry-run ${args}`.text();
      // Extract just the workflow content (after "Workflow content:")
      const match = result.match(/Workflow content:\n\n([\s\S]*)/);
      return match ? match[1] : result;
    }

    describe('Without AI (--skip-ai)', () => {
      let workflow: string;
      let parsed: any;

      beforeEach(async () => {
        workflow = await generateWorkflow(['--skip-ai']);
        parsed = parseYaml(workflow);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have header without AI suffix on first line', () => {
        // First line should NOT have AI suffix when --skip-ai is used
        const firstLine = workflow.split('\n')[0];
        expect(firstLine).toBe('# Automated dependency updates with dep-updater');
        expect(firstLine).not.toContain('AI Changelog Analysis');
      });

      it('should have correct step name (no AI suffix)', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.startsWith('Run dep-updater'));
        expect(runStep.name).toBe('Run dep-updater');
        expect(runStep.name).not.toContain('AI changelog analysis');
      });

      it('should not have AI API key env vars in run step', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.startsWith('Run dep-updater'));
        const envVars = Object.keys(runStep.env || {});
        expect(envVars).not.toContain('ANTHROPIC_API_KEY');
        expect(envVars).not.toContain('OPENAI_API_KEY');
        expect(envVars).not.toContain('GOOGLE_API_KEY');
      });
    });

    describe('With Free AI (default: opencode)', () => {
      let workflow: string;
      let parsed: any;

      beforeEach(async () => {
        // Default provider is opencode (free), so AI is enabled by default
        workflow = await generateWorkflow([]);
        parsed = parseYaml(workflow);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header with Free AI on first line', () => {
        const firstLine = workflow.split('\n')[0];
        expect(firstLine).toContain('+ Free AI Changelog Analysis');
      });

      it('should have free AI suffix in step name', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.startsWith('Run dep-updater'));
        expect(runStep.name).toBe('Run dep-updater with free AI changelog analysis');
      });

      it('should not have paid API key env vars in run step (free tier)', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.startsWith('Run dep-updater'));
        const envVars = Object.keys(runStep.env || {});
        expect(envVars).not.toContain('ANTHROPIC_API_KEY');
        expect(envVars).not.toContain('OPENAI_API_KEY');
        expect(envVars).not.toContain('GOOGLE_API_KEY');
      });
    });

    describe('Runtime Auth Detection', () => {
      let workflow: string;
      let parsed: any;

      beforeEach(async () => {
        workflow = await generateWorkflow([]);
        parsed = parseYaml(workflow);
      });

      it('should have conditional GitHub App token step', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const appTokenStep = steps.find((s: WorkflowStep) => s.name === 'Generate GitHub App token');

        expect(appTokenStep).toBeDefined();
        expect(appTokenStep.if).toContain("vars.DEP_UPDATER_APP_ID != ''");
        expect(appTokenStep.uses).toBe('actions/create-github-app-token@v2');
      });

      it('should use token fallback in checkout', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const checkoutStep = steps.find((s: WorkflowStep) => s.name === 'Checkout repository');

        expect(checkoutStep.with.token).toContain('steps.app-token.outputs.token');
        expect(checkoutStep.with.token).toContain('github.token');
      });

      it('should use token fallback in GH_TOKEN env', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.includes('Run dep-updater'));

        expect(runStep.env.GH_TOKEN).toContain('steps.app-token.outputs.token');
        expect(runStep.env.GH_TOKEN).toContain('secrets.DEP_UPDATER_TOKEN');
      });

      it('should have conditional OpenCode CLI installation', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const openCodeStep = steps.find((s: WorkflowStep) => s.name === 'Install OpenCode CLI');

        expect(openCodeStep).toBeDefined();
        expect(openCodeStep.if).toContain("vars.DEP_UPDATER_SKIP_AI != 'true'");
      });

      it('should have runtime skip-ai flag handling', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name?.includes('Run dep-updater'));

        expect(runStep.run).toContain('DEP_UPDATER_SKIP_AI');
        expect(runStep.run).toContain('--skip-ai');
      });
    });

    describe('Common Workflow Properties', () => {
      it.each([
        ['without AI', ['--skip-ai']],
        ['with Free AI (default)', []],
      ])('%s should have correct workflow structure', async (_name, args) => {
        const workflow = await generateWorkflow(args);
        const parsed = parseYaml(workflow);

        expect(parsed.name).toBe('Update Dependencies');
        expect(parsed.on.schedule).toEqual([{ cron: '0 2 * * *' }]);
        expect(parsed.on.workflow_dispatch).toBeDefined();
        expect(parsed.permissions).toEqual({ contents: 'write', 'pull-requests': 'write' });
        expect(parsed.jobs['update-deps']).toBeDefined();
        expect(parsed.jobs['update-deps']['runs-on']).toBe('ubuntu-latest');
      });

      it.each([
        ['without AI', ['--skip-ai']],
        ['with Free AI (default)', []],
      ])('%s should have required workflow steps', async (_name, args) => {
        const workflow = await generateWorkflow(args);
        const parsed = parseYaml(workflow);
        const steps = parsed.jobs['update-deps'].steps;

        const stepNames = steps.map((s: WorkflowStep) => s.name);
        expect(stepNames).toContain('Generate GitHub App token');
        expect(stepNames).toContain('Checkout repository');
        expect(stepNames).toContain('Setup Bun');
        expect(stepNames).toContain('Configure git');
        expect(stepNames).toContain('Install OpenCode CLI');
        expect(stepNames.some((n: string) => n.includes('Run dep-updater'))).toBe(true);
      });

      it.each([
        ['without AI', ['--skip-ai']],
        ['with Free AI (default)', []],
      ])('%s should have Nix steps with hashFiles condition', async (_name, args) => {
        const workflow = await generateWorkflow(args);
        const parsed = parseYaml(workflow);
        const steps = parsed.jobs['update-deps'].steps;

        const nixStep = steps.find((s: WorkflowStep) => s.name === 'Install Nix');
        expect(nixStep).toBeDefined();
        expect(nixStep.if).toContain("hashFiles('**/devenv.yaml')");
      });
    });

    describe('Documentation Links', () => {
      it('should link to GETTING-STARTED guide', async () => {
        const workflow = await generateWorkflow([]);
        expect(workflow).toContain('GETTING-STARTED.md');
      });
    });
  });
});
