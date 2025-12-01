/**
 * Tests for generate-workflow command
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Import the functions we need to test
// Note: We'll need to export these from generate-workflow.ts
// For now, we'll test via the CLI

// Type definitions for test assertions
interface WorkflowStep {
  name: string;
  uses?: string;
  with?: Record<string, unknown>;
  env?: Record<string, string>;
}

interface ExecaError extends Error {
  exitCode: number;
  stderr: Buffer;
}

describe('Workflow Template Generation', () => {
  const packageRoot = join(__dirname, '../..');
  const templatesDir = join(packageRoot, 'templates/workflows');

  describe('Template Files', () => {
    it('should have exactly 2 template files', async () => {
      const { readdirSync } = await import('node:fs');
      const files = readdirSync(templatesDir)
        .filter((f) => f.endsWith('.yml'))
        .sort();
      expect(files).toEqual(['github-app.yml', 'pat.yml']);
    });

    it('should have valid template placeholders in PAT template', async () => {
      const template = await readFile(join(templatesDir, 'pat.yml'), 'utf-8');

      // Check for expected placeholders
      expect(template).toContain('{{AI_HEADER_SUFFIX}}');
      expect(template).toContain('{{AI_SETUP_STEP}}');
      expect(template).toContain('{{AI_SECRETS_PLURAL}}');
      expect(template).toContain('{{AI_SECRET_COMMAND}}');
      expect(template).toContain('{{AI_STEP_SUFFIX}}');
      expect(template).toContain('{{AI_ENV_VAR}}');
      expect(template).toContain('{{STEP_SECRETS}}');
      expect(template).toContain('{{STEP_COPY}}');
      expect(template).toContain('{{STEP_COMMIT}}');

      // Should NOT have these (GitHub App only)
      expect(template).not.toContain('{{STEP_VAR}}');
      expect(template).not.toContain('{{STEP_VALIDATE}}');
      expect(template).not.toContain('{{AI_SECRET_LIST}}');
    });

    it('should have valid template placeholders in GitHub App template', async () => {
      const template = await readFile(join(templatesDir, 'github-app.yml'), 'utf-8');

      // Check for expected placeholders
      expect(template).toContain('{{AI_HEADER_SUFFIX}}');
      expect(template).toContain('{{AI_SETUP_STEP}}');
      expect(template).toContain('{{AI_SECRETS_PLURAL}}');
      expect(template).toContain('{{AI_SECRET_LIST}}');
      expect(template).toContain('{{AI_STEP_SUFFIX}}');
      expect(template).toContain('{{AI_ENV_VAR}}');
      expect(template).toContain('{{STEP_VAR}}');
      expect(template).toContain('{{STEP_SECRETS}}');
      expect(template).toContain('{{STEP_COPY}}');
      expect(template).toContain('{{STEP_VALIDATE}}');

      // Should NOT have these (PAT only)
      expect(template).not.toContain('{{AI_SECRET_COMMAND}}');
      expect(template).not.toContain('{{STEP_COMMIT}}'); // GitHub App template doesn't have this placeholder
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

    describe('PAT without AI (--skip-ai)', () => {
      let workflow: string;

      beforeEach(async () => {
        // Must explicitly skip AI since default provider (opencode) enables free AI
        workflow = await generateWorkflow(['--skip-ai']);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header', () => {
        expect(workflow).toContain('Personal Access Token (Simple Setup)');
        expect(workflow).not.toContain('AI Changelog Analysis');
      });

      it('should have steps numbered 1-4', () => {
        expect(workflow).toContain('#   1. Generate PAT');
        expect(workflow).toContain('#   2. Add organization secret:');
        expect(workflow).toContain('#   3. Copy this file');
        expect(workflow).toContain('#   4. Commit and push');
        expect(workflow).not.toContain('#   5.');
      });

      it('should have only GH_TOKEN env var', () => {
        expect(workflow).toContain('GH_TOKEN: $' + '{{ secrets.DEP_UPDATER_TOKEN }}');
        expect(workflow).not.toContain('ANTHROPIC_API_KEY');
      });

      it('should have correct step name (no AI suffix)', () => {
        expect(workflow).toContain('- name: Run dep-updater');
        expect(workflow).not.toContain('with AI changelog analysis');
        expect(workflow).not.toContain('with free AI changelog analysis');
      });

      it('should link to Quick Start guide', () => {
        expect(workflow).toContain('QUICK-START.md');
      });
    });

    describe('PAT with Free AI (default: opencode)', () => {
      let workflow: string;

      beforeEach(async () => {
        // Default provider is opencode (free), so AI is enabled by default
        // No --enable-ai needed, but we can use it to be explicit
        workflow = await generateWorkflow([]);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header with Free AI', () => {
        expect(workflow).toContain('Personal Access Token + Free AI Changelog Analysis');
      });

      it('should have steps numbered 1-4 (no API key step for free tier)', () => {
        expect(workflow).toContain('#   1. Generate PAT');
        expect(workflow).toContain('#   2. Add organization secret:'); // singular, not secrets
        expect(workflow).toContain('#   3. Copy this file');
        expect(workflow).toContain('#   4. Commit and push');
        expect(workflow).not.toContain('#   5.');
      });

      it('should have only GH_TOKEN env var (no API key for free tier)', () => {
        expect(workflow).toContain('GH_TOKEN: $' + '{{ secrets.DEP_UPDATER_TOKEN }}');
        expect(workflow).not.toContain('ANTHROPIC_API_KEY');
        expect(workflow).not.toContain('OPENAI_API_KEY');
        expect(workflow).not.toContain('GOOGLE_API_KEY');
      });

      it('should have free AI suffix in step name', () => {
        expect(workflow).toContain('- name: Run dep-updater with free AI changelog analysis');
      });

      it('should mention only DEP_UPDATER_TOKEN secret', () => {
        expect(workflow).toContain('gh secret set DEP_UPDATER_TOKEN');
        expect(workflow).not.toContain('gh secret set ANTHROPIC_API_KEY');
      });
    });

    describe('GitHub App without AI (--skip-ai)', () => {
      let workflow: string;

      beforeEach(async () => {
        // Must explicitly skip AI since default provider (opencode) enables free AI
        workflow = await generateWorkflow(['--auth-type', 'github-app', '--skip-ai']);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header', () => {
        expect(workflow).toContain('GitHub App (Simple Setup)');
        expect(workflow).not.toContain('AI Changelog Analysis');
      });

      it('should have steps numbered 1-6', () => {
        expect(workflow).toContain('#   1. Create GitHub App');
        expect(workflow).toContain('#   2. Install app');
        expect(workflow).toContain('#   3. Add organization variable');
        expect(workflow).toContain('#   4. Add organization secret:');
        expect(workflow).toContain('#   5. Copy this file');
        expect(workflow).toContain('#   6. Validate:');
        expect(workflow).not.toContain('#   7.');
      });

      it('should have GitHub App token generation step', () => {
        expect(workflow).toContain('Generate GitHub App token');
        expect(workflow).toContain('actions/create-github-app-token@v2');
        expect(workflow).toContain('app-id: $' + '{{ vars.DEP_UPDATER_APP_ID }}');
        expect(workflow).toContain('private-key: $' + '{{ secrets.DEP_UPDATER_APP_PRIVATE_KEY }}');
      });

      it('should use GitHub App token in env', () => {
        expect(workflow).toContain('GH_TOKEN: $' + '{{ steps.app-token.outputs.token }}');
        expect(workflow).not.toContain('ANTHROPIC_API_KEY');
      });

      it('should link to Setup guide (not Quick Start)', () => {
        expect(workflow).toContain('SETUP.md');
        expect(workflow).not.toContain('QUICK-START.md');
      });

      it('should have validate-setup instruction', () => {
        expect(workflow).toContain('validate-setup');
      });
    });

    describe('GitHub App with Free AI (default: opencode)', () => {
      let workflow: string;

      beforeEach(async () => {
        // Default provider is opencode (free), so AI is enabled by default
        workflow = await generateWorkflow(['--auth-type', 'github-app']);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header with Free AI', () => {
        expect(workflow).toContain('GitHub App + Free AI Changelog Analysis');
      });

      it('should have steps numbered 1-6 (no API key step for free tier)', () => {
        expect(workflow).toContain('#   1. Create GitHub App');
        expect(workflow).toContain('#   2. Install app');
        expect(workflow).toContain('#   3. Add organization variable');
        expect(workflow).toContain('#   4. Add organization secret:'); // singular
        expect(workflow).toContain('#   5. Copy this file');
        expect(workflow).toContain('#   6. Validate:');
        expect(workflow).not.toContain('#   7.');
      });

      it('should have only GH_TOKEN env var (no API key for free tier)', () => {
        expect(workflow).toContain('GH_TOKEN: $' + '{{ steps.app-token.outputs.token }}');
        expect(workflow).not.toContain('ANTHROPIC_API_KEY');
        expect(workflow).not.toContain('OPENAI_API_KEY');
        expect(workflow).not.toContain('GOOGLE_API_KEY');
      });

      it('should list only APP_PRIVATE_KEY in secrets section', () => {
        expect(workflow).toContain('- DEP_UPDATER_APP_PRIVATE_KEY');
        expect(workflow).not.toContain('- ANTHROPIC_API_KEY');
      });

      it('should have free AI suffix in step name', () => {
        expect(workflow).toContain('- name: Run dep-updater with free AI changelog analysis');
      });
    });

    describe('Common Workflow Properties', () => {
      it.each([
        ['PAT without AI', ['--skip-ai']],
        ['PAT with Free AI (default)', []],
        ['GitHub App without AI', ['--auth-type', 'github-app', '--skip-ai']],
        ['GitHub App with Free AI (default)', ['--auth-type', 'github-app']],
      ])('%s should have correct workflow structure', async (_name, args) => {
        const workflow = await generateWorkflow(args);
        const parsed = parseYaml(workflow);

        expect(parsed.name).toBe('Update Dependencies');
        expect(parsed.on.schedule).toEqual([{ cron: '0 2 * * *' }]);
        expect(parsed.on.workflow_dispatch).toBeDefined();
        expect(parsed.jobs['update-deps']).toBeDefined();
        expect(parsed.jobs['update-deps']['runs-on']).toBe('ubuntu-latest');
      });

      it.each([
        ['PAT without AI', ['--skip-ai']],
        ['PAT with Free AI (default)', []],
        ['GitHub App without AI', ['--auth-type', 'github-app', '--skip-ai']],
        ['GitHub App with Free AI (default)', ['--auth-type', 'github-app']],
      ])('%s should have required workflow steps', async (_name, args) => {
        const workflow = await generateWorkflow(args);
        const parsed = parseYaml(workflow);
        const steps = parsed.jobs['update-deps'].steps;

        // Check for required steps
        const stepNames = steps.map((s: WorkflowStep) => s.name);
        expect(stepNames).toContain('Checkout repository');
        expect(stepNames).toContain('Setup Bun');
        expect(stepNames).toContain('Configure git');
        expect(stepNames.some((n: string) => n.includes('Run dep-updater'))).toBe(true);
      });
    });
  });

  describe('Error Cases', () => {
    it('should reject invalid auth type', async () => {
      const { $ } = await import('bun');
      const cliPath = join(packageRoot, 'dist/cli.js');

      try {
        await $`node ${cliPath} generate-workflow --auth-type invalid`.text();
        expect(true).toBe(false); // Should not reach here
      } catch (error: unknown) {
        const execaError = error as ExecaError;
        expect(execaError.exitCode).toBe(1);
        expect(execaError.stderr.toString()).toContain('must be either "pat" or "github-app"');
      }
    });
  });
});
