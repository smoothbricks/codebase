/**
 * Tests for generate-workflow command (action wrapper template)
 */

import { beforeEach, describe, expect, it } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

interface WorkflowStep {
  name: string;
  if?: string;
  uses?: string;
  with?: Record<string, unknown>;
  env?: Record<string, string>;
}

interface ParsedWorkflow {
  name: string;
  on: Record<string, unknown>;
  permissions: Record<string, string>;
  jobs: Record<string, { 'runs-on': string; steps: WorkflowStep[] }>;
}

describe('Action Wrapper Workflow Generation', () => {
  const packageRoot = join(__dirname, '../..');
  const templatesDir = join(packageRoot, 'templates/workflows');

  async function generateWorkflow(args: string[]): Promise<string> {
    const cliPath = join(packageRoot, 'dist/cli.js');
    const { $ } = await import('bun');
    const result = await $`node ${cliPath} generate-workflow --dry-run ${args}`.text();
    const match = result.match(/Workflow content:\n\n([\s\S]*)/);
    return match ? match[1] : result;
  }

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

      expect(template).toContain('{{AI_HEADER_SUFFIX}}');
      expect(template).toContain('{{AI_SETUP_NOTE}}');
      expect(template).toContain('{{AI_ENV_VAR}}');
      expect(template).toContain('{{SKIP_AI_INPUT}}');
      expect(template).toContain('{{WORKFLOW_NAME}}');
      expect(template).toContain('{{SCHEDULE}}');
      expect(template).toContain('{{BASE_BRANCH}}');
      expect(template).toContain('{{CONFIG_PATH_BLOCK}}');
      expect(template).not.toContain('{{AI_STEP_SUFFIX}}');
      expect(template).not.toContain('{{STEP_VAR}}');
      expect(template).not.toContain('{{STEP_SECRETS}}');
      expect(template).not.toContain('{{STEP_COPY}}');
      expect(template).not.toContain('{{STEP_COMMIT}}');
      expect(template).not.toContain('{{STEP_VALIDATE}}');
    });

    it('should have push trigger and rebase-open-prs job in template', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');

      expect(template).toContain('push:');
      expect(template).toContain("branches: ['{{BASE_BRANCH}}']");
      expect(template).toContain('rebase-open-prs:');
      expect(template).toContain('patchnote-rebase');
      expect(template).toContain('fetch-depth: 0');
    });

    it('should have runtime auth detection and action usage in template', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');
      const ghaExpr = '${' + '{';

      expect(template).toContain(`if: ${ghaExpr} vars.PATCHNOTE_APP_ID != '' }}`);
      expect(template).toContain('actions/create-github-app-token@v2');
      expect(template).toContain(
        `${ghaExpr} steps.app-token.outputs.token || secrets.PATCHNOTE_TOKEN || github.token }}`,
      );
      expect(template).toContain('uses: smoothbricks/codebase/packages/patchnote-action@feat/add-dep-updater-package');
    });

    it('should have conditional AI skip logic', async () => {
      const template = await readFile(join(templatesDir, 'unified.yml'), 'utf-8');

      expect(template).toContain('PATCHNOTE_SKIP_AI');
      expect(template).toContain('skip-ai:');
    });
  });

  describe('Generated Workflows', () => {
    describe('Without AI (--skip-ai)', () => {
      let workflow: string;
      let parsed: ParsedWorkflow;

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
        const firstLine = workflow.split('\n')[0];
        expect(firstLine).toBe('# Automated dependency updates with patchnote');
        expect(firstLine).not.toContain('AI Changelog Analysis');
      });

      it('should use the patchnote action step', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(runStep!.uses).toBe('smoothbricks/codebase/packages/patchnote-action@feat/add-dep-updater-package');
      });

      it('should set skip-ai input to true', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(runStep!.with?.['skip-ai']).toBe('true');
        expect(runStep!.env).toBeUndefined();
      });
    });

    describe('With AI (zai)', () => {
      let workflow: string;
      let parsed: ParsedWorkflow;

      beforeEach(async () => {
        workflow = await generateWorkflow(['--enable-ai']);
        parsed = parseYaml(workflow);
      });

      it('should generate valid YAML', () => {
        expect(() => parseYaml(workflow)).not.toThrow();
      });

      it('should have no leftover template placeholders', () => {
        expect(workflow).not.toMatch(/{{AI_/);
        expect(workflow).not.toMatch(/{{STEP_/);
      });

      it('should have correct header with AI on first line', () => {
        const firstLine = workflow.split('\n')[0];
        expect(firstLine).toContain('+ AI Changelog Analysis');
      });

      it('should use the patchnote action step', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(runStep!.uses).toBe('smoothbricks/codebase/packages/patchnote-action@feat/add-dep-updater-package');
      });

      it('should pass runtime skip-ai expression and ZAI secret env', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(String(runStep!.with?.['skip-ai'])).toContain('PATCHNOTE_SKIP_AI');
        expect(runStep!.env?.ZAI_API_KEY).toContain('secrets.ZAI_API_KEY');
      });
    });

    describe('Runtime Auth Detection', () => {
      let workflow: string;
      let parsed: ParsedWorkflow;

      beforeEach(async () => {
        workflow = await generateWorkflow([]);
        parsed = parseYaml(workflow);
      });

      it('should have conditional GitHub App token step', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const appTokenStep = steps.find((s: WorkflowStep) => s.name === 'Generate GitHub App token');

        expect(appTokenStep).toBeDefined();
        expect(appTokenStep!.if).toContain("vars.PATCHNOTE_APP_ID != ''");
        expect(appTokenStep!.uses).toBe('actions/create-github-app-token@v2');
      });

      it('should pass token fallback into the action', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(String(runStep!.with?.token)).toContain('steps.app-token.outputs.token');
        expect(String(runStep!.with?.token)).toContain('secrets.PATCHNOTE_TOKEN');
        expect(String(runStep!.with?.token)).toContain('github.token');
      });

      it('should have three-tier token fallback in priority order: app-token, PATCHNOTE_TOKEN, github.token', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        const ghToken = String(runStep!.with?.token);

        expect(ghToken).toContain('steps.app-token.outputs.token');
        expect(ghToken).toContain('secrets.PATCHNOTE_TOKEN');
        expect(ghToken).toContain('github.token');

        const appTokenIdx = ghToken.indexOf('steps.app-token.outputs.token');
        const patIdx = ghToken.indexOf('secrets.PATCHNOTE_TOKEN');
        const githubTokenIdx = ghToken.indexOf('github.token');

        expect(appTokenIdx).toBeLessThan(patIdx);
        expect(patIdx).toBeLessThan(githubTokenIdx);
      });

      it('should have runtime skip-ai flag handling', () => {
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(String(runStep!.with?.['skip-ai'])).toContain('PATCHNOTE_SKIP_AI');
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
        expect(stepNames).toContain('Run patchnote action');
      });
    });

    describe('Optional wrapper settings', () => {
      it('should support workflow name and schedule overrides', async () => {
        const workflow = await generateWorkflow(['--workflow-name', 'Weekly Patchnote', '--schedule', '0 3 * * 1']);
        const parsed = parseYaml(workflow);

        expect(parsed.name).toBe('Weekly Patchnote');
        expect(parsed.on.schedule).toEqual([{ cron: '0 3 * * 1' }]);
      });

      it('should support explicit config path', async () => {
        const workflow = await generateWorkflow(['--config-path', 'config/custom-patchnote.json']);
        const parsed = parseYaml(workflow);
        const steps = parsed.jobs['update-deps'].steps;
        const runStep = steps.find((s: WorkflowStep) => s.name === 'Run patchnote action');

        expect(runStep).toBeDefined();
        expect(runStep!.with?.['config-path']).toBe('config/custom-patchnote.json');
      });
    });

    describe('Push Trigger and Rebase Job', () => {
      let workflow: string;
      let parsed: ParsedWorkflow;

      beforeEach(async () => {
        workflow = await generateWorkflow(['--skip-ai']);
        parsed = parseYaml(workflow);
      });

      it('should have push trigger on base branch', () => {
        expect(parsed.on.push).toBeDefined();
        const pushConfig = parsed.on.push as { branches: string[] };
        expect(pushConfig.branches).toEqual(['main']);
      });

      it('should have no leftover {{BASE_BRANCH}} placeholder', () => {
        expect(workflow).not.toContain('{{BASE_BRANCH}}');
      });

      it('should have workflow_dispatch with command input', () => {
        const wd = parsed.on.workflow_dispatch as { inputs: { command: { type: string; options: string[] } } };
        expect(wd.inputs.command).toBeDefined();
        expect(wd.inputs.command.type).toBe('choice');
        expect(wd.inputs.command.options).toContain('update-deps');
        expect(wd.inputs.command.options).toContain('rebase-open-prs');
      });

      it('should have conditional update-deps job (not on push)', () => {
        const job = parsed.jobs['update-deps'] as { if?: string; 'runs-on': string; steps: WorkflowStep[] };
        expect(job.if).toBeDefined();
        expect(job.if).toContain("github.event_name == 'schedule'");
        expect(job.if).toContain("inputs.command == 'update-deps'");
      });

      it('should have rebase-open-prs job', () => {
        expect(parsed.jobs['rebase-open-prs']).toBeDefined();
      });

      it('should have rebase job with correct condition', () => {
        const job = parsed.jobs['rebase-open-prs'] as { if?: string; 'runs-on': string; steps: WorkflowStep[] };
        expect(job.if).toContain("github.event_name == 'push'");
        expect(job.if).toContain("inputs.command == 'rebase-open-prs'");
      });

      it('should have concurrency group on rebase job', () => {
        const job = parsed.jobs['rebase-open-prs'] as {
          concurrency?: { group: string; 'cancel-in-progress': boolean };
        };
        expect(job.concurrency).toBeDefined();
        expect(job.concurrency!.group).toBe('patchnote-rebase');
        expect(job.concurrency!['cancel-in-progress']).toBe(true);
      });

      it('should have full git history checkout in rebase job', () => {
        const job = parsed.jobs['rebase-open-prs'] as { steps: WorkflowStep[] };
        const checkoutStep = job.steps.find((s) => s.uses?.startsWith('actions/checkout'));
        expect(checkoutStep).toBeDefined();
        expect(checkoutStep!.with?.['fetch-depth']).toBe(0);
      });

      it('should have GH_TOKEN env var in rebase step', () => {
        const job = parsed.jobs['rebase-open-prs'] as { steps: WorkflowStep[] };
        const rebaseStep = job.steps.find((s) => s.name === 'Rebase open PRs');
        expect(rebaseStep).toBeDefined();
        expect(rebaseStep!.env?.GH_TOKEN).toBeDefined();
        expect(rebaseStep!.env?.GH_TOKEN).toContain('steps.app-token.outputs.token');
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
