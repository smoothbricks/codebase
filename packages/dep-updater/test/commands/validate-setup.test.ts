/**
 * Tests for validate-setup command
 */

import { describe, expect, test } from 'bun:test';
import { validateSetup } from '../../src/commands/validate-setup.js';
import { SilentLogger } from '../../src/logger.js';
import { createErrorExeca, createExecaSpy } from '../helpers/mock-execa.js';

describe('validateSetup', () => {
  const logger = new SilentLogger();

  test('should return 0 when all checks pass', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': JSON.stringify({
        contents: 'write',
        pull_requests: 'write',
      }),
    });

    // Mock environment variables
    process.env.DEP_UPDATER_APP_ID = '123456';
    process.env.DEP_UPDATER_APP_PRIVATE_KEY = 'test-key';

    const exitCode = await validateSetup(logger, '/repo', spy.mock);

    expect(exitCode).toBe(0);

    // Clean up
    delete process.env.DEP_UPDATER_APP_ID;
    delete process.env.DEP_UPDATER_APP_PRIVATE_KEY;
  });

  test('should return 1 when gh CLI is not installed', async () => {
    const mockExeca = createErrorExeca('command not found: gh');

    const exitCode = await validateSetup(logger, '/repo', mockExeca);

    expect(exitCode).toBe(1);
  });

  test('should return 1 when gh CLI is not authenticated', async () => {
    // gh auth status will fail (not mocked, so will throw)
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh --version') {
        return { stdout: 'gh version 2.40.0', stderr: '', exitCode: 0 };
      }
      if (key === 'gh auth status') {
        throw new Error('not logged in');
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const exitCode = await validateSetup(logger, '/repo', mockExeca);

    expect(exitCode).toBe(1);
  });

  test('should return 1 when GitHub App is not installed (404)', async () => {
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh --version') {
        return { stdout: 'gh version 2.40.0', stderr: '', exitCode: 0 };
      }
      if (key === 'gh auth status') {
        return { stdout: 'Logged in', stderr: '', exitCode: 0 };
      }
      if (key.includes('gh api /repos/{owner}/{repo}/installation')) {
        const error = new Error('Not Found') as Error & { stderr: string };
        error.stderr = '404';
        throw error;
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const exitCode = await validateSetup(logger, '/repo', mockExeca);

    expect(exitCode).toBe(1);
  });

  test('should return 1 when GitHub App has insufficient permissions', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': JSON.stringify({
        contents: 'read', // Wrong permission level
        pull_requests: 'write',
      }),
    });

    const exitCode = await validateSetup(logger, '/repo', spy.mock);

    expect(exitCode).toBe(1);
  });

  test('should return 1 when GitHub App credentials are not configured', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': JSON.stringify({
        contents: 'write',
        pull_requests: 'write',
      }),
    });

    // Ensure env vars are not set
    delete process.env.DEP_UPDATER_APP_ID;
    delete process.env.DEP_UPDATER_APP_PRIVATE_KEY;
    delete process.env.GH_APP_ID;
    delete process.env.GH_APP_PRIVATE_KEY;

    const exitCode = await validateSetup(logger, '/repo', spy.mock);

    expect(exitCode).toBe(1);
  });

  test('should pass when config file exists and is valid', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': JSON.stringify({
        contents: 'write',
        pull_requests: 'write',
      }),
    });

    process.env.DEP_UPDATER_APP_ID = '123456';
    process.env.DEP_UPDATER_APP_PRIVATE_KEY = 'test-key';

    // Run from actual repo root where config exists
    const exitCode = await validateSetup(logger, process.cwd(), spy.mock);

    expect(exitCode).toBe(0);

    // Clean up
    delete process.env.DEP_UPDATER_APP_ID;
    delete process.env.DEP_UPDATER_APP_PRIVATE_KEY;
  });

  test('should handle missing permissions gracefully', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': JSON.stringify({
        contents: 'write',
        // Missing pull_requests permission
      }),
    });

    const exitCode = await validateSetup(logger, '/repo', spy.mock);

    expect(exitCode).toBe(1);
  });

  test('should detect when installation check fails for non-404 reasons', async () => {
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');

      if (key === 'gh --version') {
        return { stdout: 'gh version 2.40.0', stderr: '', exitCode: 0 };
      }
      if (key === 'gh auth status') {
        return { stdout: 'Logged in', stderr: '', exitCode: 0 };
      }
      if (key.includes('gh api /repos/{owner}/{repo}/installation')) {
        throw new Error('Network error'); // Not a 404
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    const exitCode = await validateSetup(logger, '/repo', mockExeca);

    expect(exitCode).toBe(1);
  });

  test('should handle malformed permissions JSON', async () => {
    const spy = createExecaSpy({
      'gh --version': 'gh version 2.40.0',
      'gh auth status': 'Logged in',
      'gh api /repos/{owner}/{repo}/installation --jq .id': '12345',
      'gh api /repos/{owner}/{repo}/installation --jq .permissions': 'not valid json',
    });

    const exitCode = await validateSetup(logger, '/repo', spy.mock);

    expect(exitCode).toBe(1);
  });

  test('should verify all commands are called in order', async () => {
    const commands: string[] = [];
    const mockExeca = async (cmd: string | URL, args?: readonly string[]) => {
      const command = typeof cmd === 'string' ? cmd : cmd.toString();
      const key = [command, ...(args || [])].join(' ');
      commands.push(key);

      if (key === 'gh --version') {
        return { stdout: 'gh version 2.40.0', stderr: '', exitCode: 0 };
      }
      if (key === 'gh auth status') {
        return { stdout: 'Logged in', stderr: '', exitCode: 0 };
      }
      if (key === 'gh api /repos/{owner}/{repo}/installation --jq .id') {
        return { stdout: '12345', stderr: '', exitCode: 0 };
      }
      if (key === 'gh api /repos/{owner}/{repo}/installation --jq .permissions') {
        return {
          stdout: JSON.stringify({ contents: 'write', pull_requests: 'write' }),
          stderr: '',
          exitCode: 0,
        };
      }
      throw new Error(`Unexpected command: ${key}`);
    };

    process.env.DEP_UPDATER_APP_ID = '123456';
    process.env.DEP_UPDATER_APP_PRIVATE_KEY = 'test-key';

    await validateSetup(logger, '/repo', mockExeca);

    // Verify checks ran in expected order
    expect(commands[0]).toBe('gh --version');
    expect(commands[1]).toBe('gh auth status');
    expect(commands[2]).toBe('gh api /repos/{owner}/{repo}/installation --jq .id');
    expect(commands[3]).toBe('gh api /repos/{owner}/{repo}/installation --jq .permissions');

    // Clean up
    delete process.env.DEP_UPDATER_APP_ID;
    delete process.env.DEP_UPDATER_APP_PRIVATE_KEY;
  });
});
