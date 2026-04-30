import { describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import {
  formatProjectTargetLines,
  nxCacheDirectories,
  nxResetCommand,
  nxShowProjectCommand,
  projectNamesFromNxShowProjectsOutput,
  projectNamesWithTarget,
  targetNamesFromNxProjectJson,
} from './index.js';

describe('Nx helper command construction', () => {
  it('builds explicit nx reset invocation', () => {
    expect(nxResetCommand()).toEqual({ command: 'nx', args: ['reset'] });
  });

  it('builds explicit nx project metadata invocation', () => {
    expect(nxShowProjectCommand('cli')).toEqual({ command: 'nx', args: ['show', 'project', 'cli', '--json'] });
  });

  it('selects only local Nx cache directories', () => {
    expect(nxCacheDirectories('/repo')).toEqual([
      join('/repo', '.nx/cache'),
      join('/repo', '.nx/workspace-data'),
      join('/repo', 'node_modules/.cache/nx'),
    ]);
  });
});

describe('Nx helper output formatting', () => {
  it('formats project target pairs like the old root helper script', () => {
    expect(
      formatProjectTargetLines([
        { project: 'web', targets: ['test', 'build'] },
        { project: 'cli', targets: ['lint'] },
      ]),
    ).toBe(['cli:lint', 'web:build', 'web:test'].join('\n'));
  });

  it('lists projects that define the requested target', () => {
    expect(
      projectNamesWithTarget(
        [
          { project: 'web', targets: ['build', 'test'] },
          { project: 'cli', targets: ['build', 'lint'] },
          { project: 'docs', targets: ['serve'] },
        ],
        'build',
      ),
    ).toEqual(['cli', 'web']);
  });

  it('parses JSON project lists from nx show projects', () => {
    expect(projectNamesFromNxShowProjectsOutput('["web","cli"]\n')).toEqual(['cli', 'web']);
  });

  it('parses legacy newline project lists from nx show projects', () => {
    expect(projectNamesFromNxShowProjectsOutput('web\ncli\n')).toEqual(['cli', 'web']);
  });

  it('extracts sorted target names from Nx project JSON', () => {
    expect(targetNamesFromNxProjectJson({ targets: { test: {}, build: {}, lint: {} } })).toEqual([
      'build',
      'lint',
      'test',
    ]);
  });

  it('treats missing target metadata as an empty project', () => {
    expect(targetNamesFromNxProjectJson({ name: 'cli' })).toEqual([]);
  });
});
