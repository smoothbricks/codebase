/**
 * Test helpers for mocking execa command execution
 */

/**
 * Create a mock execa function with predefined responses
 *
 * @param responses - Map of command strings to stdout responses
 * @returns Mock execa function
 *
 * @example
 * ```typescript
 * const mockExeca = createMockExeca({
 *   'git rev-parse --abbrev-ref HEAD': 'main\n',
 *   'git status --porcelain': '',
 * });
 *
 * const branch = await getCurrentBranch('/repo', mockExeca);
 * expect(branch).toBe('main');
 * ```
 */
export function createMockExeca(responses: Record<string, string>) {
  return async (cmd: string | URL, args?: readonly string[], _opts?: Record<string, any>) => {
    const command = typeof cmd === 'string' ? cmd : cmd.toString();
    const key = [command, ...(args || [])].join(' ');
    if (key in responses) {
      return { stdout: responses[key], stderr: '', exitCode: 0 };
    }
    throw new Error(`Unexpected command: ${key}\nAvailable commands:\n${Object.keys(responses).join('\n')}`);
  };
}

/**
 * Create a mock execa function that throws an error
 *
 * @param errorMessage - Error message to throw
 * @returns Mock execa function that always throws
 *
 * @example
 * ```typescript
 * const mockExeca = createErrorExeca('not a git repository');
 * await expect(getCurrentBranch('/repo', mockExeca)).rejects.toThrow('not a git repository');
 * ```
 */
export function createErrorExeca(errorMessage: string) {
  return async (_cmd: string | URL, _args?: readonly string[], _opts?: Record<string, any>) => {
    const error: any = new Error(errorMessage);
    error.exitCode = 1;
    error.stderr = errorMessage;
    throw error;
  };
}

/**
 * Create a spy that tracks calls and returns predefined responses
 *
 * @param responses - Map of command strings to stdout responses
 * @returns Object with mock function and spy utilities
 *
 * @example
 * ```typescript
 * const spy = createExecaSpy({
 *   'git add -A': '',
 *   'git commit -m message': '',
 * });
 *
 * await createUpdateCommit({ repoRoot: '/repo' }, 'message', undefined, spy.mock);
 *
 * expect(spy.calls).toHaveLength(2);
 * expect(spy.calls[0]).toEqual(['git', ['add', '-A']]);
 * ```
 */
export function createExecaSpy(responses: Record<string, string>) {
  const calls: Array<[string | URL, readonly string[] | undefined, Record<string, any> | undefined]> = [];

  const mock = async (cmd: string | URL, args?: readonly string[], opts?: Record<string, any>) => {
    calls.push([cmd, args, opts]);
    const command = typeof cmd === 'string' ? cmd : cmd.toString();
    const key = [command, ...(args || [])].join(' ');
    if (key in responses) {
      return { stdout: responses[key], stderr: '', exitCode: 0 };
    }
    throw new Error(`Unexpected command: ${key}`);
  };

  return {
    mock,
    calls,
    reset: () => {
      calls.length = 0;
    },
    getCallsFor: (command: string) => {
      return calls.filter(([cmd]) => {
        const cmdString = typeof cmd === 'string' ? cmd : cmd.toString();
        return cmdString === command;
      });
    },
  };
}
