import { afterEach, describe, expect, it } from 'bun:test';
import { reportFatal } from './cli.js';

const originalConsoleError = console.error;

afterEach(() => {
  console.error = originalConsoleError;
});

describe('CLI fatal error reporting', () => {
  it('passes structured thrown values to the console without coercion', () => {
    const output: unknown[][] = [];
    console.error = (...args: unknown[]) => {
      output.push(args);
    };
    const failure = {
      message: 'Failed to load Nx plugins',
      errors: [{ message: 'router plugin received an invalid path' }],
    };

    reportFatal(failure);

    expect(output).toEqual([[failure]]);
  });
});
