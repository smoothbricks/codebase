import { expect, it } from 'bun:test';
import { describeLockFailure } from './locks.js';

it('keeps each lock operation, exact path, errno and OS cause in canonical refusal text', () => {
  for (const [operation, path, cause, expected] of [
    [
      'open',
      '/Target/Denied Lock',
      'errno 13: Permission denied',
      "open '/Target/Denied Lock': errno 13: permission denied",
    ],
    [
      'flock',
      '/Target/Unsupported',
      'errno 45: Operation not supported',
      "flock '/Target/Unsupported': errno 45: operation not supported",
    ],
  ] as const) {
    expect(describeLockFailure(operation, path, cause)).toBe(expected);
  }
});
