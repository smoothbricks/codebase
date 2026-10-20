import { describe, expect, it } from 'bun:test';
import { createRunnerLifecycle, withRunnerLifecycle } from './runner-lifecycle.js';

const event = (id: number, phase: string): string => JSON.stringify({ id, phase });

describe('private runner lifecycle', () => {
  it('bounds startup and active execution, never build intervals, through the final owned close', () => {
    const lifecycle = createRunnerLifecycle(2);
    expect(lifecycle.isTesting()).toBe(true);
    lifecycle.accept(event(0, 'build'));
    expect(lifecycle.isTesting()).toBe(false);
    lifecycle.accept(event(0, 'active'));
    expect(lifecycle.isTesting()).toBe(true);
    lifecycle.accept(event(0, 'build'));
    expect(lifecycle.isTesting()).toBe(false);
    lifecycle.accept(event(0, 'active'));
    lifecycle.accept(event(0, 'terminal'));
    expect(lifecycle.isTesting()).toBe(true);
    lifecycle.accept(event(1, 'build'));
    expect(lifecycle.isTesting()).toBe(false);
    lifecycle.accept(event(1, 'active'));
    expect(lifecycle.isTesting()).toBe(true);
    lifecycle.accept(event(1, 'terminal'));
    expect(lifecycle.isTesting()).toBe(false);
    expect(lifecycle.error()).toBeNull();
  });

  it('allows shell short-circuiting but refuses overlapping or resurrected invocations', () => {
    const lifecycle = createRunnerLifecycle(3);
    lifecycle.accept(event(0, 'build'));
    lifecycle.accept(event(0, 'terminal'));
    expect(lifecycle.accept(event(2, 'build'))).toEqual({ id: 2, phase: 'build' });
    expect(lifecycle.accept(event(1, 'build'))).toBeNull();
    expect(lifecycle.isTesting()).toBe(true);
    expect(lifecycle.error()).not.toBeNull();
    for (const lines of [
      [event(0, 'terminal'), event(0, 'build')],
      [event(0, 'build'), event(1, 'build')],
      ['test result: ok'],
      [event(0, 'heartbeat')],
    ]) {
      const invalid = createRunnerLifecycle(2);
      for (const line of lines) invalid.accept(line);
      expect(invalid.error()).not.toBeNull();
      expect(invalid.isTesting()).toBe(true);
    }
  });
});

describe('native runner hooks', () => {
  it('preserves runner selection, toolchains, separators, and harness arguments', () => {
    const hooked = withRunnerLifecycle(
      'cargo +stable --frozen test --message-format=human --lib selected -- --nocapture && go test -json ./second',
    );
    expect(hooked?.runners).toBe(2);
    expect(hooked?.command).toContain(
      '0 cargo cargo +stable --frozen test --message-format=json-render-diagnostics --lib selected -- --nocapture && ',
    );
    expect(hooked?.command).toContain('1 go go test -json ./second');
    expect(withRunnerLifecycle('cargo test a || cargo test b; go test -json ./...')?.runners).toBe(3);
    expect(withRunnerLifecycle('cargo test -- --message-format=human')?.command).toContain(
      'cargo test --message-format=json-render-diagnostics -- --message-format=human',
    );
  });

  it('retains spawn accounting for Bun, nextest, mixed wrappers and unknown shell syntax', () => {
    for (const command of [
      'bun test src',
      'cargo nextest run',
      'cargo build && cargo test',
      'go test ./... && bun test src',
      'env FOO=bar cargo test',
      'cargo test | cat',
      'cargo test > output',
      'cargo test 3>&1',
      'cargo test &&',
      'cargo test\ngo test',
      'sh -c "cargo test"',
    ])
      expect(withRunnerLifecycle(command)).toBeNull();
  });
});
