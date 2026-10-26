import { describe, expect, it } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { checkWaitWhy, unexplainedWaitAllows, WAIT_METHODS } from './wait-why-policy.js';

const allow = '#[allow(clippy::disallowed_methods)]';

describe('WAIT-WHY', () => {
  it('refuses a suppression covering a wait with no reason beside it', () => {
    for (const source of [
      `fn reap(child: &mut Child) {\n    ${allow}\n    child.wait().unwrap();\n}`,
      `${allow}\nfn run(command: &mut Command) -> Output {\n    command.output_locked().unwrap()\n}`,
      `#[expect(clippy::disallowed_methods, reason = "fine")]\nfn lock(fd: i32) { unsafe { libc::flock(fd, 2) }; }`,
      '#![allow(dead_code, clippy::disallowed_methods)]\nfn hold(path: &Path) { UnixStream::connect(path); }',
      '#[cfg_attr(test, allow(clippy::disallowed_methods))]\nfn reap(pid: i32) { libc::waitpid(pid, null_mut(), 0); }',
      `${allow}\nlet output = Command::new("git").arg("status").output()?;`,
      `// WHY unbounded:\n${allow}\nlet status = child.wait();`,
    ]) {
      expect(unexplainedWaitAllows(source), source).toEqual([
        source.split('\n').findIndex((line) => line.includes('#')) + 1,
      ]);
    }
  });

  it('admits a kept wait whose reason is above, inside, after or on the line of its attribute', () => {
    for (const source of [
      `// The child exited: its status is collected at once.\n// WHY unbounded: it has exited.\n${allow}\nlet status = child.wait();`,
      `/// Doc.\n// WHY unbounded: the operator's interrupt ends it.\n#[inline]\n${allow}\nfn run(c: &mut Command) { c.status_locked(); }`,
      `${allow} // WHY unbounded: LOCK_NB never waits.\nunsafe { libc::flock(fd, LOCK_EX | LOCK_NB) };`,
      `${allow}\n// WHY unbounded: the test runner's no-progress bound ends a stalled test.\nmod tests { fn f() { child.wait(); } }`,
      '#[allow(\n    // WHY unbounded: tokio reaps it once the pipes close.\n    clippy::disallowed_methods\n)]\nlet output = child.wait_with_output().await;',
    ]) {
      expect(unexplainedWaitAllows(source), source).toEqual([]);
    }
  });

  it('keeps the fork lock its own reasons: a suppression covering no wait needs no WHY', () => {
    for (const source of [
      `#[expect(\n    clippy::disallowed_methods,\n    reason = "the one std spawn, under the fork lock"\n)]\nfn spawn_locked(&mut self) -> io::Result<Child> {\n    let _spawning = spawning();\n    self.spawn()\n}`,
      `${allow}\nlet child = command.spawn();\nchild.wait();`,
      `fn f() {\n    ${allow}\n    command.spawn()\n}\nfn g() { child.wait(); }`,
      `// child.wait() in a comment calls nothing\n${allow}\nlet text = "flock";`,
      `${allow}\nlet status = unsafe { libc::copyfile(from, to, state, flags) };\nif status == 0 { Ok(()) } else { Err(output) }`,
    ]) {
      expect(unexplainedWaitAllows(source), source).toEqual([]);
    }
  });

  it('covers exactly the statement or item the attribute is on', () => {
    const source = [
      'fn f() {',
      `    ${allow}`,
      '    let reaped = if ready { child.wait() } else { Ok(status) };',
      `    ${allow}`,
      '    let spawned = command.spawn().map(|child| child).unwrap_or_else(|_| fallback());',
      '    other.wait();',
      '}',
    ].join('\n');
    expect(unexplainedWaitAllows(source)).toEqual([2]);
  });

  it('refuses an enrolled crate whose clippy.toml lacks a wait, and an unexplained allow in it', () => {
    const crates = mkdtempSync(join(tmpdir(), 'wait-why-'));
    try {
      const entries = (methods: readonly string[]) =>
        `disallowed-methods = [\n${methods.map((path) => `  { path = "${path}", reason = "bound it" },`).join('\n')}\n]\n`;
      mkdirSync(join(crates, 'kept/src'), { recursive: true });
      writeFileSync(join(crates, 'kept/clippy.toml'), entries(WAIT_METHODS));
      writeFileSync(
        join(crates, 'kept/src/lib.rs'),
        `// WHY unbounded: it has exited.\n${allow}\nfn f() { child.wait(); }\n`,
      );
      mkdirSync(join(crates, 'drifted/src'), { recursive: true });
      writeFileSync(
        join(crates, 'drifted/clippy.toml'),
        entries(WAIT_METHODS.filter((path) => path !== 'libc::flock')),
      );
      writeFileSync(join(crates, 'drifted/src/lib.rs'), `${allow}\nfn f() { child.wait(); }\n`);
      mkdirSync(join(crates, 'unenrolled/src'), { recursive: true });
      writeFileSync(join(crates, 'unenrolled/src/lib.rs'), `${allow}\nfn f() { child.wait(); }\n`);
      expect(checkWaitWhy(crates).map(({ path, line }) => `${path}:${line}`)).toEqual([
        'drifted/clippy.toml:1',
        'drifted/src/lib.rs:1',
      ]);
    } finally {
      rmSync(crates, { recursive: true, force: true });
    }
  });
});
