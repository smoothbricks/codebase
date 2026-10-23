import { describe, expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * Managed CI templates run on runners their owners do not control, so a `sudo` in one forces passwordless root onto
 * every self-hosted runner that uses it. A call stays only with a `sudo-allowed: <reason>` marker that says why no
 * unprivileged route exists and where the branch runs. The scan is a word match, not a shell parser: a mention of sudo
 * in a step name or a quoted message is flagged too and is cleared by rewording it.
 */
const MIN_REASON_LENGTH = 20;
// Not a word, a path part or a variable on either side; `sudo-allowed` is the marker itself.
const SUDO_COMMAND = /(?<![\w.$])sudo(?![\w.]|-allowed)/;
// A marker counts only inside a comment, so a string that merely says it cannot justify a call.
const MARKER = /(?:#|\/\/)\s*sudo-allowed:/;

const templatesRoot = fileURLToPath(new URL('../../managed/templates/', import.meta.url));

function isComment(line: string): boolean {
  const text = line.trimStart();
  return text.startsWith('#') || text.startsWith('//');
}

/** The reason text after a comment marker in `line`, or undefined when it has none. */
function markedReason(line: string): string | undefined {
  const found = MARKER.exec(line);
  return found ? line.slice(found.index + found[0].length) : undefined;
}

/** Line numbers (1-based) of `sudo` commands that carry no marker with a real reason. */
function unjustifiedSudoLines(text: string): number[] {
  const lines = text.split('\n');
  const found: number[] = [];
  lines.forEach((line, index) => {
    if (isComment(line) || !SUDO_COMMAND.test(line)) return;
    if (!hasJustification(lines, index)) found.push(index + 1);
  });
  return found;
}

function hasJustification(lines: string[], sudoIndex: number): boolean {
  const sameLine = markedReason(lines[sudoIndex] ?? '');
  if (sameLine !== undefined) return reasonLength(sameLine) >= MIN_REASON_LENGTH;
  // The contiguous comment block directly above. The whole reason sits on the marker's own line, so unrelated
  // comment text next to a bare marker cannot satisfy the length check.
  for (let index = sudoIndex - 1; index >= 0 && isComment(lines[index] ?? ''); index--) {
    const reason = markedReason(lines[index] ?? '');
    if (reason !== undefined) return reasonLength(reason) >= MIN_REASON_LENGTH;
  }
  return false;
}

function reasonLength(reason: string): number {
  return reason.replace(/\s+/g, ' ').trim().length;
}

function templateFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
    entry.isDirectory() ? templateFiles(join(dir, entry.name)) : [join(dir, entry.name)],
  );
}

describe('the scanner', () => {
  test('flags sudo in command position, however it is spelled or chained', () => {
    const text = [
      'sudo rm x',
      'a && sudo rm x',
      'a | sudo tee y',
      'if sudo -n true; then',
      'x=$(sudo id)',
      'run: "sudo id"',
      "run: 'sudo id'",
      '{sudo id;}',
      'run: /usr/bin/sudo -n true',
      "'sudo' -n true",
      'sudo</dev/null -n true',
      'SUDO=sudo; $SUDO rm x',
      '${SUDO:-sudo} rm x',
      'args: [sudo, -n, mount]',
      'sudo\\',
      'echo "sudo-allowed: a quoted string is no justification at all" && sudo rm x',
      "spawn('sudo', ['-n', 'id']);",
    ].join('\n');
    expect(unjustifiedSudoLines(text)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]);
  });

  test('ignores comments and words that merely contain sudo', () => {
    const text = [
      '# sudo rm x',
      '    # no sudo here',
      '// sudo rm x',
      'echo pseudo',
      'echo sudoku',
      'visudo -c',
      'echo $SUDO_USER',
    ].join('\n');
    expect(unjustifiedSudoLines(text)).toEqual([]);
  });

  test('accepts a marker with a real reason on the line or in the comment block above', () => {
    const reason = 'GitHub-hosted runners only, where passwordless sudo is the platform contract';
    const text = [
      `sudo rm x # sudo-allowed: ${reason}`,
      `# sudo-allowed: ${reason}`,
      'sudo rm y',
      `# sudo-allowed: ${reason}`,
      '# more about the step',
      'sudo rm z',
      `// sudo-allowed: ${reason}`,
      "spawn('sudo', ['id']);",
    ].join('\n');
    expect(unjustifiedSudoLines(text)).toEqual([]);
  });

  test('rejects a short or missing reason, a marker behind a code line, and a marker after the call', () => {
    const reason = 'GitHub-hosted runners only, where passwordless sudo is the platform contract';
    const text = [
      '# sudo-allowed: because',
      'sudo rm a',
      '# sudo-allowed:',
      '# Single-user Nix unpacked into /nix in seconds: no installer volume, no daemon',
      'sudo rm b',
      `# sudo-allowed: ${reason}`,
      'echo between',
      'sudo rm c',
      'sudo rm d',
      `# sudo-allowed: ${reason}`,
    ].join('\n');
    expect(unjustifiedSudoLines(text)).toEqual([2, 5, 8, 9]);
  });
});

describe('managed templates', () => {
  test('scan at least the setup action', () => {
    const files = templateFiles(templatesRoot);
    expect(files.some((file) => file.endsWith('/setup-devenv/action.yml'))).toBe(true);
  });

  test('call sudo only with a justified sudo-allowed marker', () => {
    const offenders = templateFiles(templatesRoot).flatMap((file) =>
      unjustifiedSudoLines(readFileSync(file, 'utf8')).map((line) => `${file.slice(templatesRoot.length)}:${line}`),
    );
    // Remove the sudo, or add `# sudo-allowed: <why no unprivileged route exists, where the branch runs>` above it.
    expect(offenders).toEqual([]);
  });
});
