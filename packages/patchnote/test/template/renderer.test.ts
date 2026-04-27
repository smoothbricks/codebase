import { describe, expect, test } from 'bun:test';
import { DEFAULT_PR_BODY_TEMPLATE } from '../../src/template/defaults.js';
import { collapseBlankLines, renderTemplate } from '../../src/template/renderer.js';

describe('renderTemplate', () => {
  test('replaces known variables', () => {
    expect(renderTemplate('Hello {{name}}', { name: 'World' })).toBe('Hello World');
  });

  test('preserves unknown variables', () => {
    expect(renderTemplate('{{a}} {{b}}', { a: 'x' })).toBe('x {{b}}');
  });

  test('replaces undefined variable with empty string', () => {
    expect(renderTemplate('{{a}}', { a: undefined })).toBe('');
  });

  test('replaces empty string variable with empty string', () => {
    expect(renderTemplate('{{a}}', { a: '' })).toBe('');
  });

  test('converts number variable to string', () => {
    expect(renderTemplate('{{count}} items', { count: 42 })).toBe('42 items');
  });

  test('passes through template with no variables', () => {
    expect(renderTemplate('no vars here', {})).toBe('no vars here');
  });

  test('replaces multiple occurrences of same variable', () => {
    expect(renderTemplate('{{x}} and {{x}}', { x: 'y' })).toBe('y and y');
  });
});

describe('collapseBlankLines', () => {
  test('collapses 3+ consecutive newlines to exactly 2', () => {
    expect(collapseBlankLines('a\n\n\n\nb')).toBe('a\n\nb');
  });

  test('trims leading and trailing whitespace', () => {
    expect(collapseBlankLines('\n\na\n\n')).toBe('a');
  });

  test('preserves two consecutive newlines', () => {
    expect(collapseBlankLines('a\n\nb')).toBe('a\n\nb');
  });

  test('preserves single newlines', () => {
    expect(collapseBlankLines('a\nb')).toBe('a\nb');
  });
});

describe('integration: renderTemplate + collapseBlankLines', () => {
  test('empty variables produce clean output without triple-blank-lines', () => {
    const template = '{{a}}\n\n{{b}}\n\n{{c}}';
    const rendered = renderTemplate(template, { a: 'hello', b: '', c: 'world' });
    const result = collapseBlankLines(rendered);
    expect(result).toBe('hello\n\nworld');
    expect(result).not.toContain('\n\n\n');
  });
});

describe('DEFAULT_PR_BODY_TEMPLATE', () => {
  test('contains all expected variable placeholders', () => {
    const expected = [
      '{{provenanceWarnings}}',
      '{{aiSummary}}',
      '{{table}}',
      '{{nixUpdates}}',
      '{{downgrades}}',
      '{{releaseNotes}}',
      '{{deprecationWarnings}}',
    ];
    for (const placeholder of expected) {
      expect(DEFAULT_PR_BODY_TEMPLATE).toContain(placeholder);
    }
  });
});
