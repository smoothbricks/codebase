import { describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { MINIMUM_SIBLING_NAME_LENGTH, siblingTerm } from './public-denylist.js';

const LETTERS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const DIGITS = '0123456789';
// Every punctuation character a directory name may carry that a regex interprets.
const REGEX_PUNCTUATION = '._-+()[]{}^$|*?\\';
const NAME_CHARACTERS = [LETTERS, DIGITS, REGEX_PUNCTUATION].join('');
// Beyond ASCII, one of them beyond the Basic Multilingual Plane (two UTF-16 units): a name's length counts characters.
const WIDE_CHARACTERS = 'é项🙂';

const text = (alphabet: string, bounds: { minLength?: number; maxLength: number }) =>
  fc.array(fc.constantFrom(...alphabet), bounds).map((characters) => characters.join(''));

const digitsOnly = text(DIGITS, { minLength: 1, maxLength: 40 });
const tooShort = text([NAME_CHARACTERS, WIDE_CHARACTERS].join(''), { maxLength: MINIMUM_SIBLING_NAME_LENGTH - 1 });
// A project's name: at least the minimum length and a letter first, so never only digits.
const projectName = fc
  .tuple(
    fc.constantFrom(...LETTERS),
    text(NAME_CHARACTERS, { minLength: MINIMUM_SIBLING_NAME_LENGTH - 1, maxLength: 30 }),
  )
  .map(([head, tail]) => `${head}${tail}`);

describe('sibling term derivation', () => {
  it('derives no term from a name of only digits, whatever its length', () => {
    fc.assert(fc.property(digitsOnly, (name) => siblingTerm(name) === null));
  });

  it('derives no term from a name of fewer characters than the minimum, whatever they are', () => {
    fc.assert(fc.property(tooShort, (name) => siblingTerm(name) === null));
  });

  it('derives a literal, case-insensitive whole-word term from every other name', () => {
    fc.assert(
      fc.property(projectName, (name) => {
        const term = siblingTerm(name);
        if (term === null) throw new Error(`no term derived from ${name}`);
        const pattern = new RegExp(term, 'i');
        expect(pattern.test(`see ${name}, ok`)).toBe(true);
        expect(pattern.test(name.toUpperCase())).toBe(true);
        expect(pattern.test(`_${name}`)).toBe(false);
        expect(pattern.test(`${name}_`)).toBe(false);
      }),
    );
  });

  it('keeps the shortest allowed name and every name that mixes digits with other characters', () => {
    expect(siblingTerm('abc')).not.toBeNull();
    expect(siblingTerm('🙂🙂🙂')).not.toBeNull();
    expect(siblingTerm('111a')).not.toBeNull();
    expect(siblingTerm('2026-10-10')).not.toBeNull();
    expect(siblingTerm('1.0.1')).not.toBeNull();
  });

  it('drops a name of only digits and a name of one or two characters', () => {
    expect(siblingTerm('111')).toBeNull();
    expect(siblingTerm('2026')).toBeNull();
    expect(siblingTerm('ab')).toBeNull();
    expect(siblingTerm('1a')).toBeNull();
    expect(siblingTerm('🙂🙂')).toBeNull();
    expect(siblingTerm('a')).toBeNull();
    expect(siblingTerm('')).toBeNull();
  });
});
