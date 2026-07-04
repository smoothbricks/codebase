import { describe, expect, it } from 'bun:test';
import { LOCAL_SECTION_MARKER, splitLocalSectionForTest } from './managed-files.js';

const MANAGED = '# managed content\npath merge=driver\n';

describe('managed-file local sections', () => {
  it('content without a marker is entirely managed', () => {
    const { managed, localTail } = splitLocalSectionForTest(MANAGED);
    expect(managed).toBe(MANAGED);
    expect(localTail).toBe('');
  });

  it('everything from the marker onward is the repo-owned tail', () => {
    const tail = `${LOCAL_SECTION_MARKER}\ncustom/*.jsonl merge=custom-log\n`;
    const { managed, localTail } = splitLocalSectionForTest(`${MANAGED}\n${tail}`);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail).toBe(tail);
  });

  it('a tail directly after the managed content tolerates the separating newline', () => {
    // The compare rule accepts `managed === content + '\n'` when a tail exists,
    // so update → check round-trips as unchanged.
    const written = `${MANAGED}\n${LOCAL_SECTION_MARKER}\nextra\n`;
    const { managed, localTail } = splitLocalSectionForTest(written);
    expect(managed).toBe(`${MANAGED}\n`);
    expect(localTail.startsWith(LOCAL_SECTION_MARKER)).toBe(true);
  });
});
