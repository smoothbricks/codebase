/** Insert new content after the closing fence of a named block */
export function insertAfterBlock(mdContent: string, afterBlockId: string, newBlock: string): string {
  const lines = mdContent.split('\n');
  const result: string[] = [];
  let insideTarget = false;
  let backtickCount = 0;
  let inserted = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';

    if (!insideTarget && !inserted) {
      const openMatch = line.match(/^(`{3,})\S*\s*\{[^}]*#(\w[\w-]*)[^}]*\}/);
      if (openMatch && openMatch[2] === afterBlockId) {
        insideTarget = true;
        backtickCount = (openMatch[1] ?? '```').length;
      }
      result.push(line);
      continue;
    }

    if (insideTarget) {
      const trimmed = line.trim();
      if (trimmed.length >= backtickCount && /^`+$/.test(trimmed)) {
        result.push(line);
        // WHY: insert the new block after the closing fence with a blank line separator
        result.push('');
        result.push(newBlock);
        insideTarget = false;
        inserted = true;
        continue;
      }
    }

    result.push(line);
  }

  return result.join('\n');
}
