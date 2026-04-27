/**
 * Template engine for PR body rendering.
 * Replaces {{variable}} placeholders with provided values.
 */

/**
 * Render a template string by replacing {{variable}} placeholders.
 * - Known variables are replaced with their string value.
 * - Undefined/null values become empty string.
 * - Unknown variables (not in the record) are left untouched.
 *
 * @param template - Template string with {{variable}} placeholders
 * @param variables - Map of variable names to values
 * @returns Rendered string
 */
export function renderTemplate(template: string, variables: Record<string, string | number | undefined>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (match, key) => {
    if (key in variables) {
      const value = variables[key];
      return value != null ? String(value) : '';
    }
    return match;
  });
}

/**
 * Collapse 3+ consecutive newlines to exactly 2, then trim leading/trailing whitespace.
 * This cleans up template output when some variables resolve to empty strings.
 *
 * @param text - Text to clean up
 * @returns Cleaned text
 */
export function collapseBlankLines(text: string): string {
  return text.replace(/\n{3,}/g, '\n\n').trim();
}
