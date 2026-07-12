import type { AnySpanBuffer } from './types.js';

/** Resolve one message row from its private compile-time template ID lane. */
export function resolveMessage(buffer: AnySpanBuffer, row: number): string | undefined {
  const templateId = buffer._messageTemplateIds?.[row] ?? 0;
  if (templateId === 0) {
    return buffer.message_values[row];
  }

  const template = buffer._opMetadata.logTemplateIds[templateId - 1];
  if (template === undefined) {
    throw new Error(`Invalid message template ID ${templateId} at row ${row}`);
  }
  return template;
}
