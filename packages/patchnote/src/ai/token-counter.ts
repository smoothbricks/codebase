/**
 * Token counting utilities using gpt-tokenizer
 *
 * Uses GPT-4o tokenizer which provides ~85-99% accuracy across all major providers.
 * This is much more accurate than character-based estimation (chars / 4).
 */

import { encode } from 'gpt-tokenizer/model/gpt-4o';

/**
 * Count the number of tokens in a string
 *
 * @param content - The text to count tokens for
 * @returns The number of tokens
 */
export function countTokens(content: string): number {
  try {
    return encode(content).length;
  } catch {
    // Fallback to character estimate (~4 chars per token) if tokenizer fails
    return Math.ceil(content.length / 4);
  }
}

/**
 * Check if content exceeds a token limit
 *
 * @param content - The text to check
 * @param maxTokens - Maximum allowed tokens
 * @returns true if content exceeds the limit
 */
export function exceedsTokenLimit(content: string, maxTokens: number): boolean {
  return countTokens(content) > maxTokens;
}
