/**
 * Backward compatibility re-export
 *
 * The AI client has been refactored into a multi-provider client at ./ai-client.ts.
 * This file re-exports the same interface for any existing consumers.
 *
 * Prefer importing from './ai-client.js' for new code.
 */
export { sendPrompt, shutdownAIClient } from './ai-client.js';
