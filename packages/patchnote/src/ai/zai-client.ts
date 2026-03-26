/**
 * Z.AI GLM-5-Turbo client for changelog analysis
 *
 * Uses the OpenAI-compatible API at https://api.z.ai/api/paas/v4/chat/completions
 * Requires ZAI_API_KEY environment variable.
 */

import type { PatchnoteConfig } from '../config.js';
import type { Logger } from '../logger.js';
import type { SendPromptOptions } from '../types.js';

const ZAI_API_URL = 'https://api.z.ai/api/paas/v4/chat/completions';
const DEFAULT_MODEL = 'glm-5-turbo';
const DEFAULT_MAX_TOKENS = 4096;

/**
 * Send a prompt to Z.AI GLM-5-Turbo and get a text response
 */
export async function sendPrompt(
  config: PatchnoteConfig,
  prompt: string,
  options?: SendPromptOptions,
): Promise<string> {
  const apiKey = config.ai.apiKey || process.env.ZAI_API_KEY;
  if (!apiKey) {
    throw new Error('No Z.AI API key found. Set ZAI_API_KEY environment variable or config.ai.apiKey.');
  }

  const model = options?.model || config.ai.model || DEFAULT_MODEL;

  config.logger?.debug?.(`Sending prompt to Z.AI ${model}`);

  const body = {
    model,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: DEFAULT_MAX_TOKENS,
    temperature: 0.6,
  };

  const response = await fetch(ZAI_API_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60_000),
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'unknown error');
    throw new Error(`Z.AI API error ${response.status}: ${errorText}`);
  }

  const data = (await response.json()) as {
    choices?: Array<{ message?: { content?: string } }>;
  };

  const content = data.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error('No content in Z.AI response');
  }

  config.logger?.debug?.(`Got response (${content.length} chars)`);
  return content.trim();
}

/**
 * No-op shutdown (no persistent server like opencode had)
 */
export async function shutdownAIClient(_logger?: Logger) {
  // Nothing to clean up - stateless HTTP calls
}
