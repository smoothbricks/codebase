/**
 * Multi-provider AI client for changelog analysis
 *
 * Supports Z.AI and Google Gemini via their OpenAI-compatible chat completion APIs.
 * Provider is resolved automatically: configured provider first, then fallback to
 * any provider with an available API key.
 */

import type { PatchnoteConfig } from '../config.js';
import type { Logger } from '../logger.js';
import type { SendPromptOptions, SupportedProvider } from '../types.js';
import { PROVIDER_CONFIGS } from './providers.js';

const DEFAULT_MAX_TOKENS = 4096;

/**
 * Resolve which AI provider and API key to use.
 *
 * Resolution order:
 * 1. Explicit config.ai.apiKey -> use configured provider
 * 2. Configured provider's env var -> use configured provider
 * 3. Fallback: check other providers' env vars -> use first found
 * 4. Return null if no keys available
 */
export function resolveProvider(config: PatchnoteConfig): { provider: SupportedProvider; apiKey: string } | null {
  const configuredProvider = config.ai.provider;
  const providerCfg = PROVIDER_CONFIGS[configuredProvider];

  // 1. Explicit config key
  if (config.ai.apiKey) {
    return { provider: configuredProvider, apiKey: config.ai.apiKey };
  }

  // 2. Configured provider's env var
  const envKey = process.env[providerCfg.envVar];
  if (envKey) {
    return { provider: configuredProvider, apiKey: envKey };
  }

  // 3. Fallback: try other providers
  for (const [name, cfg] of Object.entries(PROVIDER_CONFIGS)) {
    if (name === configuredProvider) continue;
    const key = process.env[cfg.envVar];
    if (key) return { provider: name as SupportedProvider, apiKey: key };
  }

  // 4. No keys available
  return null;
}

/**
 * Get the token budget for changelog analysis, respecting both
 * the user config override and the resolved provider's default.
 */
export function getProviderTokenBudget(config: PatchnoteConfig): number {
  if (config.ai.tokenBudget) {
    return config.ai.tokenBudget;
  }

  const resolved = resolveProvider(config);
  const provider = resolved?.provider ?? config.ai.provider;
  return PROVIDER_CONFIGS[provider].defaultTokenBudget;
}

/**
 * Send a prompt to the resolved AI provider and get a text response.
 *
 * Resolves the provider automatically. Throws if no API key is available.
 */
export async function sendPrompt(
  config: PatchnoteConfig,
  prompt: string,
  options?: SendPromptOptions,
): Promise<string> {
  const resolved = resolveProvider(config);
  if (!resolved) {
    const configuredCfg = PROVIDER_CONFIGS[config.ai.provider];
    throw new Error(`No AI API key found. Set ${configuredCfg.envVar} environment variable or config.ai.apiKey.`);
  }

  const { provider, apiKey } = resolved;
  const providerCfg = PROVIDER_CONFIGS[provider];
  const model = options?.model || config.ai.model || providerCfg.defaultModel;

  config.logger?.debug?.(`Sending prompt to ${provider} ${model}`);

  const body = {
    model,
    messages: [{ role: 'user', content: prompt }],
    max_tokens: DEFAULT_MAX_TOKENS,
    temperature: 0.6,
  };

  const response = await fetch(providerCfg.apiUrl, {
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
    throw new Error(`${provider} API error ${response.status}: ${errorText}`);
  }

  const data = (await response.json()) as {
    choices?: Array<{ message?: { content?: string } }>;
  };

  const content = data.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error(`No content in ${provider} response`);
  }

  config.logger?.debug?.(`Got response from ${provider} (${content.length} chars)`);
  return content.trim();
}

/**
 * No-op shutdown (no persistent server -- stateless HTTP calls)
 */
export async function shutdownAIClient(_logger?: Logger) {
  // Nothing to clean up - stateless HTTP calls
}
