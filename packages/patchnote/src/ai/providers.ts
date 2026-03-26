/**
 * AI provider configuration registry
 *
 * Maps each supported provider to its API URL, environment variable,
 * default model, and default token budget.
 */

import type { SupportedProvider } from '../types.js';

export interface ProviderConfig {
  /** API endpoint URL for chat completions */
  apiUrl: string;
  /** Environment variable name for the API key */
  envVar: string;
  /** Default model ID for this provider */
  defaultModel: string;
  /** Default token budget for changelog analysis prompts */
  defaultTokenBudget: number;
}

export const PROVIDER_CONFIGS: Record<SupportedProvider, ProviderConfig> = {
  zai: {
    apiUrl: 'https://api.z.ai/api/paas/v4/chat/completions',
    envVar: 'ZAI_API_KEY',
    defaultModel: 'glm-5-turbo',
    defaultTokenBudget: 64000,
  },
  gemini: {
    apiUrl: 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions',
    envVar: 'GEMINI_API_KEY',
    defaultModel: 'gemini-2.5-flash',
    defaultTokenBudget: 128000,
  },
};
