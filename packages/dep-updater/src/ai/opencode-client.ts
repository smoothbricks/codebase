/**
 * OpenCode SDK client wrapper for multi-provider AI support
 *
 * Uses SST's OpenCode SDK (github.com/sst/opencode) to support multiple AI providers:
 * - opencode: Free tier using big-pickle model (requires `opencode auth login`)
 * - anthropic: Claude models (requires ANTHROPIC_API_KEY)
 * - openai: GPT models (requires OPENAI_API_KEY)
 * - google: Gemini models (requires GOOGLE_API_KEY)
 *
 * Note: The OpenCode SDK spawns a local server. Call shutdownOpenCodeClient()
 * when done to allow the process to exit cleanly.
 */

import { createOpencode } from '@opencode-ai/sdk';
import type { DepUpdaterConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { type SendPromptOptions, SUPPORTED_PROVIDERS, type SupportedProvider } from '../types.js';

/** Singleton client and server instances */
let clientInstance: Awaited<ReturnType<typeof createOpencode>>['client'] | null = null;
let serverInstance: Awaited<ReturnType<typeof createOpencode>>['server'] | null = null;

/** Default models per provider */
const DEFAULT_MODELS: Record<SupportedProvider, string> = {
  opencode: 'big-pickle', // Free model via OpenCode (requires `opencode auth login`)
  anthropic: 'claude-sonnet-4-5-20250929',
  openai: 'gpt-4o',
  google: 'gemini-1.5-pro',
};

/** Environment variable names per provider (empty string = no key needed) */
export const PROVIDER_ENV_VARS: Record<SupportedProvider, string> = {
  opencode: '', // No API key required (uses `opencode auth login`)
  anthropic: 'ANTHROPIC_API_KEY',
  openai: 'OPENAI_API_KEY',
  google: 'GOOGLE_API_KEY',
};

/**
 * Format SDK error into a human-readable message
 */
function formatSDKError(error: unknown, provider?: string): string {
  if (!error || typeof error !== 'object') {
    return 'Unknown error';
  }

  // Handle named errors (ProviderAuthError, NotFoundError, APIError, etc.)
  if ('name' in error && 'data' in error) {
    const e = error as { name: string; data: Record<string, unknown> };
    const message = e.data?.message ? String(e.data.message) : e.name;

    // Add helpful hint for auth errors
    if (e.name === 'ProviderAuthError' && provider) {
      const envVar = PROVIDER_ENV_VARS[provider as SupportedProvider] || `${provider.toUpperCase()}_API_KEY`;
      return `${message}. Check that ${envVar} is set correctly.`;
    }

    return message;
  }

  // Handle BadRequestError: { errors: [...], success: false }
  if ('errors' in error && Array.isArray((error as { errors: unknown }).errors)) {
    const errors = (error as { errors: Array<{ message?: string }> }).errors;
    const messages = errors.map((e) => e.message || JSON.stringify(e)).filter(Boolean);
    return messages.length > 0 ? messages.join('; ') : 'Bad request';
  }

  // Fallback to JSON
  return JSON.stringify(error);
}

/**
 * Get or create OpenCode client instance
 * Reuses client across calls for efficiency
 */
export async function getOpenCodeClient(logger?: Logger) {
  if (!clientInstance) {
    try {
      logger?.debug?.('Initializing OpenCode client...');
      const { client, server } = await createOpencode();
      clientInstance = client;
      serverInstance = server;
      logger?.debug?.('OpenCode client initialized');
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      logger?.error?.(`Failed to initialize OpenCode client: ${msg}`);
      throw new Error(`OpenCode SDK initialization failed: ${msg}`);
    }
  }
  return clientInstance;
}

/**
 * Reset the client instance (useful for testing)
 */
export function resetOpenCodeClient() {
  clientInstance = null;
  serverInstance = null;
}

/**
 * Shutdown the OpenCode server gracefully
 * Call this when done using the AI to allow the process to exit cleanly
 */
export async function shutdownOpenCodeClient(logger?: Logger) {
  if (serverInstance) {
    try {
      logger?.debug?.('Shutting down OpenCode server...');
      serverInstance.close();
      logger?.debug?.('OpenCode server shut down');
    } catch (error) {
      // Ignore shutdown errors - best effort only
      logger?.debug?.(`OpenCode shutdown warning: ${error instanceof Error ? error.message : String(error)}`);
    }
    serverInstance = null;
    clientInstance = null;
  }
}

// Re-export for backwards compatibility
export type { SendPromptOptions } from '../types.js';

/**
 * Send a prompt to the AI and get a text response
 *
 * The OpenCode SDK's session.prompt() is synchronous - it waits for the AI response
 * and returns it directly in the result.
 *
 * @param config - dep-updater configuration
 * @param prompt - The prompt to send
 * @param options - Optional overrides for model, provider
 * @returns The AI's text response
 */
export async function sendPrompt(
  config: DepUpdaterConfig,
  prompt: string,
  options?: SendPromptOptions,
): Promise<string> {
  const provider = options?.provider || config.ai.provider;

  // Validate provider before making any API calls
  if (!SUPPORTED_PROVIDERS.includes(provider as SupportedProvider)) {
    throw new Error(`Unsupported AI provider: '${provider}'. Supported providers: ${SUPPORTED_PROVIDERS.join(', ')}`);
  }

  const client = await getOpenCodeClient(config.logger);
  const model = options?.model || config.ai.model || getDefaultModel(provider);

  config.logger?.debug?.(`Sending prompt to ${provider}/${model}`);

  // Create ephemeral session for this request
  const sessionResult = await client.session.create({
    body: { title: `dep-updater-${Date.now()}` },
  });

  if (sessionResult.error || !sessionResult.data) {
    throw new Error(`Failed to create session: ${formatSDKError(sessionResult.error)}`);
  }

  const sessionId = sessionResult.data.id;

  try {
    // Send the prompt - returns the full AI response synchronously
    const promptResult = await client.session.prompt({
      path: { id: sessionId },
      body: {
        model: {
          providerID: provider,
          modelID: model,
        },
        parts: [{ type: 'text', text: prompt }],
      },
    });

    if (promptResult.error) {
      throw new Error(`Prompt failed: ${formatSDKError(promptResult.error, provider)}`);
    }

    // Extract text from the response parts
    const responseData = promptResult.data as {
      info?: { error?: unknown };
      parts?: Array<{ type: string; text?: string }>;
    };

    // Check for errors in the response
    if (responseData?.info?.error) {
      throw new Error(`AI response error: ${formatSDKError(responseData.info.error)}`);
    }

    // Extract text from parts array
    if (responseData?.parts && Array.isArray(responseData.parts)) {
      const textContent = responseData.parts
        .filter((part) => part.type === 'text' && typeof part.text === 'string')
        .map((part) => part.text?.trim())
        .filter(Boolean)
        .join('\n');

      if (textContent) {
        config.logger?.debug?.(`Got response (${textContent.length} chars)`);
        return textContent;
      }
    }

    throw new Error('No text content in AI response');
  } catch (error) {
    config.logger?.warn?.(`OpenCode API call failed: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  } finally {
    // Clean up ephemeral session to prevent leaks on OpenCode server
    await client.session.delete({ path: { id: sessionId } }).catch(() => {
      // Ignore cleanup errors - best effort only
    });
  }
}

/**
 * Get the default model for a provider
 */
export function getDefaultModel(provider: string): string {
  return DEFAULT_MODELS[provider as SupportedProvider] || DEFAULT_MODELS.anthropic;
}

/**
 * Extract text content from OpenCode SDK response (for tests)
 */
export function extractTextFromResponse(result: unknown): string {
  // OpenCode SDK returns an AssistantMessage with parts array
  if (result && typeof result === 'object' && 'parts' in result) {
    const message = result as { parts?: Array<{ type: string; text?: string }> };
    if (Array.isArray(message.parts)) {
      const textContent = message.parts
        .filter((part) => part.type === 'text' && typeof part.text === 'string')
        .map((part) => part.text)
        .join('\n');
      if (textContent) {
        return textContent;
      }
    }
  }

  // Fallback: try to get text directly
  if (result && typeof result === 'object' && 'text' in result) {
    return String((result as { text: unknown }).text);
  }

  throw new Error('No text content in AI response');
}
