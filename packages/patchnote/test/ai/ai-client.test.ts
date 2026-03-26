import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { getProviderTokenBudget, resolveProvider, sendPrompt, shutdownAIClient } from '../../src/ai/ai-client.js';
import { PROVIDER_CONFIGS } from '../../src/ai/providers.js';
import type { PatchnoteConfig } from '../../src/config.js';
import type { Logger } from '../../src/logger.js';

function createMockLogger(): Logger {
  return {
    info: mock(() => {}),
    warn: mock(() => {}),
    error: mock(() => {}),
    debug: mock(() => {}),
  };
}

describe('ai-client', () => {
  let mockConfig: PatchnoteConfig;
  const originalFetch = globalThis.fetch;
  let savedZaiKey: string | undefined;
  let savedGeminiKey: string | undefined;

  beforeEach(() => {
    savedZaiKey = process.env.ZAI_API_KEY;
    savedGeminiKey = process.env.GEMINI_API_KEY;
    delete process.env.ZAI_API_KEY;
    delete process.env.GEMINI_API_KEY;

    mockConfig = {
      ai: {
        provider: 'zai',
        model: 'glm-5-turbo',
        apiKey: 'test-zai-key',
      },
      logger: createMockLogger(),
    } as PatchnoteConfig;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    if (savedZaiKey !== undefined) {
      process.env.ZAI_API_KEY = savedZaiKey;
    } else {
      delete process.env.ZAI_API_KEY;
    }
    if (savedGeminiKey !== undefined) {
      process.env.GEMINI_API_KEY = savedGeminiKey;
    } else {
      delete process.env.GEMINI_API_KEY;
    }
  });

  describe('PROVIDER_CONFIGS', () => {
    it('should contain zai config with correct values', () => {
      expect(PROVIDER_CONFIGS.zai).toEqual({
        apiUrl: 'https://api.z.ai/api/paas/v4/chat/completions',
        envVar: 'ZAI_API_KEY',
        defaultModel: 'glm-5-turbo',
        defaultTokenBudget: 64000,
      });
    });

    it('should contain gemini config with correct values', () => {
      expect(PROVIDER_CONFIGS.gemini).toEqual({
        apiUrl: 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions',
        envVar: 'GEMINI_API_KEY',
        defaultModel: 'gemini-2.5-flash',
        defaultTokenBudget: 128000,
      });
    });
  });

  describe('sendPrompt', () => {
    it('should send prompt and return response text', async () => {
      globalThis.fetch = mock(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              choices: [{ message: { content: 'AI response text' } }],
            }),
            { status: 200 },
          ),
        ),
      ) as typeof fetch;

      const result = await sendPrompt(mockConfig, 'Test prompt');
      expect(result).toBe('AI response text');
    });

    it('should send correct request body', async () => {
      let capturedBody = '';
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedBody = (init?.body as string) || '';
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test prompt');

      const body = JSON.parse(capturedBody);
      expect(body.model).toBe('glm-5-turbo');
      expect(body.messages).toEqual([{ role: 'user', content: 'Test prompt' }]);
      expect(body.temperature).toBe(0.6);
    });

    it('should send authorization header', async () => {
      let capturedHeaders: HeadersInit | undefined;
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedHeaders = init?.headers;
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test prompt');

      expect(capturedHeaders).toEqual(
        expect.objectContaining({
          Authorization: 'Bearer test-zai-key',
        }),
      );
    });

    it('should use model override from options', async () => {
      let capturedBody = '';
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedBody = (init?.body as string) || '';
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test', { model: 'glm-4-plus' });

      const body = JSON.parse(capturedBody);
      expect(body.model).toBe('glm-4-plus');
    });

    it('should fall back to ZAI_API_KEY env var', async () => {
      mockConfig.ai.apiKey = undefined;
      process.env.ZAI_API_KEY = 'env-key';

      let capturedHeaders: HeadersInit | undefined;
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedHeaders = init?.headers;
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test');

      expect(capturedHeaders).toEqual(
        expect.objectContaining({
          Authorization: 'Bearer env-key',
        }),
      );
    });

    it('should throw when no API key available', async () => {
      mockConfig.ai.apiKey = undefined;

      await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('No AI API key found');
    });

    it('should throw on HTTP error with provider name', async () => {
      globalThis.fetch = mock(() => Promise.resolve(new Response('Unauthorized', { status: 401 }))) as typeof fetch;

      await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('zai API error 401');
    });

    it('should throw when no content in response', async () => {
      globalThis.fetch = mock(() =>
        Promise.resolve(new Response(JSON.stringify({ choices: [] }), { status: 200 })),
      ) as typeof fetch;

      await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('No content in zai response');
    });

    it('should trim response text', async () => {
      globalThis.fetch = mock(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              choices: [{ message: { content: '  \n\nHello World  \n' } }],
            }),
            { status: 200 },
          ),
        ),
      ) as typeof fetch;

      const result = await sendPrompt(mockConfig, 'Test');
      expect(result).toBe('Hello World');
    });

    it('should send to Z.AI URL when provider is zai', async () => {
      let capturedUrl = '';
      globalThis.fetch = mock((url: string | URL | Request, _init?: RequestInit) => {
        capturedUrl = typeof url === 'string' ? url : url instanceof URL ? url.href : url.url;
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test');

      expect(capturedUrl).toBe('https://api.z.ai/api/paas/v4/chat/completions');
    });
  });

  describe('gemini provider', () => {
    beforeEach(() => {
      mockConfig.ai.provider = 'gemini';
      mockConfig.ai.apiKey = 'test-gemini-key';
      mockConfig.ai.model = undefined;
    });

    it('should send to Gemini URL when provider is gemini', async () => {
      let capturedUrl = '';
      globalThis.fetch = mock((url: string | URL | Request, _init?: RequestInit) => {
        capturedUrl = typeof url === 'string' ? url : url instanceof URL ? url.href : url.url;
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test');

      expect(capturedUrl).toBe('https://generativelanguage.googleapis.com/v1beta/openai/chat/completions');
    });

    it('should use gemini-2.5-flash as default model', async () => {
      let capturedBody = '';
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedBody = (init?.body as string) || '';
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test');

      const body = JSON.parse(capturedBody);
      expect(body.model).toBe('gemini-2.5-flash');
    });

    it('should use GEMINI_API_KEY env var when no config.ai.apiKey', async () => {
      mockConfig.ai.apiKey = undefined;
      process.env.GEMINI_API_KEY = 'gemini-env-key';

      let capturedHeaders: HeadersInit | undefined;
      globalThis.fetch = mock((_url: string | URL | Request, init?: RequestInit) => {
        capturedHeaders = init?.headers;
        return Promise.resolve(
          new Response(JSON.stringify({ choices: [{ message: { content: 'ok' } }] }), { status: 200 }),
        );
      }) as typeof fetch;

      await sendPrompt(mockConfig, 'Test');

      expect(capturedHeaders).toEqual(
        expect.objectContaining({
          Authorization: 'Bearer gemini-env-key',
        }),
      );
    });

    it('should throw gemini-specific error on HTTP error', async () => {
      globalThis.fetch = mock(() => Promise.resolve(new Response('Unauthorized', { status: 401 }))) as typeof fetch;

      await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('gemini API error 401');
    });
  });

  describe('resolveProvider', () => {
    it('should return configured provider when config.ai.apiKey is set', () => {
      mockConfig.ai.provider = 'zai';
      mockConfig.ai.apiKey = 'explicit-key';

      const result = resolveProvider(mockConfig);

      expect(result).toEqual({ provider: 'zai', apiKey: 'explicit-key' });
    });

    it('should return configured provider when matching env var is set', () => {
      mockConfig.ai.apiKey = undefined;
      mockConfig.ai.provider = 'zai';
      process.env.ZAI_API_KEY = 'zai-env-key';

      const result = resolveProvider(mockConfig);

      expect(result).toEqual({ provider: 'zai', apiKey: 'zai-env-key' });
    });

    it('should fall back to gemini when ZAI_API_KEY missing but GEMINI_API_KEY present', () => {
      mockConfig.ai.apiKey = undefined;
      mockConfig.ai.provider = 'zai';
      process.env.GEMINI_API_KEY = 'gemini-fallback-key';

      const result = resolveProvider(mockConfig);

      expect(result).toEqual({ provider: 'gemini', apiKey: 'gemini-fallback-key' });
    });

    it('should fall back to zai when GEMINI_API_KEY missing but ZAI_API_KEY present', () => {
      mockConfig.ai.apiKey = undefined;
      mockConfig.ai.provider = 'gemini';
      process.env.ZAI_API_KEY = 'zai-fallback-key';

      const result = resolveProvider(mockConfig);

      expect(result).toEqual({ provider: 'zai', apiKey: 'zai-fallback-key' });
    });

    it('should return null when no keys available', () => {
      mockConfig.ai.apiKey = undefined;

      const result = resolveProvider(mockConfig);

      expect(result).toBeNull();
    });

    it('should return configured provider with explicit apiKey even when env vars exist', () => {
      mockConfig.ai.provider = 'gemini';
      mockConfig.ai.apiKey = 'explicit-gemini-key';
      process.env.ZAI_API_KEY = 'should-not-use-this';

      const result = resolveProvider(mockConfig);

      expect(result).toEqual({ provider: 'gemini', apiKey: 'explicit-gemini-key' });
    });
  });

  describe('getProviderTokenBudget', () => {
    it('should return zai default budget (64k) for zai provider', () => {
      mockConfig.ai.provider = 'zai';
      mockConfig.ai.tokenBudget = undefined;
      mockConfig.ai.apiKey = 'key';

      const budget = getProviderTokenBudget(mockConfig);

      expect(budget).toBe(64000);
    });

    it('should return gemini default budget (128k) for gemini provider', () => {
      mockConfig.ai.provider = 'gemini';
      mockConfig.ai.tokenBudget = undefined;
      mockConfig.ai.apiKey = 'key';

      const budget = getProviderTokenBudget(mockConfig);

      expect(budget).toBe(128000);
    });

    it('should return config override when set', () => {
      mockConfig.ai.provider = 'zai';
      mockConfig.ai.tokenBudget = 32000;
      mockConfig.ai.apiKey = 'key';

      const budget = getProviderTokenBudget(mockConfig);

      expect(budget).toBe(32000);
    });

    it('should return fallback budget based on resolved provider when falling back', () => {
      mockConfig.ai.provider = 'zai';
      mockConfig.ai.tokenBudget = undefined;
      mockConfig.ai.apiKey = undefined;
      process.env.GEMINI_API_KEY = 'gemini-key';

      const budget = getProviderTokenBudget(mockConfig);

      expect(budget).toBe(128000);
    });
  });

  describe('shutdownAIClient', () => {
    it('should be a no-op', async () => {
      // Should not throw
      await shutdownAIClient();
      await shutdownAIClient(createMockLogger());
    });
  });
});
