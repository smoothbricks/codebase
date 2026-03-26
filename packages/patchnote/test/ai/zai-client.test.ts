import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { sendPrompt, shutdownAIClient } from '../../src/ai/zai-client.js';
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

describe('zai-client (backward compat re-export)', () => {
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

    it('should throw when no API key', async () => {
      mockConfig.ai.apiKey = undefined;

      await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('No AI API key found');
    });

    it('should throw on HTTP error', async () => {
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
  });

  describe('shutdownAIClient', () => {
    it('should be a no-op', async () => {
      // Should not throw
      await shutdownAIClient();
      await shutdownAIClient(createMockLogger());
    });
  });
});
