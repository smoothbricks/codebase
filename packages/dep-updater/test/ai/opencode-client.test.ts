import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import type { DepUpdaterConfig } from '../../src/config.js';
import type { Logger } from '../../src/logger.js';

function createMockLogger(): Logger {
  return {
    info: mock(() => {}),
    warn: mock(() => {}),
    error: mock(() => {}),
    debug: mock(() => {}),
  };
}

// Mock the SDK module before importing the client
const mockSessionCreate = mock(() =>
  Promise.resolve({
    data: { id: 'test-session-123' },
    error: null,
  }),
);

const mockSessionPrompt = mock(() =>
  Promise.resolve({
    data: {
      info: { id: 'msg-123' },
      parts: [{ type: 'text', text: 'AI response text' }],
    },
    error: null,
  }),
);

const mockSessionDelete = mock(() => Promise.resolve({ data: null, error: null }));

const mockClient = {
  session: {
    create: mockSessionCreate,
    prompt: mockSessionPrompt,
    delete: mockSessionDelete,
  },
};

const mockServerClose = mock(() => {});
const mockServer = {
  close: mockServerClose,
};

mock.module('@opencode-ai/sdk', () => ({
  createOpencode: () => Promise.resolve({ client: mockClient, server: mockServer }),
}));

// Import after mocking
const {
  getOpenCodeClient,
  resetOpenCodeClient,
  shutdownOpenCodeClient,
  sendPrompt,
  getDefaultModel,
  extractTextFromResponse,
} = await import('../../src/ai/opencode-client.js');

describe('opencode-client', () => {
  let mockConfig: DepUpdaterConfig;

  beforeEach(() => {
    resetOpenCodeClient();
    mockSessionCreate.mockClear();
    mockSessionPrompt.mockClear();
    mockSessionDelete.mockClear();
    mockServerClose.mockClear();

    // Reset to successful defaults
    mockSessionCreate.mockImplementation(() =>
      Promise.resolve({
        data: { id: 'test-session-123' },
        error: null,
      }),
    );
    mockSessionPrompt.mockImplementation(() =>
      Promise.resolve({
        data: {
          info: { id: 'msg-123' },
          parts: [{ type: 'text', text: 'AI response text' }],
        },
        error: null,
      }),
    );
    mockSessionDelete.mockImplementation(() => Promise.resolve({ data: null, error: null }));

    mockConfig = {
      ai: {
        enabled: true,
        provider: 'anthropic',
        model: 'claude-sonnet-4-5-20250929',
      },
      logger: createMockLogger(),
    } as DepUpdaterConfig;
  });

  afterEach(() => {
    resetOpenCodeClient();
  });

  describe('getOpenCodeClient', () => {
    it('should create and cache client instance', async () => {
      const client1 = await getOpenCodeClient();
      const client2 = await getOpenCodeClient();

      expect(client1).toBe(client2);
      expect(client1).toBe(mockClient);
    });

    it('should log debug messages when logger provided', async () => {
      const logger = createMockLogger();
      await getOpenCodeClient(logger);

      expect(logger.debug).toHaveBeenCalledWith('Initializing OpenCode client...');
      expect(logger.debug).toHaveBeenCalledWith('OpenCode client initialized');
    });
  });

  describe('resetOpenCodeClient', () => {
    it('should clear cached client instance', async () => {
      await getOpenCodeClient();
      resetOpenCodeClient();

      // Next call should re-initialize
      const logger = createMockLogger();
      await getOpenCodeClient(logger);

      expect(logger.debug).toHaveBeenCalledWith('Initializing OpenCode client...');
    });
  });

  describe('getDefaultModel', () => {
    it('should return correct default for opencode (free tier)', () => {
      expect(getDefaultModel('opencode')).toBe('big-pickle');
    });

    it('should return correct default for anthropic', () => {
      expect(getDefaultModel('anthropic')).toBe('claude-sonnet-4-5-20250929');
    });

    it('should return correct default for openai', () => {
      expect(getDefaultModel('openai')).toBe('gpt-4o');
    });

    it('should return correct default for google', () => {
      expect(getDefaultModel('google')).toBe('gemini-1.5-pro');
    });

    it('should fall back to anthropic default for unknown provider', () => {
      expect(getDefaultModel('unknown')).toBe('claude-sonnet-4-5-20250929');
    });
  });

  describe('sendPrompt', () => {
    it('should send prompt and return response text', async () => {
      const result = await sendPrompt(mockConfig, 'Test prompt');

      expect(result).toBe('AI response text');
      expect(mockSessionCreate).toHaveBeenCalledTimes(1);
      expect(mockSessionPrompt).toHaveBeenCalledWith({
        path: { id: 'test-session-123' },
        body: {
          model: {
            providerID: 'anthropic',
            modelID: 'claude-sonnet-4-5-20250929',
          },
          parts: [{ type: 'text', text: 'Test prompt' }],
        },
      });
    });

    it('should use config provider and model', async () => {
      mockConfig.ai.provider = 'openai';
      mockConfig.ai.model = 'gpt-4-turbo';

      await sendPrompt(mockConfig, 'Test prompt');

      expect(mockSessionPrompt).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.objectContaining({
            model: {
              providerID: 'openai',
              modelID: 'gpt-4-turbo',
            },
          }),
        }),
      );
    });

    it('should allow provider/model override via options', async () => {
      await sendPrompt(mockConfig, 'Test prompt', {
        provider: 'google',
        model: 'gemini-2.0-flash',
      });

      expect(mockSessionPrompt).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.objectContaining({
            model: {
              providerID: 'google',
              modelID: 'gemini-2.0-flash',
            },
          }),
        }),
      );
    });

    it('should use default model when not specified', async () => {
      mockConfig.ai.model = undefined;

      await sendPrompt(mockConfig, 'Test prompt');

      expect(mockSessionPrompt).toHaveBeenCalledWith(
        expect.objectContaining({
          body: expect.objectContaining({
            model: {
              providerID: 'anthropic',
              modelID: 'claude-sonnet-4-5-20250929',
            },
          }),
        }),
      );
    });

    it('should clean up session after successful prompt', async () => {
      await sendPrompt(mockConfig, 'Test prompt');

      expect(mockSessionDelete).toHaveBeenCalledWith({
        path: { id: 'test-session-123' },
      });
    });

    it('should clean up session even after prompt failure', async () => {
      mockSessionPrompt.mockImplementation(() =>
        Promise.resolve({
          data: null,
          error: { name: 'APIError', data: { message: 'Rate limited' } },
        }),
      );

      await expect(sendPrompt(mockConfig, 'Test prompt')).rejects.toThrow('Prompt failed');
      expect(mockSessionDelete).toHaveBeenCalledWith({
        path: { id: 'test-session-123' },
      });
    });

    describe('provider validation', () => {
      it('should reject unsupported provider', async () => {
        mockConfig.ai.provider = 'unsupported-provider';

        await expect(sendPrompt(mockConfig, 'Test prompt')).rejects.toThrow(
          "Unsupported AI provider: 'unsupported-provider'. Supported providers: opencode, anthropic, openai, google",
        );

        // Should not make any API calls
        expect(mockSessionCreate).not.toHaveBeenCalled();
      });

      it('should accept anthropic provider', async () => {
        mockConfig.ai.provider = 'anthropic';
        await expect(sendPrompt(mockConfig, 'Test')).resolves.toBe('AI response text');
      });

      it('should accept openai provider', async () => {
        mockConfig.ai.provider = 'openai';
        await expect(sendPrompt(mockConfig, 'Test')).resolves.toBe('AI response text');
      });

      it('should accept google provider', async () => {
        mockConfig.ai.provider = 'google';
        await expect(sendPrompt(mockConfig, 'Test')).resolves.toBe('AI response text');
      });

      it('should accept opencode provider (free tier)', async () => {
        mockConfig.ai.provider = 'opencode';
        await expect(sendPrompt(mockConfig, 'Test')).resolves.toBe('AI response text');
      });
    });

    describe('error handling', () => {
      it('should throw on session creation failure', async () => {
        mockSessionCreate.mockImplementation(() =>
          Promise.resolve({
            data: null,
            error: { name: 'APIError', data: { message: 'Server error' } },
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('Failed to create session: Server error');
      });

      it('should throw on prompt failure with formatted error', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: null,
            error: { name: 'BadRequestError', errors: [{ message: 'Invalid model' }] },
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('Prompt failed: Invalid model');
      });

      it('should add helpful hint for auth errors', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: null,
            error: { name: 'ProviderAuthError', data: { message: 'Invalid API key' } },
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow(
          'Prompt failed: Invalid API key. Check that ANTHROPIC_API_KEY is set correctly.',
        );
      });

      it('should log warning on API failure', async () => {
        mockSessionPrompt.mockImplementation(() => Promise.reject(new Error('Network error')));

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('Network error');
        expect(mockConfig.logger?.warn).toHaveBeenCalledWith('OpenCode API call failed: Network error');
      });

      it('should throw on AI response error in info', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: {
              info: { error: { name: 'APIError', data: { message: 'Model overloaded' } } },
              parts: [],
            },
            error: null,
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('AI response error: Model overloaded');
      });
    });

    describe('response parsing', () => {
      it('should extract text from parts array', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: {
              info: { id: 'msg-123' },
              parts: [
                { type: 'text', text: 'First part' },
                { type: 'tool_use', name: 'test' },
                { type: 'text', text: 'Second part' },
              ],
            },
            error: null,
          }),
        );

        const result = await sendPrompt(mockConfig, 'Test');
        expect(result).toBe('First part\nSecond part');
      });

      it('should trim whitespace from text parts', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: {
              info: { id: 'msg-123' },
              parts: [{ type: 'text', text: '  \n\nHello World  \n' }],
            },
            error: null,
          }),
        );

        const result = await sendPrompt(mockConfig, 'Test');
        expect(result).toBe('Hello World');
      });

      it('should throw error for empty parts array', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: {
              info: { id: 'msg-123' },
              parts: [],
            },
            error: null,
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('No text content in AI response');
      });

      it('should throw error when no text parts exist', async () => {
        mockSessionPrompt.mockImplementation(() =>
          Promise.resolve({
            data: {
              info: { id: 'msg-123' },
              parts: [{ type: 'tool_use', name: 'test' }],
            },
            error: null,
          }),
        );

        await expect(sendPrompt(mockConfig, 'Test')).rejects.toThrow('No text content in AI response');
      });
    });
  });

  describe('extractTextFromResponse', () => {
    it('should extract text from parts array', () => {
      const result = extractTextFromResponse({
        parts: [
          { type: 'text', text: 'Hello' },
          { type: 'tool_use', name: 'test' },
          { type: 'text', text: 'World' },
        ],
      });
      expect(result).toBe('Hello\nWorld');
    });

    it('should extract text from text property', () => {
      const result = extractTextFromResponse({ text: 'Direct text' });
      expect(result).toBe('Direct text');
    });

    it('should throw for unknown format', () => {
      expect(() => extractTextFromResponse({ unknown: 'format' })).toThrow('No text content in AI response');
    });

    it('should throw for empty parts', () => {
      expect(() => extractTextFromResponse({ parts: [] })).toThrow('No text content in AI response');
    });
  });

  describe('shutdownOpenCodeClient', () => {
    it('should close server and clear instances when server exists', async () => {
      // First initialize the client (which also captures the server)
      await getOpenCodeClient();

      // Shutdown should call server.close()
      await shutdownOpenCodeClient();

      expect(mockServerClose).toHaveBeenCalledTimes(1);
    });

    it('should handle no server gracefully', async () => {
      // Don't initialize - just call shutdown directly
      resetOpenCodeClient();

      // Should not throw
      await shutdownOpenCodeClient();

      // close() should not have been called
      expect(mockServerClose).not.toHaveBeenCalled();
    });

    it('should log debug messages when logger provided', async () => {
      const logger = createMockLogger();
      await getOpenCodeClient(logger);

      await shutdownOpenCodeClient(logger);

      expect(logger.debug).toHaveBeenCalledWith('Shutting down OpenCode server...');
      expect(logger.debug).toHaveBeenCalledWith('OpenCode server shut down');
    });

    it('should clear instances so next getOpenCodeClient re-initializes', async () => {
      const logger1 = createMockLogger();
      await getOpenCodeClient(logger1);
      await shutdownOpenCodeClient();

      // Next call should re-initialize
      const logger2 = createMockLogger();
      await getOpenCodeClient(logger2);

      expect(logger2.debug).toHaveBeenCalledWith('Initializing OpenCode client...');
    });
  });
});
