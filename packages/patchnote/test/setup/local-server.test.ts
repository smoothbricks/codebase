import { afterEach, describe, expect, test } from 'bun:test'
import { startCallbackServer } from '../../src/setup/local-server.js'

// Track servers to clean up
let serverCloseFunc: (() => void) | null = null

afterEach(() => {
  if (serverCloseFunc) {
    serverCloseFunc()
    serverCloseFunc = null
  }
})

describe('startCallbackServer', () => {
  test('returns port, waitForCode, and close', async () => {
    const server = await startCallbackServer({ manifestPage: '<html>test</html>' })
    serverCloseFunc = server.close

    expect(server.port).toBeGreaterThan(0)
    expect(typeof server.waitForCode).toBe('function')
    expect(typeof server.close).toBe('function')

    server.close()
    serverCloseFunc = null
  })

  test('serves manifest HTML page on GET /', async () => {
    const testHtml = '<html><body>Test Manifest Page</body></html>'
    const server = await startCallbackServer({ manifestPage: testHtml })
    serverCloseFunc = server.close

    const response = await fetch(`http://localhost:${server.port}/`)
    const body = await response.text()

    expect(response.headers.get('content-type')).toContain('text/html')
    expect(body).toBe(testHtml)

    server.close()
    serverCloseFunc = null
  })

  test('resolves waitForCode with code from GET /callback?code=XYZ', async () => {
    const server = await startCallbackServer({ manifestPage: '<html></html>' })
    serverCloseFunc = server.close

    const codePromise = server.waitForCode()

    // Simulate GitHub callback
    const response = await fetch(`http://localhost:${server.port}/callback?code=ABC123`)
    expect(response.status).toBe(200)

    const code = await codePromise
    expect(code).toBe('ABC123')

    // Server should auto-close after receiving callback
    serverCloseFunc = null
  })

  test('responds with success HTML on callback', async () => {
    const server = await startCallbackServer({ manifestPage: '<html></html>' })
    serverCloseFunc = server.close

    const response = await fetch(`http://localhost:${server.port}/callback?code=TEST`)
    const body = await response.text()

    expect(body).toContain('GitHub App created')
    expect(body).toContain('close this tab')

    await server.waitForCode()
    serverCloseFunc = null
  })

  test('rejects waitForCode on timeout', async () => {
    const server = await startCallbackServer({
      manifestPage: '<html></html>',
      timeoutMs: 100, // Very short timeout for testing
    })
    serverCloseFunc = server.close

    await expect(server.waitForCode()).rejects.toThrow(/timeout/i)
    serverCloseFunc = null
  })
})
