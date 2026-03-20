import { describe, expect, test } from 'bun:test'
import { createExecaSpy, createErrorExeca } from '../helpers/mock-execa.js'
import {
  checkAuthScopes,
  deleteApp,
  detectOrg,
  exchangeCode,
  storeCredentials,
} from '../../src/setup/credential-store.js'

describe('detectOrg', () => {
  test('calls gh repo view and returns trimmed stdout', async () => {
    const spy = createExecaSpy({
      'gh repo view --json owner --jq .owner.login': 'smoothbricks\n',
    })
    const org = await detectOrg(spy.mock)
    expect(org).toBe('smoothbricks')
    expect(spy.calls).toHaveLength(1)
    expect(spy.calls[0]![1]).toEqual(['repo', 'view', '--json', 'owner', '--jq', '.owner.login'])
  })

  test('throws when gh command fails', async () => {
    const mockExeca = createErrorExeca('not a git repository')
    await expect(detectOrg(mockExeca)).rejects.toThrow('not a git repository')
  })
})

describe('checkAuthScopes', () => {
  test('returns true when gh auth status succeeds', async () => {
    const spy = createExecaSpy({
      'gh auth status': 'Logged in to github.com\n',
    })
    const result = await checkAuthScopes(spy.mock)
    expect(result).toBe(true)
  })

  test('returns false when gh auth status fails', async () => {
    const mockExeca = createErrorExeca('not authenticated')
    const result = await checkAuthScopes(mockExeca)
    expect(result).toBe(false)
  })
})

describe('storeCredentials', () => {
  test('calls gh variable set for app ID and gh secret set for PEM', async () => {
    const spy = createExecaSpy({
      'gh variable set PATCHNOTE_APP_ID --org myorg --visibility all --body 12345': '',
      'gh secret set PATCHNOTE_APP_PRIVATE_KEY --org myorg --visibility all': '',
    })

    await storeCredentials({ org: 'myorg', appId: 12345, pem: '-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----' }, spy.mock)

    expect(spy.calls).toHaveLength(2)

    // First call: variable set
    expect(spy.calls[0]![0]).toBe('gh')
    expect(spy.calls[0]![1]).toEqual([
      'variable', 'set', 'PATCHNOTE_APP_ID',
      '--org', 'myorg',
      '--visibility', 'all',
      '--body', '12345',
    ])

    // Second call: secret set with input option
    expect(spy.calls[1]![0]).toBe('gh')
    expect(spy.calls[1]![1]).toEqual([
      'secret', 'set', 'PATCHNOTE_APP_PRIVATE_KEY',
      '--org', 'myorg',
      '--visibility', 'all',
    ])
    expect(spy.calls[1]![2]).toHaveProperty('input')
  })
})

describe('deleteApp', () => {
  test('calls gh api DELETE for the app slug', async () => {
    const spy = createExecaSpy({
      'gh api -X DELETE /apps/my-app-slug': '',
    })
    await deleteApp('my-app-slug', spy.mock)
    expect(spy.calls).toHaveLength(1)
    expect(spy.calls[0]![1]).toEqual(['api', '-X', 'DELETE', '/apps/my-app-slug'])
  })

  test('does not throw when delete fails (best-effort)', async () => {
    const mockExeca = createErrorExeca('not found')
    // Should not throw
    await expect(deleteApp('nonexistent', mockExeca)).resolves.toBeUndefined()
  })
})

describe('exchangeCode', () => {
  test('calls gh api POST and parses credentials from response', async () => {
    const response = JSON.stringify({
      id: 98765,
      pem: '-----BEGIN RSA PRIVATE KEY-----\nkey\n-----END RSA PRIVATE KEY-----',
      webhook_secret: 'whsecret123',
      slug: 'patchnote-myorg',
    })
    const spy = createExecaSpy({
      'gh api POST /app-manifests/TEMPCODE123/conversions': response,
    })

    const credentials = await exchangeCode('TEMPCODE123', spy.mock)

    expect(credentials.id).toBe(98765)
    expect(credentials.pem).toContain('BEGIN RSA PRIVATE KEY')
    expect(credentials.webhookSecret).toBe('whsecret123')
    expect(credentials.slug).toBe('patchnote-myorg')
  })

  test('throws when gh api call fails', async () => {
    const mockExeca = createErrorExeca('invalid code')
    await expect(exchangeCode('BADCODE', mockExeca)).rejects.toThrow('invalid code')
  })
})
