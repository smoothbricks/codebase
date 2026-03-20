import { describe, expect, test } from 'bun:test'
import { buildManifest, generateManifestPage } from '../../src/setup/manifest.js'

describe('buildManifest', () => {
  test('returns correct manifest structure with all required fields', () => {
    const manifest = buildManifest({ org: 'myorg', appName: 'Patchnote - myorg', port: 54321 })

    expect(manifest.name).toBe('Patchnote - myorg')
    expect(manifest.description).toBe('Automated dependency updates for myorg')
    expect(manifest.url).toBe('https://github.com/smoothbricks/smoothbricks')
    expect(manifest.hook_attributes).toEqual({ url: 'https://example.com/unused', active: false })
    expect(manifest.redirect_url).toBe('http://localhost:54321/callback')
    expect(manifest.public).toBe(false)
    expect(manifest.default_permissions).toEqual({
      contents: 'write',
      pull_requests: 'write',
      workflows: 'write',
      metadata: 'read',
    })
    expect(manifest.default_events).toEqual([])
  })

  test('uses provided appName in name field', () => {
    const manifest = buildManifest({ org: 'testorg', appName: 'My Custom App', port: 12345 })
    expect(manifest.name).toBe('My Custom App')
  })

  test('uses provided port in redirect_url', () => {
    const manifest = buildManifest({ org: 'testorg', appName: 'Test App', port: 9999 })
    expect(manifest.redirect_url).toBe('http://localhost:9999/callback')
  })

  test('includes org in description', () => {
    const manifest = buildManifest({ org: 'acme-corp', appName: 'Test', port: 1234 })
    expect(manifest.description).toBe('Automated dependency updates for acme-corp')
  })
})

describe('generateManifestPage', () => {
  test('returns HTML with auto-submitting form POST to GitHub', () => {
    const manifest = { name: 'Test App' }
    const html = generateManifestPage('myorg', manifest)

    expect(html).toContain('https://github.com/organizations/myorg/settings/apps/new')
    expect(html).toContain('method="post"')
    expect(html).toContain('name="manifest"')
    expect(html).toContain('submit()')
  })

  test('HTML-escapes manifest JSON to prevent XSS', () => {
    const manifest = { name: 'Test <script>alert("xss")</script>' }
    const html = generateManifestPage('myorg', manifest)

    // Should not contain raw HTML special chars in the value
    expect(html).not.toContain('<script>alert')
    expect(html).toContain('&lt;script&gt;')
  })

  test('includes redirect message in body', () => {
    const manifest = { name: 'Test' }
    const html = generateManifestPage('myorg', manifest)

    expect(html).toContain('Redirecting to GitHub')
  })
})
