/**
 * GitHub App manifest generation for the setup command.
 * Builds the manifest JSON and generates the auto-submitting HTML form page.
 */

interface ManifestOptions {
  org: string
  appName: string
  port: number
}

interface AppManifest {
  name: string
  description: string
  url: string
  hook_attributes: { url: string; active: boolean }
  redirect_url: string
  public: boolean
  default_permissions: Record<string, string>
  default_events: string[]
}

/**
 * Build a GitHub App manifest JSON object.
 */
export function buildManifest(options: ManifestOptions): AppManifest {
  return {
    name: options.appName,
    description: `Automated dependency updates for ${options.org}`,
    url: 'https://github.com/smoothbricks/smoothbricks',
    hook_attributes: {
      url: 'https://example.com/unused',
      active: false,
    },
    redirect_url: `http://localhost:${options.port}/callback`,
    public: false,
    default_permissions: {
      contents: 'write',
      pull_requests: 'write',
      workflows: 'write',
      metadata: 'read',
    },
    default_events: [],
  }
}

/**
 * Escape HTML special characters to prevent XSS.
 */
function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/'/g, '&#39;')
    .replace(/"/g, '&quot;')
}

/**
 * Generate an HTML page that auto-submits the manifest form to GitHub.
 *
 * The form POSTs to GitHub's app creation endpoint with the manifest as a hidden input.
 * The page auto-submits on load, so the user is immediately redirected to GitHub.
 */
export function generateManifestPage(org: string, manifest: object): string {
  const url = `https://github.com/organizations/${org}/settings/apps/new`
  const escapedManifest = escapeHtml(JSON.stringify(manifest))

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Patchnote Setup</title></head>
<body>
  <p>Redirecting to GitHub...</p>
  <form id="manifest-form" action="${url}" method="post">
    <input type="hidden" name="manifest" value="${escapedManifest}">
  </form>
  <script>document.getElementById('manifest-form').submit()</script>
</body>
</html>`
}
