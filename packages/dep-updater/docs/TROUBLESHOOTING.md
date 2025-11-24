# Troubleshooting Guide

This guide covers common issues and solutions for `dep-updater`. Issues are organized by category for easy navigation.

## Quick Links

- [Authentication Issues](#authentication-issues)
- [GitHub CLI Issues](#github-cli-issues)
- [CI Workflow Problems](#ci-workflow-problems)
- [Rate Limiting](#rate-limiting)
- [Configuration Errors](#configuration-errors)
- [Setup Validation](#setup-validation)

## Authentication Issues

### PAT: "gh: authentication required"

**Problem:** The Personal Access Token isn't accessible to the workflow.

**Symptoms:**

```
Error: gh: authentication required
Run `gh auth login` to authenticate
```

**Solutions:**

1. **Verify secret exists:**

   ```bash
   gh secret list --org YOUR_ORG
   ```

   Look for `DEP_UPDATER_TOKEN` in the list.

2. **Check repository access:**
   - Go to Organization Settings → Secrets and variables → Actions
   - Click on `DEP_UPDATER_TOKEN`
   - Verify "Repository access" includes your repository

3. **Check token expiration:**
   - PAT tokens expire after 90 days
   - Regenerate at https://github.com/settings/tokens
   - Update organization secret with new token

4. **Verify workflow configuration:**
   ```yaml
   env:
     GH_TOKEN: ${{ secrets.DEP_UPDATER_TOKEN }}
   ```

### GitHub App: "Bad credentials" or "401 Unauthorized"

**Problem:** GitHub App credentials are incorrect or inaccessible.

**Symptoms:**

```
Error: Bad credentials
Error: 401 Unauthorized
```

**Possible Causes:**

- App ID is incorrect
- Private key is malformed or incomplete
- App is not installed on the repository
- Secrets not accessible to this repository

**Solutions:**

1. **Verify App ID:**
   - Go to Organization Settings → Developer settings → GitHub Apps
   - Click your app → Note the App ID
   - Compare with `DEP_UPDATER_APP_ID` variable value

2. **Verify private key:**
   - Open the `.pem` file and ensure you copied **entire content**
   - Should start with `-----BEGIN RSA PRIVATE KEY-----`
   - Should end with `-----END RSA PRIVATE KEY-----`
   - Should be ~1600-3200 characters
   - Re-copy if uncertain:
     ```bash
     cat your-app.pem | pbcopy  # macOS
     cat your-app.pem | xclip   # Linux
     ```

3. **Check app installation:**

   ```bash
   gh api /repos/OWNER/REPO/installation --jq .id
   ```

   Should return an installation ID. If error, app isn't installed.

4. **Verify secret/variable access:**
   - Organization Settings → Secrets and variables → Actions
   - Check both Variables and Secrets tabs
   - Ensure "Repository access" includes your repository

### GitHub App: "404 Not Found"

**Problem:** App is not installed or repository doesn't have access.

**Symptoms:**

```
Error: 404 Not Found
HttpError: Not Found
```

**Possible Causes:**

- App is not installed on this repository
- Repository access was not granted to organization secrets

**Solutions:**

1. **Verify app installation:**
   - Go to Organization Settings → GitHub Apps
   - Click "Configure" next to your app
   - Check if repository is in the allowed list
   - Add repository if needed

2. **Check secret/variable access:**
   - Go to Organization Settings → Secrets and variables → Actions
   - Click on `DEP_UPDATER_APP_ID` (Variables tab)
   - Click on `DEP_UPDATER_APP_PRIVATE_KEY` (Secrets tab)
   - Verify "Repository access" includes your repository

3. **Reinstall app if needed:**
   - Go to GitHub App settings → Install App
   - Click "Configure" for your organization
   - Choose "All repositories" or add specific repositories

### GitHub App: "403 Forbidden" or "Resource not accessible"

**Problem:** App doesn't have required permissions.

**Symptoms:**

```
Error: 403 Forbidden
Error: Resource not accessible by integration
```

**Possible Causes:**

- App doesn't have required permissions
- Token generation failed
- Permissions were revoked

**Solutions:**

1. **Check app permissions:**
   - Go to Organization Settings → Developer settings → GitHub Apps
   - Click your app → Permissions
   - Verify the following:
     - **Contents**: Read and write ✅
     - **Pull requests**: Read and write ✅
     - **Workflows**: Read and write (optional) ⚠️

2. **Update permissions:**
   - If permissions are missing, update them
   - Click "Save changes"
   - Re-install the app (permissions update requires reinstall):
     - Go to Install App tab
     - Click "Configure"
     - Click "Save" to apply new permissions

3. **Verify token generation:**
   ```bash
   # Test token generation manually
   dep-updater validate-setup
   ```

## GitHub CLI Issues

### "gh: command not found"

**Problem:** GitHub CLI is not installed.

**Symptoms:**

```
Error: gh: command not found
```

**Solutions:**

Install GitHub CLI for your platform:

**macOS:**

```bash
brew install gh
```

**Linux (Debian/Ubuntu):**

```bash
(type -p wget >/dev/null || (sudo apt update && sudo apt-get install wget -y)) \
  && sudo mkdir -p -m 755 /etc/apt/keyrings \
  && wget -qO- https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
  && sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
  && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
  && sudo apt update \
  && sudo apt install gh -y
```

**Windows:**

```powershell
winget install GitHub.cli
```

**Verify installation:**

```bash
gh --version
gh auth status
```

### "gh: not authenticated"

**Problem:** GitHub CLI is not authenticated.

**Solution:**

```bash
gh auth login
```

Follow the prompts to authenticate.

## CI Workflow Problems

### PRs don't trigger CI workflows

**Problem:** PRs created by dep-updater don't trigger other CI workflows.

**Symptoms:**

- PR is created successfully
- No CI checks run on the PR
- Manual push to the branch triggers CI

**Explanation:**

This behavior depends on your authentication method:

**PAT Authentication:**

- ❌ This is **expected behavior** with PAT
- GitHub prevents infinite loop scenarios by not triggering workflows from PAT-created PRs
- This is a GitHub platform limitation

**Solutions:**

1. **Workaround (PAT):** Manually trigger CI:

   ```bash
   gh workflow run tests.yml --ref chore/update-deps-2025-01-20
   ```

2. **Permanent fix:** Upgrade to GitHub App authentication:
   - See [SETUP.md](./SETUP.md) for GitHub App setup
   - GitHub App tokens **do** trigger CI workflows properly
   - Takes 15-20 minutes to set up (one-time)

**GitHub App Authentication:**

- ✅ CI workflows trigger automatically
- No workarounds needed

**Verify workflow uses GitHub App correctly:**

```yaml
- name: Generate GitHub App Token
  id: app-token
  uses: actions/create-github-app-token@v2
  with:
    app-id: ${{ vars.DEP_UPDATER_APP_ID }}
    private-key: ${{ secrets.DEP_UPDATER_APP_PRIVATE_KEY }}

- name: Run dep-updater
  env:
    GH_TOKEN: ${{ steps.app-token.outputs.token }} # Use app token, not GITHUB_TOKEN
```

### Workflow fails with "permission denied"

**Problem:** Workflow doesn't have write permissions.

**Solutions:**

1. **Check workflow permissions:**

   ```yaml
   permissions:
     contents: write
     pull-requests: write
   ```

2. **Check organization settings:**
   - Go to Organization Settings → Actions → General
   - Under "Workflow permissions"
   - Select "Read and write permissions"

## Rate Limiting

### PAT: "API rate limit exceeded"

**Problem:** PAT has hit the 5,000 requests/hour rate limit.

**Symptoms:**

```
Error: API rate limit exceeded
Error: 403 rate limit exceeded
```

**Solutions:**

1. **Check rate limit usage:**

   ```bash
   gh api rate_limit
   ```

2. **Reduce update frequency:**
   - Change schedule in `.github/workflows/update-deps.yml`
   - From `0 2 * * *` (daily) to `0 2 * * 1` (weekly)

3. **Upgrade to GitHub App:**
   - GitHub App: 15,000 requests/hour
   - PAT: 5,000 requests/hour
   - 3x rate limit improvement
   - See [SETUP.md](./SETUP.md)

### GitHub App: Rate limit issues

**Problem:** Even GitHub App hits rate limits in high-volume scenarios.

**Solutions:**

1. **Check rate limit:**

   ```bash
   gh api rate_limit
   ```

2. **Stagger workflow schedules:**
   - If multiple repos run simultaneously, stagger them
   - Repo A: `0 2 * * *` (2 AM)
   - Repo B: `0 3 * * *` (3 AM)
   - Repo C: `0 4 * * *` (4 AM)

3. **Reduce concurrent operations:**
   - Disable Nix updates if not needed (`nix.enabled: false`)
   - Disable AI analysis temporarily (`--skip-ai` flag)

## Configuration Errors

### "Failed to parse config file"

**Problem:** Config file has invalid syntax.

**Symptoms:**

```
Error: Failed to parse config file at tooling/dep-updater.json
SyntaxError: Unexpected token
```

**Solutions:**

1. **Validate JSON syntax:**

   ```bash
   cat tooling/dep-updater.json | jq
   ```

2. **Common JSON mistakes:**
   - Trailing commas: `"value",}` → `"value"}`
   - Missing quotes: `{key: value}` → `{"key": "value"}`
   - Wrong brackets: `[}` → `{}`

3. **Use TypeScript config instead:**
   - Rename to `tooling/dep-updater.ts`
   - Use `defineConfig()` helper for type safety
   - See [CONFIGURATION.md](./CONFIGURATION.md)

### "Invalid config file"

**Problem:** Config values don't match expected types.

**Symptoms:**

```
Error: Invalid config file
Error: expected object, got array
```

**Solutions:**

1. **Check config structure:**
   - See [CONFIGURATION.md](./CONFIGURATION.md) for valid options
   - Ensure nested objects are properly structured

2. **Use TypeScript config:**
   - TypeScript will catch type errors before runtime
   - See [CONFIGURATION.md](./CONFIGURATION.md)

### Private key format errors

**Problem:** Private key is not in correct format.

**Symptoms:**

```
Error: error:1E08010C:DECODER routines::unsupported
Error: Invalid PEM formatted message
```

**Solution:**

1. **Use the downloaded file as-is:**
   - Don't convert or modify the `.pem` file
   - GitHub provides correct format (PKCS#1 or PKCS#8)

2. **Re-download private key:**
   - You cannot retrieve an existing key
   - Generate a new key from GitHub App settings
   - Update organization secret with new key
   - GitHub supports multiple active keys for zero-downtime rotation

## Setup Validation

### Running validate-setup

The `validate-setup` command checks your entire setup:

```bash
dep-updater validate-setup
```

**Checks performed:**

- ✓ GitHub CLI is installed
- ✓ GitHub CLI is authenticated
- ✓ GitHub App is installed on repository
- ✓ App has required permissions (contents:write, pull-requests:write)
- ✓ Can generate GitHub App tokens
- ✓ Config file is valid

**Exit codes:**

- `0` - All checks passed
- `1` - One or more checks failed

**When to use:**

- After initial setup
- When troubleshooting authentication issues
- Before running workflows for the first time
- After changing GitHub App permissions

## Getting Help

If you've tried the solutions above and still have issues:

1. **Run validation:**

   ```bash
   dep-updater validate-setup
   ```

2. **Check workflow logs:**
   - Go to Actions tab in GitHub
   - Click failed workflow run
   - Review error messages

3. **Enable verbose logging:**

   ```bash
   dep-updater update-deps --verbose
   ```

4. **Open an issue:**
   - Repository: https://github.com/smoothbricks/smoothbricks/issues
   - Include:
     - Authentication method (PAT or GitHub App)
     - Error messages
     - Workflow logs (redact sensitive information)
     - Output of `dep-updater validate-setup`

## See Also

- [Getting Started Guide](./GETTING-STARTED.md) - Complete setup walkthrough (PAT or GitHub App)
- [Configuration Reference](./CONFIGURATION.md) - All config options
- [API Reference](./API.md) - Programmatic usage
- [README](../README.md) - Main documentation
