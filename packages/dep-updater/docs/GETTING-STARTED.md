# Getting Started with dep-updater

This guide will help you set up dep-updater for automated dependency updates in your organization.

## Unified Workflow with Runtime Auth Detection

dep-updater uses a **single workflow file** that automatically detects your authentication method at runtime:

- **If `DEP_UPDATER_APP_ID` is set** → Uses GitHub App (priority)
- **Otherwise** → Falls back to PAT using `DEP_UPDATER_TOKEN`

This means you can **switch auth methods without regenerating the workflow file** - just add or remove the appropriate
secrets/variables.

## Choose Your Auth Method

| Feature            | PAT (Simple)               | GitHub App (Recommended)  |
| ------------------ | -------------------------- | ------------------------- |
| **Setup time**     | 5 minutes                  | 15-20 minutes             |
| **Best for**       | Small teams, trying it out | Organizations, production |
| **Rate limit**     | 5,000 req/hour             | 15,000 req/hour           |
| **Triggers CI**    | No (manual trigger needed) | Yes (automatic)           |
| **Token lifetime** | 90 days (must renew)       | 1 hour (auto-renewed)     |
| **Complexity**     | 3 simple steps             | Multi-step app creation   |

### Recommendation

- 🚀 **Just trying it out?** → Start with [PAT Setup](#option-a-pat-setup-5-minutes)
- 🏢 **Setting up for your organization?** → Use [GitHub App Setup](#option-b-github-app-setup-15-20-minutes)
- ❓ **Not sure?** Start with PAT, you can upgrade to GitHub App later without changing the workflow file

---

## Option A: PAT Setup (5 Minutes)

Personal Access Token (PAT) authentication is the simplest way to get started.

### Prerequisites

- GitHub CLI (`gh`) installed (optional, but recommended)
- Organization admin access to add secrets
- Nx monorepo with dep-updater installed

### Step 1: Generate Personal Access Token (2 minutes)

#### Option A: Using GitHub CLI (Easiest)

```bash
# Generate a new PAT with repo scope
gh auth token

# Or create a new token with specific expiration
gh auth login --scopes repo --with-token
```

#### Option B: Using GitHub Web UI

1. Go to https://github.com/settings/tokens/new
2. Give it a name: "dep-updater for [ORG_NAME]"
3. Select expiration: 90 days (recommended)
4. Select scope: **`repo`** (Full control of private repositories)
5. Click "Generate token"
6. **Copy the token immediately** (you won't see it again!)

**Token looks like:** `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

### Step 2: Add Organization Secret (1 minute)

#### Option A: Using GitHub CLI

```bash
# Replace YOUR_ORG with your organization name
gh secret set DEP_UPDATER_TOKEN --org YOUR_ORG

# When prompted, paste your token
```

#### Option B: Using GitHub Web UI

1. Go to `https://github.com/organizations/YOUR_ORG/settings/secrets/actions`
2. Click "New organization secret"
3. Name: `DEP_UPDATER_TOKEN`
4. Value: Paste your token
5. Repository access: Choose repositories or "All repositories"
6. Click "Add secret"

**✅ That's it for authentication!** You won't need to do this again for any repo in your organization.

### Step 3: Generate Workflow File (1 minute)

```bash
npx @smoothbricks/dep-updater generate-workflow
```

This creates `.github/workflows/update-deps.yml` with runtime auth detection. The workflow will automatically use PAT
authentication since you have `DEP_UPDATER_TOKEN` configured.

**AI Changelog Analysis** is enabled by default using the free OpenCode provider. To disable AI:

- Set repository variable `DEP_UPDATER_SKIP_AI=true`, or
- Regenerate with `--skip-ai` flag

**Premium AI providers:** Add your API key secret (e.g., `ANTHROPIC_API_KEY`) to use Anthropic, OpenAI, or Google.

### Step 4: Commit and Push (1 minute)

```bash
git add .github/workflows/update-deps.yml
git commit -m "chore: add automated dependency updates"
git push
```

### Step 5: Test It! (1 minute)

```bash
# Trigger the workflow manually
gh workflow run update-deps.yml

# Watch it run
gh run watch

# Or view in browser
gh workflow view update-deps.yml --web
```

### PAT Setup Complete! 🎉

**Next steps:**

- The workflow will run daily at 2 AM UTC
- PRs will be created automatically for dependency updates
- Review and merge PRs when ready

**Limitations of PAT:**

- ⚠️ PRs don't trigger CI workflows (GitHub security limitation)
- ⚠️ Token must be renewed every 90 days
- ⚠️ Lower rate limits (5,000 req/hour)

**Want to upgrade?** See [GitHub App Setup](#option-b-github-app-setup-15-20-minutes) below for automatic CI triggering
and higher rate limits.

---

## Option B: GitHub App Setup (15-20 Minutes)

GitHub App authentication provides better rate limits, automatic CI triggering, and auto-refreshing tokens. Recommended
for production use.

### Why GitHub App?

- ✅ Higher rate limits (15,000 vs 5,000 requests/hour)
- ✅ PRs properly trigger CI workflows (PAT doesn't)
- ✅ No 90-day token renewal (auto-refreshes)
- ✅ Better for production and high-volume usage

### Prerequisites

- Organization admin access (to create and install GitHub Apps)
- Access to repository settings (to run workflows)
- `gh` CLI installed and authenticated

### Step 1: Create GitHub App (10 minutes)

#### 1.1 Navigate to GitHub App Settings

1. Go to your **Organization Settings** (not personal settings)
   - Click your organization avatar → Settings
   - Or visit: `https://github.com/organizations/YOUR-ORG/settings`

2. In the left sidebar:
   - Scroll to **Developer settings** (near bottom)
   - Click **GitHub Apps**
   - Click **New GitHub App**

#### 1.2 Configure GitHub App

Fill in the following fields:

**GitHub App name**:

```
Dep Updater - YOUR-ORG-NAME
```

(Must be globally unique across all of GitHub)

**Homepage URL**:

```
https://github.com/YOUR-ORG/YOUR-REPO
```

(Or your organization's homepage)

**Webhook**:

- ☑ Uncheck "Active" - webhooks are not needed for this use case

**Repository permissions**:

- **Contents**: Read and write ✅
  - _Needed to: commit dependency updates, push to branches_
- **Pull requests**: Read and write ✅
  - _Needed to: create PRs, update PR descriptions, close old PRs_
- **Workflows**: Read and write (optional) ⚠️
  - _Only needed if you want to update workflow files automatically_
- **Metadata**: Read-only (automatically checked)

**Where can this GitHub App be installed**:

- ☑ **Only on this account** (recommended for organization-specific apps)

#### 1.3 Create the App

1. Click **Create GitHub App** at the bottom
2. You'll see a confirmation page with your App ID
3. **Important**: Note down your **App ID** - you'll need this later

Example: `App ID: 123456`

#### 1.4 Generate Private Key

1. On the app settings page, scroll to **Private keys** section
2. Click **Generate a private key**
3. A `.pem` file will download automatically
4. **Important**: Store this file securely - you'll need it in the next step

The file name will look like: `your-app-name.2025-01-21.private-key.pem`

### Step 2: Install GitHub App (2 minutes)

#### 2.1 Install to Organization

1. In the left sidebar of your GitHub App settings, click **Install App**
2. Click **Install** next to your organization name
3. Choose installation scope:
   - **All repositories** (recommended for organization-wide updates)
   - Or **Only select repositories** (if you want to limit access)
4. Click **Install**

#### 2.2 Verify Installation

Check that the app is installed:

```bash
gh api /orgs/YOUR-ORG/installations --jq '.[].app_slug'
```

You should see your app in the list.

### Step 3: Configure Organization Secrets (3 minutes)

Organization-level secrets allow all repositories in your organization to use the same GitHub App credentials.

#### 3.1 Add Organization Variable (App ID)

1. Go to **Organization Settings** → **Secrets and variables** → **Actions**
2. Click the **Variables** tab
3. Click **New organization variable**
4. Configure:
   - **Name**: `DEP_UPDATER_APP_ID`
   - **Value**: Your App ID from Step 1.3 (e.g., `123456`)
   - **Repository access**:
     - Choose "All repositories" or select specific repos
5. Click **Add variable**

#### 3.2 Add Organization Secret (Private Key)

1. In the same settings page, click the **Secrets** tab
2. Click **New organization secret**
3. Configure:
   - **Name**: `DEP_UPDATER_APP_PRIVATE_KEY`
   - **Value**: Open the `.pem` file in a text editor and copy **entire contents**
     ```
     -----BEGIN RSA PRIVATE KEY-----
     MIIEpAIBAAKCAQEA...
     (many lines of base64 text)
     ...
     -----END RSA PRIVATE KEY-----
     ```
   - **Repository access**: Match the same access as the variable above
4. Click **Add secret**

**Important:**

- Copy the entire PEM file including the BEGIN and END lines
- Don't add extra spaces or newlines
- The secret should be around 1600-3200 characters

#### 3.3 Add AI API Key (Optional)

For AI-powered changelog analysis:

```bash
gh secret set ANTHROPIC_API_KEY --org YOUR_ORG
```

Or add via web UI in the same Secrets tab.

### Step 4: Add Workflow to Repository (2 minutes)

#### 4.1 Generate Workflow File

In each repository where you want to run dep-updater:

```bash
cd /path/to/your/repository
npx @smoothbricks/dep-updater generate-workflow
```

This creates `.github/workflows/update-deps.yml` with runtime auth detection. The workflow will automatically use GitHub
App authentication since you have `DEP_UPDATER_APP_ID` configured.

**AI Changelog Analysis** is enabled by default using the free OpenCode provider. To disable:

- Set repository variable `DEP_UPDATER_SKIP_AI=true`

#### 4.2 Commit and Push

```bash
git add .github/workflows/update-deps.yml
git commit -m "chore: add dep-updater workflow"
git push
```

### Step 5: Validate Setup (2 minutes)

Run the validation command to verify everything is configured correctly:

```bash
npx @smoothbricks/dep-updater validate-setup
```

This will check:

- ✓ GitHub CLI is installed and authenticated
- ✓ Repository has GitHub App installed
- ✓ App has required permissions
- ✓ Can generate tokens successfully
- ✓ Configuration file is valid

If validation passes, you're ready to go!

### Step 6: Test the Workflow (1 minute)

#### Manual Trigger

Trigger the workflow manually to test:

```bash
gh workflow run update-deps.yml
```

Or use the GitHub UI:

1. Go to **Actions** tab
2. Click **Update Dependencies** workflow
3. Click **Run workflow** → **Run workflow**

#### Monitor Execution

```bash
gh run watch
```

Or check the Actions tab in GitHub UI.

### GitHub App Setup Complete! 🎉

**Expected behavior:**

1. New branch created: `chore/update-deps-YYYY-MM-DD`
2. Dependencies analyzed and updated
3. PR created with changelog analysis
4. CI workflows triggered automatically ✅

---

## What Happens Next?

After setup is complete:

1. **Daily at 2 AM UTC**: Workflow runs automatically
2. **Checks for updates**: Scans npm, Expo SDK, and Nix packages
3. **Creates PRs**: One PR per update (or stacked PRs if enabled)
4. **Adds details**: Each PR includes changelog and version info
5. **Awaits review**: You review and merge when ready

## Nix/devenv Support

For repositories using [devenv](https://devenv.sh) for development environments, dep-updater automatically handles Nix
tooling.

### Automatic Installation

The generated workflow **automatically installs Nix tooling** when `devenv.yaml` is detected in your repository:

- **Nix installer** -
  [DeterminateSystems/nix-installer-action](https://github.com/DeterminateSystems/nix-installer-action)
- **Nix cache** -
  [DeterminateSystems/magic-nix-cache-action](https://github.com/DeterminateSystems/magic-nix-cache-action)
- **devenv & nvfetcher** - Installed via `nix profile install`

No manual configuration required - just add a `devenv.yaml` to your repo and the workflow handles the rest.

### What Gets Updated

When Nix is enabled, dep-updater can update:

1. **devenv.lock** - Updates all Nix flake inputs (nixpkgs, git-hooks, etc.)
2. **nixpkgs overlays** - Updates packages tracked by nvfetcher in `_sources/`

### Derivation Diffing with dix

For accurate package version detection, dep-updater uses [dix](https://github.com/faukah/dix) to compare Nix derivation
closures. This shows actual package version changes (e.g., `python3 3.13.8 → 3.13.9`) rather than just commit hash
changes.

dix is fetched on-demand via `nix shell github:faukah/dix` - no installation required.

### GitHub API Rate Limits

nvfetcher queries GitHub releases, which may hit API rate limits. The workflow automatically passes `GH_TOKEN` to
nvfetcher for authenticated requests (15,000 req/hour vs 60 unauthenticated).

## Configuration (Optional)

Create `tooling/dep-updater.json` to customize behavior:

```json
{
  "expo": {
    "enabled": true,
    "autoDetect": true
  },
  "nix": {
    "enabled": true
  },
  "prStrategy": {
    "stackingEnabled": true,
    "maxStackDepth": 5
  }
}
```

Or run the interactive setup wizard:

```bash
npx @smoothbricks/dep-updater init
```

📖 **See [Configuration Reference](./CONFIGURATION.md) for all options and examples.**

## Troubleshooting

### Common Issues

**Workflow fails with authentication errors:**

- PAT: Verify `DEP_UPDATER_TOKEN` secret exists and has correct scope
- GitHub App: Check App ID and private key are correct

**PRs don't trigger CI workflows:**

- This is expected with PAT authentication
- Upgrade to GitHub App for automatic CI triggering
- Or manually trigger CI: `gh workflow run tests.yml --ref branch-name`

**Rate limit exceeded:**

- PAT: 5,000 requests/hour limit
- Upgrade to GitHub App for 15,000 requests/hour
- Or reduce update frequency in workflow schedule

📖 **See [Troubleshooting Guide](./TROUBLESHOOTING.md) for complete solutions to all common issues.**

## Upgrading from PAT to GitHub App

Already using PAT and want to upgrade? With the unified workflow, **no regeneration needed**:

1. Follow [GitHub App Setup](#option-b-github-app-setup-15-20-minutes) steps 1-3 (create app, install, add secrets)
2. Add the organization variable: `DEP_UPDATER_APP_ID`
3. Add the organization secret: `DEP_UPDATER_APP_PRIVATE_KEY`
4. **That's it!** The workflow will automatically use GitHub App on next run

The workflow detects auth method at runtime:

- If `DEP_UPDATER_APP_ID` is set → GitHub App (priority)
- Otherwise → PAT fallback

**Optional cleanup:** Delete old `DEP_UPDATER_TOKEN` secret after confirming GitHub App works.

## Security Best Practices

### For PAT

- Rotate tokens every 90 days (GitHub enforces this)
- Use organization-level secrets (not repository-level)
- Set minimum required scope (`repo` only)
- Document token owner in case of team member changes

### For GitHub App

- Rotate private keys every 90 days:
  1. Generate new key from GitHub App settings
  2. Update organization secret with new key
  3. Wait 24 hours, then revoke old key
  4. GitHub supports multiple active keys for zero downtime
- Use organization-level secrets for easier management
- Limit app installation to only repositories that need it
- Regularly audit app permissions and installations
- Monitor rate limit usage: `gh api rate_limit`

## Next Steps

After successful setup:

1. **Wait for first run** (scheduled or manual trigger)
2. **Review first PR** created by the bot
3. **Merge if looks good** - this will trigger CI as expected (GitHub App only)
4. **Adjust configuration** if needed ([Configuration Reference](./CONFIGURATION.md))
5. **Roll out to other repositories** (just add workflow file)

## Additional Resources

- **[Configuration Reference](./CONFIGURATION.md)** - All configuration options and examples
- **[API Reference](./API.md)** - Programmatic usage and TypeScript types
- **[Troubleshooting Guide](./TROUBLESHOOTING.md)** - Solutions to common issues
- **[README](../README.md)** - Main documentation and feature overview

## Getting Help

If you encounter issues:

1. Run validation: `dep-updater validate-setup` (GitHub App only)
2. Check workflow logs in GitHub Actions tab
3. Review [Troubleshooting Guide](./TROUBLESHOOTING.md)
4. Open an issue: https://github.com/smoothbricks/smoothbricks/issues

---

Congratulations! Your dep-updater is now set up and ready to keep your dependencies up to date automatically.
