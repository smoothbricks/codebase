//! Agent-harness skill directories, generated from an upstream snapshot.
//!
//! DO NOT EDIT. Regenerate with `nx run cowshed:refresh-harnesses`.
//!
//! Upstream:  https://github.com/vercel-labs/skills
//! Source:    src/agents.ts
//! Revision:  c6f69c631292444cc541ac6d91e2226b0ff247da
//!
//! Paths are relative to the install base: the home directory for the global
//! scope, the repository root for the project scope. `configHome` is resolved
//! as `.config`, its default when XDG_CONFIG_HOME is unset.
//!
//! Entries whose paths or detection probe do not reduce to a literal home
//! path are skipped rather than guessed (8 of 76):
//!   - eve: globalSkillsDir is not a literal home path (undefined)
//!   - kimchi: detectInstalled has no literal home-relative probe
//!   - minimax-code: detectInstalled has no literal home-relative probe
//!   - openclaw: globalSkillsDir is not a literal home path (getOpenClawGlobalSkillsDir())
//!   - promptscript: globalSkillsDir is not a literal home path (undefined)
//!   - replit: detectInstalled has no literal home-relative probe
//!   - universal: detectInstalled has no literal home-relative probe
//!   - zcode: detectInstalled has no literal home-relative probe

use super::HarnessEntry;

/// The upstream snapshot. Entries in `VERIFIED_HARNESSES` override these by name.
pub const GENERATED_HARNESSES: &[HarnessEntry] = &[
    HarnessEntry {
        name: "adal",
        global_root: ".adal",
        global_skills: ".adal/skills",
        project_skills: ".adal/skills",
    },
    HarnessEntry {
        name: "aider-desk",
        global_root: ".aider-desk",
        global_skills: ".aider-desk/skills",
        project_skills: ".aider-desk/skills",
    },
    HarnessEntry {
        name: "amp",
        global_root: ".config/amp",
        global_skills: ".config/agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "antigravity",
        global_root: ".gemini/antigravity",
        global_skills: ".gemini/antigravity/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "antigravity-cli",
        global_root: ".gemini/antigravity-cli",
        global_skills: ".gemini/antigravity-cli/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "astrbot",
        global_root: ".astrbot",
        global_skills: ".astrbot/data/skills",
        project_skills: "data/skills",
    },
    HarnessEntry {
        name: "augment",
        global_root: ".augment",
        global_skills: ".augment/skills",
        project_skills: ".augment/skills",
    },
    HarnessEntry {
        name: "autohand-code",
        global_root: ".autohand",
        global_skills: ".autohand/skills",
        project_skills: ".autohand/skills",
    },
    HarnessEntry {
        name: "bob",
        global_root: ".bob",
        global_skills: ".bob/skills",
        project_skills: ".bob/skills",
    },
    HarnessEntry {
        name: "claude-code",
        global_root: ".claude",
        global_skills: ".claude/skills",
        project_skills: ".claude/skills",
    },
    HarnessEntry {
        name: "cline",
        global_root: ".cline",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "codearts-agent",
        global_root: ".codeartsdoer",
        global_skills: ".codeartsdoer/skills",
        project_skills: ".codeartsdoer/skills",
    },
    HarnessEntry {
        name: "codebuddy",
        global_root: ".codebuddy",
        global_skills: ".codebuddy/skills",
        project_skills: ".codebuddy/skills",
    },
    HarnessEntry {
        name: "codemaker",
        global_root: ".codemaker",
        global_skills: ".codemaker/skills",
        project_skills: ".codemaker/skills",
    },
    HarnessEntry {
        name: "codestudio",
        global_root: ".codestudio",
        global_skills: ".codestudio/skills",
        project_skills: ".codestudio/skills",
    },
    HarnessEntry {
        name: "codex",
        global_root: ".codex",
        global_skills: ".codex/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "command-code",
        global_root: ".commandcode",
        global_skills: ".commandcode/skills",
        project_skills: ".commandcode/skills",
    },
    HarnessEntry {
        name: "continue",
        global_root: ".continue",
        global_skills: ".continue/skills",
        project_skills: ".continue/skills",
    },
    HarnessEntry {
        name: "cortex",
        global_root: ".snowflake/cortex",
        global_skills: ".snowflake/cortex/skills",
        project_skills: ".cortex/skills",
    },
    HarnessEntry {
        name: "crush",
        global_root: ".config/crush",
        global_skills: ".config/crush/skills",
        project_skills: ".crush/skills",
    },
    HarnessEntry {
        name: "cursor",
        global_root: ".cursor",
        global_skills: ".cursor/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "deepagents",
        global_root: ".deepagents",
        global_skills: ".deepagents/agent/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "devin",
        global_root: ".config/devin",
        global_skills: ".config/devin/skills",
        project_skills: ".devin/skills",
    },
    HarnessEntry {
        name: "dexto",
        global_root: ".dexto",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "droid",
        global_root: ".factory",
        global_skills: ".factory/skills",
        project_skills: ".factory/skills",
    },
    HarnessEntry {
        name: "firebender",
        global_root: ".firebender",
        global_skills: ".firebender/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "forgecode",
        global_root: ".forge",
        global_skills: ".forge/skills",
        project_skills: ".forge/skills",
    },
    HarnessEntry {
        name: "gemini-cli",
        global_root: ".gemini",
        global_skills: ".gemini/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "github-copilot",
        global_root: ".copilot",
        global_skills: ".copilot/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "goose",
        global_root: ".config/goose",
        global_skills: ".config/goose/skills",
        project_skills: ".goose/skills",
    },
    HarnessEntry {
        name: "grok",
        global_root: ".grok",
        global_skills: ".grok/skills",
        project_skills: ".grok/skills",
    },
    HarnessEntry {
        name: "hermes-agent",
        global_root: ".hermes",
        global_skills: ".hermes/skills",
        project_skills: ".hermes/skills",
    },
    HarnessEntry {
        name: "iflow-cli",
        global_root: ".iflow",
        global_skills: ".iflow/skills",
        project_skills: ".iflow/skills",
    },
    HarnessEntry {
        name: "inference-sh",
        global_root: ".inferencesh",
        global_skills: ".inferencesh/skills",
        project_skills: ".inferencesh/skills",
    },
    HarnessEntry {
        name: "jazz",
        global_root: ".jazz",
        global_skills: ".jazz/skills",
        project_skills: ".jazz/skills",
    },
    HarnessEntry {
        name: "junie",
        global_root: ".junie",
        global_skills: ".junie/skills",
        project_skills: ".junie/skills",
    },
    HarnessEntry {
        name: "kilo",
        global_root: ".kilocode",
        global_skills: ".kilocode/skills",
        project_skills: ".kilocode/skills",
    },
    HarnessEntry {
        name: "kimi-code-cli",
        global_root: ".kimi-code",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "kiro-cli",
        global_root: ".kiro",
        global_skills: ".kiro/skills",
        project_skills: ".kiro/skills",
    },
    HarnessEntry {
        name: "kode",
        global_root: ".kode",
        global_skills: ".kode/skills",
        project_skills: ".kode/skills",
    },
    HarnessEntry {
        name: "lingma",
        global_root: ".lingma",
        global_skills: ".lingma/skills",
        project_skills: ".lingma/skills",
    },
    HarnessEntry {
        name: "loaf",
        global_root: ".loaf",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "mcpjam",
        global_root: ".mcpjam",
        global_skills: ".mcpjam/skills",
        project_skills: ".mcpjam/skills",
    },
    HarnessEntry {
        name: "mistral-vibe",
        global_root: ".vibe",
        global_skills: ".vibe/skills",
        project_skills: ".vibe/skills",
    },
    HarnessEntry {
        name: "moxby",
        global_root: ".moxby",
        global_skills: ".moxby/skills",
        project_skills: ".moxby/skills",
    },
    HarnessEntry {
        name: "mux",
        global_root: ".mux",
        global_skills: ".mux/skills",
        project_skills: ".mux/skills",
    },
    HarnessEntry {
        name: "neovate",
        global_root: ".neovate",
        global_skills: ".neovate/skills",
        project_skills: ".neovate/skills",
    },
    HarnessEntry {
        name: "ona",
        global_root: ".ona",
        global_skills: ".ona/skills",
        project_skills: ".ona/skills",
    },
    HarnessEntry {
        name: "opencode",
        global_root: ".config/opencode",
        global_skills: ".config/opencode/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "openhands",
        global_root: ".openhands",
        global_skills: ".openhands/skills",
        project_skills: ".openhands/skills",
    },
    HarnessEntry {
        name: "pi",
        global_root: ".pi/agent",
        global_skills: ".pi/agent/skills",
        project_skills: ".pi/skills",
    },
    HarnessEntry {
        name: "pochi",
        global_root: ".pochi",
        global_skills: ".pochi/skills",
        project_skills: ".pochi/skills",
    },
    HarnessEntry {
        name: "qoder",
        global_root: ".qoder",
        global_skills: ".qoder/skills",
        project_skills: ".qoder/skills",
    },
    HarnessEntry {
        name: "qoder-cn",
        global_root: ".qoder-cn",
        global_skills: ".qoder-cn/skills",
        project_skills: ".qoder/skills",
    },
    HarnessEntry {
        name: "qwen-code",
        global_root: ".qwen",
        global_skills: ".qwen/skills",
        project_skills: ".qwen/skills",
    },
    HarnessEntry {
        name: "reasonix",
        global_root: ".reasonix",
        global_skills: ".reasonix/skills",
        project_skills: ".reasonix/skills",
    },
    HarnessEntry {
        name: "roo",
        global_root: ".roo",
        global_skills: ".roo/skills",
        project_skills: ".roo/skills",
    },
    HarnessEntry {
        name: "rovodev",
        global_root: ".rovodev",
        global_skills: ".rovodev/skills",
        project_skills: ".rovodev/skills",
    },
    HarnessEntry {
        name: "tabnine-cli",
        global_root: ".tabnine",
        global_skills: ".tabnine/agent/skills",
        project_skills: ".tabnine/agent/skills",
    },
    HarnessEntry {
        name: "terramind",
        global_root: ".terramind",
        global_skills: ".terramind/skills",
        project_skills: ".terramind/skills",
    },
    HarnessEntry {
        name: "tinycloud",
        global_root: ".tinycloud",
        global_skills: ".tinycloud/skills",
        project_skills: ".tinycloud/skills",
    },
    HarnessEntry {
        name: "trae",
        global_root: ".trae",
        global_skills: ".trae/skills",
        project_skills: ".trae/skills",
    },
    HarnessEntry {
        name: "trae-cn",
        global_root: ".trae-cn",
        global_skills: ".trae-cn/skills",
        project_skills: ".trae/skills",
    },
    HarnessEntry {
        name: "warp",
        global_root: ".warp",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "windsurf",
        global_root: ".codeium/windsurf",
        global_skills: ".codeium/windsurf/skills",
        project_skills: ".windsurf/skills",
    },
    HarnessEntry {
        name: "zed",
        global_root: ".config/zed",
        global_skills: ".agents/skills",
        project_skills: ".agents/skills",
    },
    HarnessEntry {
        name: "zencoder",
        global_root: ".zencoder",
        global_skills: ".zencoder/skills",
        project_skills: ".zencoder/skills",
    },
    HarnessEntry {
        name: "zenflow",
        global_root: ".zencoder",
        global_skills: ".zencoder/skills",
        project_skills: ".zencoder/skills",
    },
];
