# `@smoothbricks/lmao` documentation site

The [Astro](https://astro.build/) + [Starlight](https://starlight.astro.build/) documentation site for
`@smoothbricks/lmao`. Content lives in `src/content/docs/`; the production build is a fully static site.

- **Stack:** Astro 6 · Starlight 0.39 · Bun · Nx (project name `lmao-docs`)
- **Requirements:** Node ≥ 22.12, Bun (this repo is Bun-only — never use `npm`/`npx`)

## Local development

From the repository root:

```bash
bun install
bun run --cwd lmao-docs dev
```

The dev server starts at http://localhost:4321.

## Commands

Run from the repository root:

| Command | Description |
| --- | --- |
| `bun install` | Install all workspace dependencies |
| `bun run --cwd targets/lmao-docs dev` | Start the local dev server |
| `nx astro-html lmao-docs` | Build the static site to `lmao-docs/dist/` |
| `bun run --cwd targets/lmao-docs preview` | Preview the production build locally |

Prefer `nx astro-html lmao-docs` for builds — it is the cached Nx target used in CI.

## Content structure

Starlight turns every `.md`/`.mdx` file under `src/content/docs/` into a route based on its path. The docs follow the
[Diátaxis](https://diataxis.fr/) framework, with one top-level directory per mode:

```txt
src/content/docs/
├── index.mdx          # Splash home (hero + card grid)
├── start-here/        # Orientation: what/why, install, quickstart, core concepts
├── tutorials/         # Learning-oriented, end-to-end walkthroughs
├── guides/            # Task-oriented how-to recipes
├── concepts/          # Understanding-oriented explanation
├── reference/         # Information-oriented API reference
└── roadmap.mdx        # Not-yet-shipped directions (clearly fenced)
```

The sidebar is configured in `astro.config.mjs`: each group autogenerates from its directory, so **adding a page never
requires editing config** — file order is controlled by each page's `sidebar.order` frontmatter.

Runnable, current-API examples for the library live in
[`packages/lmao/examples/`](../packages/lmao/examples) and are linked from the guides.

## Project layout

```txt
lmao-docs/
├── public/             # Static assets served as-is (favicon, etc.)
├── src/
│   ├── assets/         # Imported images (referenced from MDX)
│   ├── components/     # Custom Astro components / overrides
│   ├── content/docs/   # The documentation pages (see above)
│   ├── content.config.ts  # Content collection + schema (docsSchema + `related`)
│   └── styles/custom.css  # Theme tokens + landing-page motion
├── astro.config.mjs    # Astro + Starlight config (sidebar, plugins, editLink)
├── package.json
└── tsconfig.json
```

## Theming

The site uses a **terminal-amber** accent (`--sl-color-accent`, `#d97706`) tied to `StdioTracer` output — so the docs
and the library's console output tell one visual story. Tokens and the splash-page motion (fade/slide reveals, a
blinking cursor, slate-deep backdrop) live in `src/styles/custom.css`; all motion is scoped to the home page and gated
behind `prefers-reduced-motion`.

## Plugins

- **[starlight-llms-txt](https://delucis.github.io/starlight-llms-txt/)** — generates `/llms.txt`, `/llms-full.txt`, and
  `/llms-small.txt` at build time so the docs can be fed directly to AI tools.
- **[starlight-links-validator](https://starlight-links-validator.vercel.app/)** — fails the build on broken internal
  links.

## Deployment

Hosting is not yet decided, so `site` in `astro.config.mjs` is a placeholder
(`https://lmao.smoothbricks.dev`). **Update it to the real domain at deploy time** — it is baked into the generated
`llms.txt` URLs, canonical links, and the sitemap. The build output in `dist/` is static and can be served by any host.

## Learn more

- [Starlight documentation](https://starlight.astro.build/)
- [Astro documentation](https://docs.astro.build/)
