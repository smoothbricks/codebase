# LMAO docs site (Astro Starlight)

This folder contains the Astro/Starlight documentation site for `@smoothbricks/lmao`.

## Local development

From the repo root:

    bun install
    cd lmao-docs
    bun run dev
.
├── public/
├── src/
│   ├── assets/
│   ├── content/
│   │   └── docs/
│   └── content.config.ts
├── astro.config.mjs
├── package.json
└── tsconfig.json
```

Starlight looks for `.md` or `.mdx` files in the `src/content/docs/` directory. Each file is exposed as a route based on its file name.

Images can be added to `src/assets/` and embedded in Markdown with a relative link.

Static assets, like favicons, can be placed in the `public/` directory.

## Commands

All commands are run from the repo root unless noted:

| Command | Action |
| :------------------------------ | :----------------------------------------------- |
| `bun install` | Installs dependencies |
| `cd lmao-docs && bun run dev` | Starts local dev server at `localhost:4321` |
| `nx run lmao-docs:astro-html` | Build the production site to `lmao-docs/dist/` |
| `cd lmao-docs && bun run preview` | Preview your build locally, before deploying |
## 👀 Want to learn more?

Check out [Starlight’s docs](https://starlight.astro.build/), read [the Astro documentation](https://docs.astro.build), or jump into the [Astro Discord server](https://astro.build/chat).
