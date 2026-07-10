// @ts-check
import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import remarkGfm from 'remark-gfm';
import starlightLinksValidator from 'starlight-links-validator';
import starlightLlmsTxt from 'starlight-llms-txt';

export default defineConfig({
	// PLACEHOLDER — hosting is not yet decided. Update `site` to the real docs
	// domain at deploy time: it is baked into the generated /llms.txt URLs,
	// canonical links, and the sitemap.
	site: 'https://lmao.smoothbricks.dev',
	// Astro 6 no longer applies GFM through the default markdown pipeline in this
	// Starlight setup, which silently broke every Markdown table (they rendered as
	// literal `| … |` text). Re-add remark-gfm explicitly so tables/strikethrough work.
	markdown: {
		remarkPlugins: [remarkGfm],
	},
	integrations: [
		starlight({
			title: 'LMAO',
			logo: { src: './src/assets/logo.svg', alt: 'LMAO' },
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/smoothbricks/codebase' }],
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{ label: 'Start Here', items: [{ autogenerate: { directory: 'start-here' } }] },
				{ label: 'Tutorials', collapsed: true, items: [{ autogenerate: { directory: 'tutorials' } }] },
				{ label: 'How-to guides', collapsed: true, items: [{ autogenerate: { directory: 'guides' } }] },
				{ label: 'Concepts', collapsed: true, items: [{ autogenerate: { directory: 'concepts' } }] },
				{ label: 'Reference', collapsed: true, items: [{ autogenerate: { directory: 'reference' } }] },
				{ label: 'Roadmap', link: '/roadmap/', badge: { text: 'Future', variant: 'caution' } },
			],
			plugins: [
				// The /llms*.txt routes are emitted by starlight-llms-txt at build time, so
				// they are not in the content collection — exclude them from link validation.
				starlightLinksValidator({ exclude: ['/llms.txt', '/llms-full.txt', '/llms-small.txt', '/_llms-txt/**'] }),
				starlightLlmsTxt({
					projectName: 'LMAO',
					description:
						'A high-performance, type-safe structured tracing and observability library for TypeScript. ' +
						'Instrumented code writes spans directly into columnar Apache Arrow buffers (near-zero hot-path ' +
						'overhead) and emits queryable Arrow tables that persist to SQLite (local file, Node, or Cloudflare D1).',
					details: [
						'The primary API is `defineOpContext({ logSchema, flags?, deps? })` -> `defineOp(name, fn)`, where the',
						'op body receives a `SpanContext` (`tag`, `log`, `span`, `ff`, `deps`, `setScope`, `ok`, `err`).',
						'Tracers (`StdioTracer`, `SQLiteTracer`, `ArrayQueueTracer`, `CompositeTracer`, `TestTracer`) require a',
						'`bufferStrategy` (`JsBufferStrategy`) and a `createTraceRoot` from `@smoothbricks/lmao/node` (Node) or',
						'`@smoothbricks/lmao/es` (browsers/Workers). A headline feature is trace-testing: assert on the emitted',
						'span tree rather than return values.',
					].join(' '),
					optionalLinks: [
						{
							label: 'GitHub repository',
							url: 'https://github.com/smoothbricks/codebase',
							description: 'Source for @smoothbricks/lmao and the monorepo.',
						},
					],
					promote: ['index*', 'start-here/**'],
					demote: ['roadmap', '404'],
					// Independently fetchable subsets for context-limited tools.
					customSets: [
						{
							label: 'Reference only',
							paths: ['reference/**'],
							description: 'Complete API reference for @smoothbricks/lmao.',
						},
						{
							label: 'Start Here + Tutorials',
							paths: ['start-here/**', 'tutorials/**'],
							description: 'Installation, quickstart, core concepts, and step-by-step tutorials.',
						},
					],
				}),
			],
		}),
	],
});
