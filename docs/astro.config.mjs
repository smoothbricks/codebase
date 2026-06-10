// @ts-check
import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import starlightLinksValidator from 'starlight-links-validator';

export default defineConfig({
	integrations: [
		starlight({
			title: 'LMAO',
			logo: { src: './src/assets/logo.svg', alt: 'LMAO' },
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/smoothbricks/codebase' }],
			editLink: { baseUrl: 'https://github.com/smoothbricks/codebase/edit/main/docs/' },
			lastUpdated: true,
			customCss: ['./src/styles/custom.css'],
			sidebar: [
				{ label: 'Start Here', items: [{ autogenerate: { directory: 'start-here' } }] },
				{ label: 'Tutorials', collapsed: true, items: [{ autogenerate: { directory: 'tutorials' } }] },
				{ label: 'How-to guides', collapsed: true, items: [{ autogenerate: { directory: 'guides' } }] },
				{ label: 'Concepts', collapsed: true, items: [{ autogenerate: { directory: 'concepts' } }] },
				{ label: 'Reference', collapsed: true, items: [{ autogenerate: { directory: 'reference' } }] },
				{ label: 'Roadmap', link: '/roadmap/', badge: { text: 'Future', variant: 'caution' } },
			],
			plugins: [starlightLinksValidator()],
		}),
	],
});
