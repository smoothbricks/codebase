import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { defineCollection, z } from 'astro:content';

export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({
      // `related`: optional hand-curated cross-links, rendered by RelatedPages in Phase 2.
      extend: z.object({
        related: z.array(z.string()).optional(),
      }),
    }),
  }),
};
