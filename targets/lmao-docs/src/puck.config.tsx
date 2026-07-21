import type { Config } from '@puckeditor/core';

/**
 * Puck visual-editor config.
 *
 * `conlocaCMS()` requires a `puckConfigPath`, but this site does not author
 * Puck-built pages — the CMS is here purely to edit the existing Starlight
 * MDX docs, which are wired up via `mdxPages` in `content/sites.json`.
 * So the component palette is intentionally empty; add components here only
 * if we later want drag-and-drop pages alongside the docs.
 */
const puckConfig: Config = {
  components: {},
};

export default puckConfig;
