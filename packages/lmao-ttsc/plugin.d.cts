import type { ITtscPlugin, ITtscPluginFactoryContext } from 'ttsc';

declare function createLmaoTtscPlugin(context: ITtscPluginFactoryContext): ITtscPlugin;

export = createLmaoTtscPlugin;
