// ttsc plugin descriptor for @smoothbricks/lmao-transformer.
//
// ttsc (samchon/ttsc, the typescript-go plugin host) discovers this via the
// `ttsc.plugin` field in package.json, or via explicit registration:
//   { "compilerOptions": { "plugins": [{ "transform": "@smoothbricks/lmao-transformer" }] } }
//
// The Go plugin source lives in ./plugin and is built on the consumer's
// machine by ttsc (cached by ttsc version, tsgo version, platform, and
// plugin source hash). The classic TypeScript transformer entry
// (createLmaoTransformer, dist/index.js) remains the canonical
// implementation for tsc/bun hosts — both implement spec 01o.
const path = require('node:path');

module.exports = function createLmaoTtscPlugin(context) {
  return {
    name: '@smoothbricks/lmao-transformer',
    source: path.resolve(context.dirname, 'plugin'),
    stage: 'transform',
  };
};
