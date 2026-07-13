// ttsc plugin descriptor for @smoothbricks/lmao-ttsc.
//
// ttsc discovers this through the package's `ttsc.plugin` field when the
// package is a direct dependency, or through explicit compiler plugin config:
//   { "compilerOptions": { "plugins": [{ "transform": "@smoothbricks/lmao-ttsc" }] } }
//
// The Go plugin source lives in ./plugin and is built on the consumer's
// machine by ttsc (cached by ttsc version, tsgo version, platform, and
// plugin source hash). Bun build and runtime hosts invoke this same native
// implementation through @ttsc/unplugin; there is no parallel JS transformer.
const path = require('node:path');

module.exports = function createLmaoTtscPlugin(context) {
  return {
    name: '@smoothbricks/lmao-ttsc',
    source: path.resolve(context.dirname, 'plugin'),
    stage: 'transform',
  };
};
