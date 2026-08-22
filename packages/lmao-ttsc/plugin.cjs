// ttsc plugin descriptor for @smoothbricks/lmao-ttsc.
//
// ttsc discovers this through the package's `ttsc.plugin` field when the
// package is a direct dependency, or through explicit compiler plugin config:
//   { "compilerOptions": { "plugins": [{ "transform": "@smoothbricks/lmao-ttsc" }] } }
//
// The Go transform lives in ./plugin/driver as a non-`main` package and is
// built on the consumer's machine by ttsc (cached by ttsc version, tsgo
// version, platform, and plugin source hash). Naming the library package —
// rather than the ./plugin/host sidecar `main` beside it — is what makes ttsc
// link the transform into a shared compiler host instead of spawning a second
// executable transform host, which could not share the emit pass with sibling
// plugins. Bun build and runtime hosts reach the same registration through
// @ttsc/unplugin; there is no parallel JS transformer.
const path = require('node:path');

module.exports = function createLmaoTtscPlugin(context) {
  return {
    name: '@smoothbricks/lmao-ttsc',
    source: path.resolve(context.dirname, 'plugin', 'driver'),
    stage: 'transform',
  };
};
