// A linked ttsc plugin for the typescript-emit executor's tests: a transform-stage ProgramPlugin whose Go package ttsc
// links into its utility host, as it links a census-writing lint plugin.
const path = require('node:path');

module.exports = function createSideOutputPlugin(context) {
  return {
    name: 'side-output-plugin',
    source: path.resolve(context.dirname, 'plugin', 'sideoutput'),
    stage: 'transform',
  };
};
