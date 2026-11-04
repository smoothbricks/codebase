// A linked ttsc plugin for the nx-plugin's tests: a ProgramPlugin that lowers a dialect construct the stock checker
// refuses, and a DiagnosticPlugin that withdraws the checker's refusal of exactly that construct. ttsc links its Go
// package into its utility host, or into typia's host when the project uses typia.
const path = require('node:path');

module.exports = function createDialectPlugin(context) {
  return {
    name: 'dialect-plugin',
    source: path.resolve(context.dirname, 'plugin', 'dialect'),
    stage: 'transform',
  };
};
