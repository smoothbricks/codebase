const formatter = require('eslint-friendly-formatter');

/**
 * Custom eslint formatter that outputs only fixed output to stdout. If you run
 * eslint on multiple files, outputs for fixed files are separated by null
 * characters. When run on a file that eslint is configured to ignore the
 * formatter will produce no output.
 */
module.exports = (results) => {
  // output: The source code for the given file with as many fixes applied as possible.
  //         This property is omitted if no fix is available.
  // - https://eslint.org/docs/latest/extend/custom-formatters#the-result-object
  const output = results.map((r) => r.output && r.output.trim()).join('\0');
  process.stdout.write(output);

  // Return human readable formatted errors if any:
  return formatter(results);
};
