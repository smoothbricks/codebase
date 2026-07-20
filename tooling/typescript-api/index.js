// Re-export the real TypeScript 6 programmatic API.
// Dependency is intentionally NOT named "typescript" — Bun rewrites nested
// npm:typescript@^6 onto a parent package named typescript (bun#33834).
module.exports = require('ts-compiler-api');
