/**
 * Directories no source walk descends into. Every walker in this plugin runs on
 * project load, so descending into a build output is paid on each graph
 * computation: a `target/` at 50k files or a `dist-test/` left by an old config
 * dominated the load. Anything a build writes is excluded by shape rather than
 * by an enumerated name: every `dist*` variant, cargo's `target`, and any
 * dot-directory (`.runtime`, `.cache`, `.venv`, `.git`, tooling state) — a
 * source tree never starts with a dot.
 */
export function isNonSourceDirectory(name: string): boolean {
  return (
    name.startsWith('.') ||
    name === 'node_modules' ||
    name === 'target' ||
    name === 'coverage' ||
    name === 'build' ||
    name === '__pycache__' ||
    name === 'dist' ||
    name.startsWith('dist-')
  );
}
