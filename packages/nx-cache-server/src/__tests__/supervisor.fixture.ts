import { fileURLToPath } from 'node:url';

const main = fileURLToPath(new URL('../main.ts', import.meta.url));
const flags = process.argv.includes('--exit-on-stdin-eof') ? ['--exit-on-stdin-eof'] : [];

const child = Bun.spawn([process.execPath, main, ...flags], {
  env: process.env,
  stdin: 'pipe',
  stdout: 'pipe',
  stderr: 'inherit',
});

await Bun.write(Bun.stdout, `${JSON.stringify({ backendPid: child.pid })}\n`);
await Bun.write(Bun.stdout, new Response(child.stdout));
process.exit(await child.exited);
