'use strict';
// The port-zero preload's test (cowshed-core src/port_zero.rs). Each reach scenario binds a port-0 listener the way a
// real tool does and connects to it from the same workspace: it must land on an odd-offset service port of the block
// in COWSHED_PORT_BASE/COWSHED_PORT_BLOCK_SIZE, and the connect must succeed, which under a workspace's sandbox it can
// only do inside the block. The crowded and exhausted scenarios never fill that shared block: they claim an eight-port
// block of their own outside every workspace range and run a child under it. A child's arguments that are not its
// runtime's own inspector option must reach it unchanged. Prints one line per scenario and exits non-zero if any failed.
const net = require('node:net');
const http = require('node:http');
const { spawn } = require('node:child_process');
const { writeFileSync } = require('node:fs');
const { join } = require('node:path');

const runtime = process.versions.bun === undefined ? 'node' : 'bun';
const base = Number(process.env.COWSHED_PORT_BASE);
const size = Number(process.env.COWSHED_PORT_BLOCK_SIZE);
const node = process.env.PORT_ZERO_NODE;
const bun = process.env.PORT_ZERO_BUN;
const servicePort = (port) => port > base && port < base + size && (port - base) % 2 === 1;
const inspectZero = '--inspect=127.0.0.1:0';

const dial = (port, host) =>
  new Promise((resolve) => {
    const socket = net.connect(port, host);
    socket.on('connect', () => {
      socket.destroy();
      resolve('connected');
    });
    socket.on('error', (error) => resolve(error.code));
  });
const listening = (server, ...args) =>
  new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(...args, () => resolve(server.address().port));
  });
const closed = (server) => new Promise((resolve) => server.close(resolve));
/** A listener on `port` that its own process dialed: the scenario passes when both hold. */
const reached = async (port, host = '127.0.0.1') => {
  const result = await dial(port, host);
  return { passed: result === 'connected' && servicePort(port), detail: `port ${port} ${result}` };
};

/** What a child prints on stdout, once it has exited. */
const printed = (program, args, env = process.env) =>
  new Promise((resolve) => {
    const child = spawn(program, args, { env, stdio: ['ignore', 'pipe', 'inherit'] });
    let text = '';
    child.stdout.on('data', (chunk) => {
      text += chunk;
    });
    child.on('close', () => resolve(text.trim()));
  });

/** What a child writes on stderr, once it has exited. */
const stderrOf = (program, args) =>
  new Promise((resolve) => {
    const child = spawn(program, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let text = '';
    child.stderr.on('data', (chunk) => {
      text += chunk;
    });
    child.on('close', () => resolve(text.trim()));
  });

/**
 * Starts `program` with an inspector on port 0 after `leading` options, alive for as long as its stdin pipe is open,
 * and dials the port its banner names; or resolves the spawn's own error.
 */
const inspector = (program, leading = []) =>
  new Promise((resolve) => {
    let child;
    try {
      child = spawn(program, [...leading, inspectZero, '-e', 'process.stdin.resume()'], {
        stdio: ['pipe', 'ignore', 'pipe'],
      });
    } catch (error) {
      resolve({ error: error.code ?? error.message });
      return;
    }
    let text = '';
    child.on('error', (error) => resolve({ error: error.code ?? error.message }));
    child.stderr.on('data', (chunk) => {
      text += chunk;
      const banner = /ws:\/\/[^\s/]+:(\d+)\//.exec(text);
      if (banner === null) return;
      child.stderr.removeAllListeners('data');
      const port = Number(banner[1]);
      dial(port, '127.0.0.1').then((result) => {
        child.stdin.end();
        resolve({ port, result });
      });
    });
    child.on('exit', () => resolve({ error: `exited: ${text.trim()}` }));
  });
const inspected = async (program, leading) => {
  const outcome = await inspector(program, leading);
  if (outcome.error !== undefined) return { passed: false, detail: outcome.error };
  return { passed: outcome.result === 'connected' && servicePort(outcome.port), detail: `port ${outcome.port}` };
};

const argvScript = join(__dirname, 'print-argument.cjs');
writeFileSync(argvScript, 'console.log(process.argv[2]);\n');
const emptyModule = join(__dirname, 'empty-module.cjs');
writeFileSync(emptyModule, '');
// Run under an owned block: prints the port a port-0 listen took, or its error's code.
const listenScript = join(__dirname, 'listen-zero.cjs');
writeFileSync(
  listenScript,
  `const s = require('node:net').createServer();
s.on('error', (error) => console.log(error.code));
s.listen(0, '127.0.0.1', () => { process.stdout.write(String(s.address().port) + '\\n'); s.close(); });
`,
);
// Spawns a Node child whose inspector follows a bare `--require`, which the preload must not map and must name.
const unmappedScript = join(__dirname, 'spawn-unmapped.cjs');
writeFileSync(
  unmappedScript,
  `require('node:child_process')
  .spawn(process.argv[2], ['--require', process.argv[3], '${inspectZero}', '-e', '0'], { stdio: 'ignore' })
  .on('exit', () => process.exit(0));
`,
);
// Run under an owned block: prints the port the inspector of a child started with port 0 took, or the spawn's error.
// The banner's lines can reach stderr as separate chunks: the first chunk that names the port prints it, once.
const inspectScript = join(__dirname, 'inspect-zero.cjs');
writeFileSync(
  inspectScript,
  `let child;
try {
  child = require('node:child_process').spawn(process.argv[2], ['${inspectZero}', '-e', 'process.stdin.resume()'], {
    stdio: ['pipe', 'ignore', 'pipe'],
  });
} catch (error) {
  console.log(error.code);
  process.exit(0);
}
child.on('error', (error) => console.log(error.code));
let text = '';
child.stderr.on('data', (chunk) => {
  text += chunk;
  const banner = /ws:\\/\\/[^\\s/]+:(\\d+)\\//.exec(text);
  if (banner === null) return;
  child.stderr.removeAllListeners('data');
  console.log(banner[1]);
  child.stdin.end();
});
`,
);

/**
 * An eight-port block this scenario alone holds, in the registered range below every workspace and fixture block and
 * below the kernel's ephemeral range, so no other test or job picks its numbers: every port of it is bound here first.
 * `release(ports)` frees just those, for the child that runs under the block.
 */
const ownedBlock = async () => {
  for (let first = 20_000; first < 32_768; first += 8) {
    const servers = [];
    try {
      for (let port = first; port < first + 8; port++) {
        const server = net.createServer();
        await listening(server, port, '127.0.0.1');
        servers.push(server);
      }
    } catch (error) {
      if (error.code !== 'EADDRINUSE') throw error;
      await Promise.all(servers.map(closed));
      continue;
    }
    const env = { ...process.env, COWSHED_PORT_BASE: String(first), COWSHED_PORT_BLOCK_SIZE: '8' };
    return {
      first,
      env,
      release: (ports) => Promise.all(servers.filter((server) => ports.includes(server.address().port)).map(closed)),
      close: () => Promise.all(servers.filter((server) => server.listening).map(closed)),
    };
  }
  throw new Error('no free eight-port block below 32768');
};
/** `script` run by `program` under an owned block whose service ports are all held except `free`. */
const underOwnedBlock = async (program, script, free, ...args) => {
  const block = await ownedBlock();
  const spared = free.map((offset) => block.first + offset);
  await block.release(spared);
  const seen = await printed(program, [script, ...args], block.env);
  await block.close();
  return { seen, spared };
};

const scenarios = {
  // @stryker-mutator/core's LoggingServer: `server.listen(cb)`, no port and no host; its workers dial localhost.
  async loggingServer() {
    const server = net.createServer((socket) => socket.end());
    const port = await listening(server);
    const outcome = await reached(port, 'localhost');
    await closed(server);
    return outcome;
  },
  // The Bun Stryker runner's sync server: `httpServer.listen(0, '127.0.0.1')`, dialed by its `bun test` child.
  async syncServer() {
    const server = http.createServer((_, response) => response.end());
    const port = await listening(server, 0, '127.0.0.1');
    const outcome = await reached(port);
    await closed(server);
    return outcome;
  },
  // A helper that binds port 0, closes it and hands the number out must not be handed the same port twice.
  async probeThenReuse() {
    const probe = async () => {
      const server = net.createServer();
      const port = await listening(server, { port: 0, host: '127.0.0.1' });
      await closed(server);
      return port;
    };
    const first = await probe();
    const second = await probe();
    if (first === second) return { passed: false, detail: `handed out ${first} twice` };
    const server = net.createServer((socket) => socket.end());
    await listening(server, first, '127.0.0.1');
    const outcome = await reached(first);
    await closed(server);
    return outcome;
  },
  async bunServe() {
    if (runtime !== 'bun') return null;
    const server = Bun.serve({ port: 0, hostname: '127.0.0.1', fetch: () => new Response('ok') });
    const outcome = await reached(server.port);
    server.stop(true);
    return outcome;
  },
  bunInspector: () => inspected(bun),
  nodeInspector: () => inspected(node),
  // `=`-joined options before the inspector are walked; the inspector after them is mapped.
  nodeInspectorAfterRequire: () => inspected(node, [`--require=${emptyModule}`]),
  bunInspectorAfterPreload: () => inspected(bun, [`--preload=${emptyModule}`]),
  // A bare flag stops the scan, whose operand it cannot know: the inspector after it keeps port 0, and the parent says
  // so on stderr, naming the flag and both remedies.
  async unmappedInspectorIsNamed() {
    const said = await stderrOf(process.execPath, [unmappedScript, node, emptyModule]);
    const named = [`${inspectZero} for ${node} was not mapped`, 'the bare flag `--require`', '--loopback-any'];
    return { passed: named.every((text) => said.includes(text)), detail: `parent said ${said}` };
  },
  // With every service port but the lowest held, a port-0 listen takes that one; with all held it fails with the last
  // candidate's own EADDRINUSE, never falling back to port 0.
  async crowdedBlock() {
    const { seen, spared } = await underOwnedBlock(process.execPath, listenScript, [1]);
    return { passed: seen === String(spared[0]), detail: `took ${seen}, free ${spared}` };
  },
  async exhaustedBlock() {
    const { seen } = await underOwnedBlock(process.execPath, listenScript, []);
    return { passed: seen === 'EADDRINUSE', detail: seen };
  },
  // The inspector's probe sees a held port, and a block with none free fails the spawn with EADDRINUSE.
  async crowdedInspector() {
    const { seen, spared } = await underOwnedBlock(process.execPath, inspectScript, [1], node);
    return { passed: seen === String(spared[0]), detail: `took ${seen}, free ${spared}` };
  },
  async exhaustedInspector() {
    const { seen } = await underOwnedBlock(process.execPath, inspectScript, [], bun);
    return { passed: seen === 'EADDRINUSE', detail: seen };
  },
  // Another program's arguments and a runtime's script arguments are data the preload leaves alone.
  echoKeepsItsArguments: async () => unchanged(await printed('echo', [inspectZero])),
  nodeScriptKeepsItsArguments: async () => unchanged(await printed(node, [argvScript, inspectZero])),
  bunScriptKeepsItsArguments: async () => unchanged(await printed(bun, [argvScript, inspectZero])),
  // Bun takes an option's separate operand even when it starts with a dash: here the inspector text is a preload path,
  // and the bare `--preload` before it stops the scan. (Node refuses such an operand: `--title requires an argument`.)
  async bunPreloadKeepsItsOperand() {
    const said = await stderrOf(bun, ['--preload', inspectZero, '-e', '1']);
    return { passed: said.includes(`preload not found "${inspectZero}"`), detail: `bun said ${said}` };
  },
};
function unchanged(seen) {
  return { passed: seen === inspectZero, detail: `child saw ${seen}` };
}

(async () => {
  let failed = 0;
  for (const [name, scenario] of Object.entries(scenarios)) {
    let outcome;
    try {
      outcome = await scenario();
    } catch (error) {
      outcome = { passed: false, detail: `threw ${error.code ?? error.message}` };
    }
    if (outcome === null) continue;
    if (!outcome.passed) failed += 1;
    console.log(`${runtime} ${name}: ${outcome.detail} ${outcome.passed ? 'PASS' : 'FAIL'}`);
  }
  process.exit(failed === 0 ? 0 : 1);
})();
