'use strict';
// cowshed's port-0 preload (specs/cowshed/04_sandbox.md, "A sandboxed job binds its listeners in its block").
//
// On macOS a job may connect only to its workspace's port block, so a listener on an OS-picked port binds but nothing in
// the workspace can reach it. Every job runs Node with `NODE_OPTIONS=--require=<this file>` and Bun with
// `BUN_OPTIONS=--preload=<this file>`; this file then gives every port-0 listen a free port of the block instead, and
// rewrites a child's `--inspect…=<host>:0` argument to one, because the inspector binds natively where no listen() is seen.
(() => {
  const loaded = Symbol.for('cowshed.portZero');
  if (globalThis[loaded]) return;
  globalThis[loaded] = true;

  const baseText = process.env.COWSHED_PORT_BASE;
  const sizeText = process.env.COWSHED_PORT_BLOCK_SIZE;
  // Outside a workspace, and on Linux, whose workspaces own a private loopback, no block is exported: nothing to do.
  if (baseText === undefined && sizeText === undefined) return;
  const base = Number(baseText);
  const size = Number(sizeText);
  if (
    !/^[0-9]+$/.test(baseText ?? '') ||
    !/^[0-9]+$/.test(sizeText ?? '') ||
    size < 2 ||
    (size & (size - 1)) !== 0 ||
    base % size !== 0 ||
    base + size > 65536
  ) {
    throw new Error(
      `cowshed: COWSHED_PORT_BASE=${baseText ?? '(unset)'} COWSHED_PORT_BLOCK_SIZE=${sizeText ?? '(unset)'} is not a port block`,
    );
  }

  // `base` is the gateway's own listener, and an even offset is the session port only the holder of the service port
  // below it binds (08_testing.md, the block's claim protocol): the candidates are the odd-offset service ports
  // `base+size-1, base+size-3, … base+1`, highest first, so tool-internal listeners stay away from a test's fixtures,
  // which scan up. Each process and thread starts at its own offset so the processes sharing a block start apart, and
  // every candidate handed out moves the cursor past it: a caller that binds, closes and passes the number on
  // (probe-then-reuse) never gets the same port twice from one process.
  const count = size / 2;
  const { threadId } = require('node:worker_threads');
  let cursor = (process.pid + threadId) % count;
  /** One allocation: `next()` yields the next candidate, `last` is true once every candidate has been tried. */
  const sweep = () => {
    let tried = 0;
    return {
      next() {
        const port = base + size - 1 - 2 * cursor;
        cursor = (cursor + 1) % count;
        tried += 1;
        return port;
      },
      get last() {
        return tried >= count;
      },
    };
  };
  const inUse = (error) => error !== null && typeof error === 'object' && error.code === 'EADDRINUSE';
  const isZero = (port) => port === 0 || port === '0' || port === null || port === undefined;

  // `--inspect=127.0.0.1:0`, `--inspect-brk=0`, `--inspect-wait=ws://localhost:0/x`, `--inspect-port=0`.
  const inspectZero = /^(--inspect(?:-brk|-wait|-port)?=(?:.*:)?)0(\/.*)?$/;
  const { basename } = require('node:path');
  /**
   * `args` (argv after argv[0]) with each port-0 inspector option of a Node or Bun `program` given a probed port of the
   * block, or `args` itself. Only an argument that is certainly the runtime's own option changes. The scan walks the
   * leading `=`-joined options (and, for Bun, one `test` or `run` subcommand) and stops at the first bare flag,
   * whose operand it cannot know, at the entrypoint and at `--`. Everything from there on is passed unchanged: an
   * option's operand, the child's own arguments, every other program's. An inspector text past the stop is named on
   * stderr, because it keeps port 0 and nothing in the workspace can connect to it.
   */
  const rewrite = (program, args, probe) => {
    const name = typeof program === 'string' ? basename(program) : '';
    const self = program === process.execPath;
    const bun = name === 'bun' || (self && process.versions.bun !== undefined);
    if (!bun && name !== 'node' && !self) return args;
    let subcommand = bun;
    let end = 0;
    let stopper;
    for (; end < args.length; end++) {
      const arg = args[end];
      if (typeof arg !== 'string') {
        stopper = 'a non-string argument';
        break;
      }
      if (arg === '--') {
        stopper = '`--`';
        break;
      }
      if (arg.startsWith('-')) {
        if (arg.includes('=')) continue;
        stopper = `the bare flag \`${arg}\`, whose operand the preload cannot know`;
        break;
      }
      if (subcommand && (arg === 'test' || arg === 'run')) {
        subcommand = false;
        continue;
      }
      stopper = `the entrypoint \`${arg}\``;
      break;
    }
    const unmapped = args.slice(end).find((arg) => typeof arg === 'string' && inspectZero.test(arg));
    if (unmapped !== undefined) {
      process.stderr.write(
        `cowshed: ${unmapped} for ${program} was not mapped into the workspace port block: the scan of its runtime ` +
          `options stopped at ${stopper}. If it is the runtime's own inspector option it keeps port 0, which nothing in ` +
          'the workspace can connect to: put it before that point with every option before it joined with `=` ' +
          '(`--flag=value`), or grant the workspace --loopback-any.\n',
      );
    }
    if (!args.slice(0, end).some((arg) => inspectZero.test(arg))) return args;
    return args.map((arg, index) =>
      index < end ? arg.replace(inspectZero, (_, head, tail) => `${head}${probe()}${tail ?? ''}`) : arg,
    );
  };

  if (process.versions.bun !== undefined) {
    // Bun's node:net and node:http listen through these globals, so they cover every Bun listener.
    const remap = (bind) =>
      function (options, ...rest) {
        if (options === null || typeof options !== 'object' || options.unix !== undefined) {
          return bind.call(this, options, ...rest);
        }
        if (options.port !== 0 && options.port !== '0') return bind.call(this, options, ...rest);
        const ports = sweep();
        for (;;) {
          // A derived object keeps the caller's handlers and getters, prototype included.
          const placed = Object.create(options, { port: { value: ports.next(), enumerable: true } });
          try {
            return bind.call(this, placed, ...rest);
          } catch (error) {
            if (!inUse(error) || ports.last) throw error;
          }
        }
      };
    const listen = Bun.listen;
    Bun.serve = remap(Bun.serve);
    Bun.listen = remap(listen);
    const probe = () => {
      const ports = sweep();
      for (;;) {
        const port = ports.next();
        try {
          listen({ hostname: '127.0.0.1', port, socket: { data() {} } }).stop(true);
          return port;
        } catch (error) {
          if (!inUse(error) || ports.last) throw error;
        }
      }
    };
    const spawn = Bun.spawn;
    Bun.spawn = function (command, ...rest) {
      const cmd = Array.isArray(command) ? command : command?.cmd;
      if (Array.isArray(cmd) && cmd.length > 0) {
        const tail = cmd.slice(1);
        const args = rewrite(cmd[0], tail, probe);
        if (args !== tail) {
          const placed = [cmd[0], ...args];
          return spawn.call(this, Array.isArray(command) ? placed : { ...command, cmd: placed }, ...rest);
        }
      }
      return spawn.call(this, command, ...rest);
    };
    return;
  }

  const net = require('node:net');
  const listen = net.Server.prototype.listen;
  /** The arguments of a port-0 TCP listen, rebuilt for a chosen port, or null for any other listen. */
  const plan = (args) => {
    const [first] = args;
    if (first !== null && typeof first === 'object') {
      // listen(options[, cb]); a handle, a descriptor or a path is not a TCP port.
      if (!('port' in first) || !isZero(first.port)) return null;
      if (
        first.path !== undefined ||
        first.fd !== undefined ||
        first.handle !== undefined ||
        first._handle !== undefined
      ) {
        return null;
      }
      return { callback: typeof args[1] === 'function' ? args[1] : undefined, args: (port) => [{ ...first, port }] };
    }
    // listen([port[, host[, backlog]]][, cb]) with the port absent or zero.
    if (typeof first !== 'function' && !isZero(first)) return null;
    const rest = typeof first === 'function' ? args : args.slice(1);
    const callback = typeof rest[rest.length - 1] === 'function' ? rest[rest.length - 1] : undefined;
    const positional = callback === undefined ? rest : rest.slice(0, -1);
    return { callback, args: (port) => [port, ...positional] };
  };
  net.Server.prototype.listen = function (...args) {
    const placed = plan(args);
    if (placed === null) return listen.apply(this, args);
    if (placed.callback !== undefined) this.once('listening', placed.callback);
    const ports = sweep();
    const attempt = () => {
      // A failed listen reports on 'error' a tick later; an in-use candidate is taken back and the next one tried, so
      // the caller sees only the listen that succeeded or the last candidate's own error.
      const owned = Object.hasOwn(this, 'emit');
      const emit = this.emit;
      const restore = () => {
        if (owned) this.emit = emit;
        else delete this.emit;
      };
      this.emit = function (event, ...payload) {
        if (event === 'error' || event === 'listening') {
          restore();
          if (event === 'error' && inUse(payload[0]) && !ports.last) {
            attempt();
            return true;
          }
        }
        return emit.call(this, event, ...payload);
      };
      try {
        listen.apply(this, placed.args(ports.next()));
      } catch (error) {
        restore();
        throw error;
      }
    };
    attempt();
    return this;
  };

  const probe = () => {
    // Node has no synchronous listen, and spawn() is synchronous: a bare handle's bind and listen show the port is free.
    // libuv defers a bind's EADDRINUSE to the listen that follows it, so only the listen's status counts.
    const { TCP, constants: tcp } = process.binding('tcp_wrap');
    const addressInUse = -require('node:os').constants.errno.EADDRINUSE;
    const ports = sweep();
    for (;;) {
      const port = ports.next();
      const handle = new TCP(tcp.SERVER);
      let syscall = 'bind';
      let status = handle.bind('127.0.0.1', port);
      if (status === 0) {
        syscall = 'listen';
        status = handle.listen(1);
      }
      handle.close();
      if (status === 0) return port;
      if (status !== addressInUse || ports.last) {
        const code = require('node:util').getSystemErrorName(status);
        throw Object.assign(new Error(`${syscall} ${code} 127.0.0.1:${port}`), {
          code,
          errno: status,
          syscall,
          address: '127.0.0.1',
          port,
        });
      }
    }
  };
  const { ChildProcess } = require('node:child_process');
  const spawn = ChildProcess.prototype.spawn;
  ChildProcess.prototype.spawn = function (options) {
    if (options !== null && typeof options === 'object' && Array.isArray(options.args) && options.args.length > 0) {
      const [argv0, ...rest] = options.args;
      const args = rewrite(options.file, rest, probe);
      if (args !== rest) return spawn.call(this, { ...options, args: [argv0, ...args] });
    }
    return spawn.call(this, options);
  };
})();
