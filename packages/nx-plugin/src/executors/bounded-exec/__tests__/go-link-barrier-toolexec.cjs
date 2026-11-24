#!/usr/bin/env node
'use strict';

const { spawnSync } = require('node:child_process');
const { appendFileSync } = require('node:fs');
const { connect } = require('node:net');
const { basename } = require('node:path');

const [tool, ...args] = process.argv.slice(2);
const isVersionProbe = args.some((arg) => arg.startsWith('-V'));

function run() {
  const result = spawnSync(tool, args, { stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.signal) process.kill(process.pid, result.signal);
  else process.exit(result.status ?? 1);
}

const toolLog = process.env.GO_PROGRESS_TOOL_LOG;
if (toolLog !== undefined && toolLog !== '' && !isVersionProbe) {
  const pkgIndex = args.indexOf('-p');
  const pkg = pkgIndex !== -1 ? (args[pkgIndex + 1] ?? '') : '';
  appendFileSync(toolLog, `${basename(tool)}\t${pkg}\n`);
}

const barrierSocket = process.env.GO_PROGRESS_BARRIER_SOCKET;
if (basename(tool) !== 'link' || isVersionProbe || barrierSocket === undefined || barrierSocket === '') {
  run();
} else {
  const socket = connect(barrierSocket, () => socket.write('link-ready\n'));
  socket.once('error', (error) => {
    throw error;
  });
  let answer = '';
  socket.on('data', (data) => {
    answer += data.toString();
  });
  socket.once('end', () => {
    if (answer !== 'go\n') throw new Error('unexpected link release');
    socket.end();
    run();
  });
}
