import { spawn } from 'node:child_process';
import { chmod, copyFile, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { availableParallelism } from 'node:os';
import { isAbsolute, join } from 'node:path';

import {
  CARGO_TEST_BINS_DIR,
  CARGO_TEST_MANIFEST,
  type CargoTestBinary,
  type CargoTestManifest,
  harnessArgsFor,
  parseCargoTestArtifacts,
  parseCargoTestManifest,
  stagedBinaryPath,
} from './artifacts.js';
import type { CargoTestOptions } from './schema.js';

const DEFAULT_KILL_AFTER_MS = 10_000;
const REAP_AFTER_FORCE_KILL_MS = 2_000;

export interface CargoTestContext {
  root: string;
}

export interface CargoTestResult {
  success: boolean;
  terminalOutput?: string;
}

export default function cargoTestExecutor(
  options: CargoTestOptions,
  context: CargoTestContext,
): Promise<CargoTestResult> {
  return runCargoTest(options, context);
}

export async function runCargoTest(options: CargoTestOptions, context: CargoTestContext): Promise<CargoTestResult> {
  const cwd = isAbsolute(options.cwd) ? options.cwd : join(context.root, options.cwd);
  const stagingDirectory = join(cwd, CARGO_TEST_BINS_DIR);
  if (options.phase === 'compile') {
    return compileCargoTests(cwd, stagingDirectory, options.release === true);
  }
  return runStagedCargoTests(cwd, stagingDirectory, options);
}

async function compileCargoTests(cwd: string, stagingDirectory: string, release: boolean): Promise<CargoTestResult> {
  const args = ['test', '--workspace', '--no-run', '--message-format=json'];
  if (release) {
    args.push('--release');
  }
  const { code, stdout, stderr } = await runProcess('cargo', args, cwd);
  process.stderr.write(stderr);
  if (code !== 0) {
    process.stderr.write(stdout);
    return { success: false, terminalOutput: stderr + stdout };
  }
  const binaries = parseCargoTestArtifacts(stdout);
  await rm(stagingDirectory, { recursive: true, force: true });
  await mkdir(stagingDirectory, { recursive: true });
  for (const binary of binaries) {
    const destination = stagedBinaryPath(stagingDirectory, binary);
    await copyFile(binary.sourcePath, destination);
    await chmod(destination, 0o755);
  }
  const manifest: CargoTestManifest = { binaries };
  await writeFile(join(stagingDirectory, CARGO_TEST_MANIFEST), `${JSON.stringify(manifest, null, 2)}\n`);
  process.stdout.write(`staged ${binaries.length} cargo test binaries under ${CARGO_TEST_BINS_DIR}\n`);
  return { success: true };
}

async function runStagedCargoTests(
  cwd: string,
  stagingDirectory: string,
  options: CargoTestOptions,
): Promise<CargoTestResult> {
  const manifestPath = join(stagingDirectory, CARGO_TEST_MANIFEST);
  let raw: string;
  try {
    raw = await readFile(manifestPath, 'utf8');
  } catch {
    process.stderr.write(
      `cargo-test run: missing ${CARGO_TEST_BINS_DIR}/${CARGO_TEST_MANIFEST}; run cargo-test-compile first\n`,
    );
    return { success: false };
  }
  const manifest = parseCargoTestManifest(raw);
  if (manifest === null) {
    process.stderr.write(`cargo-test run: invalid ${CARGO_TEST_BINS_DIR}/${CARGO_TEST_MANIFEST}\n`);
    return { success: false };
  }
  const extraArgs = options.__unparsed__ ?? [];
  const timeoutMs = options.timeoutMs;
  const killAfterMs = options.killAfterMs ?? DEFAULT_KILL_AFTER_MS;
  const jobs = Math.max(1, options.jobs ?? availableParallelism());
  const pending = [...manifest.binaries];
  const failures: string[] = [];
  let next = 0;
  async function worker(): Promise<void> {
    while (next < pending.length) {
      const binary = pending[next];
      next += 1;
      if (binary === undefined) {
        return;
      }
      const result = await runOneBinary(cwd, stagingDirectory, binary, extraArgs, timeoutMs, killAfterMs);
      if (!result.success) {
        failures.push(`${binary.name}: ${result.detail}`);
      }
    }
  }
  const workers = Math.min(jobs, pending.length);
  if (workers > 0) {
    await Promise.all(Array.from({ length: workers }, () => worker()));
  }
  if (failures.length > 0) {
    process.stderr.write(
      `${failures.length} cargo test binaries failed:\n${failures.map((line) => `  ${line}`).join('\n')}\n`,
    );
    return { success: false, terminalOutput: failures.join('\n') };
  }
  process.stdout.write(`ran ${manifest.binaries.length} cargo test binaries\n`);
  return { success: true };
}

async function runOneBinary(
  cwd: string,
  stagingDirectory: string,
  binary: CargoTestBinary,
  extraArgs: string[],
  timeoutMs: number | undefined,
  killAfterMs: number,
): Promise<{ success: boolean; detail: string }> {
  const file = stagedBinaryPath(stagingDirectory, binary);
  const args = [...harnessArgsFor(binary), ...extraArgs];
  writeStream(process.stdout, `     ${binary.name}\n`);
  const child = spawn(file, args, {
    cwd,
    detached: process.platform !== 'win32',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout?.on('data', (chunk: Buffer | string) => {
    writeStream(process.stdout, chunk.toString());
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    writeStream(process.stderr, chunk.toString());
  });
  const pid = child.pid;
  let settled = false;
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.once('error', (error) => {
      settled = true;
      process.stderr.write(`${binary.name}: ${error.message}\n`);
      resolve({ code: 1, signal: null });
    });
    child.once('exit', (code, signal) => {
      settled = true;
      resolve({ code, signal });
    });
  });
  let timedOut = false;
  let timeout: NodeJS.Timeout | undefined;
  let forceKill: NodeJS.Timeout | undefined;
  if (timeoutMs !== undefined && pid) {
    timeout = setTimeout(() => {
      timedOut = true;
      process.stderr.write(`\n${binary.name} timed out after ${timeoutMs}ms\n`);
      killTree(pid, 'SIGTERM');
      forceKill = setTimeout(() => {
        if (!settled) {
          process.stderr.write(`Force-killing ${binary.name} after killAfterMs=${killAfterMs}\n`);
          killTree(pid, 'SIGKILL');
        }
      }, killAfterMs);
    }, timeoutMs);
  }
  const finished =
    timeoutMs === undefined
      ? await exit
      : await Promise.race([
          exit,
          delay(timeoutMs + killAfterMs + REAP_AFTER_FORCE_KILL_MS).then(() => ({
            code: 1 as number | null,
            signal: 'SIGKILL' as NodeJS.Signals | null,
          })),
        ]);
  clearTimeout(timeout);
  clearTimeout(forceKill);
  if (timedOut && !settled && pid) {
    killTree(pid, 'SIGKILL');
  }
  if (timedOut) {
    return { success: false, detail: `timed out after ${timeoutMs}ms` };
  }
  if (finished.code === 0 && finished.signal === null) {
    return { success: true, detail: '' };
  }
  return {
    success: false,
    detail: finished.signal ? `signal ${finished.signal}` : `exit ${finished.code ?? 1}`,
  };
}

function runProcess(
  command: string,
  args: string[],
  cwd: string,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  const child = spawn(command, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '';
  let stderr = '';
  child.stdout?.on('data', (chunk: Buffer | string) => {
    stdout += chunk.toString();
  });
  child.stderr?.on('data', (chunk: Buffer | string) => {
    stderr += chunk.toString();
  });
  return new Promise((resolve) => {
    child.once('error', (error) => {
      resolve({ code: 1, stdout, stderr: `${stderr}${error.message}\n` });
    });
    child.once('exit', (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

function writeStream(stream: NodeJS.WritableStream, text: string): void {
  try {
    stream.write(text);
  } catch (error) {
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'EPIPE') {
      return;
    }
    throw error;
  }
}

function killTree(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-pid, signal);
  } catch {
    // Process group already gone, or this pid is not a group leader.
  }
  try {
    process.kill(pid, signal);
  } catch {
    // Already reaped.
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
