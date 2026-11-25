import { mkdirSync, writeFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { checkBendFile, type MachineSpec } from './emit.js';

function parseMachineSpec(raw: string): MachineSpec {
  const colon = raw.indexOf(':');
  if (colon <= 0 || colon === raw.length - 1) {
    throw new Error(
      `bend-emit refuses: invalid --machine ${JSON.stringify(raw)} (expected name:init=<fn>,step=<fn>,input=<Type>[,handled=<fn>])`,
    );
  }
  const name = raw.slice(0, colon);
  const fields = new Map<string, string>();
  for (const part of raw.slice(colon + 1).split(',')) {
    const eq = part.indexOf('=');
    if (eq <= 0 || eq === part.length - 1) {
      throw new Error(`bend-emit refuses: invalid --machine field ${JSON.stringify(part)} in ${JSON.stringify(raw)}`);
    }
    const key = part.slice(0, eq);
    const val = part.slice(eq + 1);
    if (key !== 'init' && key !== 'step' && key !== 'input' && key !== 'handled') {
      throw new Error(`bend-emit refuses: unknown --machine key ${JSON.stringify(key)} in ${JSON.stringify(raw)}`);
    }
    if (fields.has(key)) {
      throw new Error(`bend-emit refuses: duplicate --machine key ${JSON.stringify(key)} in ${JSON.stringify(raw)}`);
    }
    fields.set(key, val);
  }
  const init = fields.get('init');
  const step = fields.get('step');
  const input = fields.get('input');
  const handled = fields.get('handled');
  if (init === undefined || step === undefined || input === undefined) {
    throw new Error(
      `bend-emit refuses: --machine ${JSON.stringify(raw)} must specify init=<fn>, step=<fn>, and input=<Type>`,
    );
  }
  return { name, init, step, input, ...(handled !== undefined ? { handled } : {}) };
}

async function runSkeleton(argv: readonly string[]): Promise<void> {
  const [file, ...rest] = argv;
  if (file === undefined || file.startsWith('-')) {
    throw new Error('bend-emit refuses: usage: bend-emit skeleton <book.bend> [--law a,b | --all]');
  }
  const laws: string[] = [];
  let all = false;
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i];
    if (arg === '--all') {
      all = true;
    } else if (arg === '--law' || arg === '--laws') {
      const val = rest[i + 1];
      if (val === undefined || val === '' || val.startsWith('-')) {
        throw new Error(`bend-emit refuses: ${arg} requires a law or lemma name`);
      }
      for (const item of val.split(',').filter(Boolean)) {
        laws.push(item);
      }
      i++;
    } else {
      throw new Error(`bend-emit refuses: unknown option ${JSON.stringify(arg)} for skeleton`);
    }
  }
  if (all && laws.length > 0) {
    throw new Error('bend-emit refuses: pass either --law <name> or --all, not both');
  }
  const book = await checkBendFile(file);
  const explicit = laws.length > 0;
  const result = book.skeleton(explicit ? { laws } : {});
  if (explicit && result.skipped.length > 0) {
    throw new Error(`bend-emit refuses: existing proof will not be overwritten: ${result.skipped.join(', ')}`);
  }
  if (!explicit && result.skipped.length > 0) {
    console.error(`bend-emit skeleton: skipped already-proved defs: ${result.skipped.join(', ')}`);
  }
  if (result.source.length > 0) {
    console.log(result.source);
  }
}

export async function run(argv: readonly string[]): Promise<void> {
  if (argv[0] === 'skeleton') {
    await runSkeleton(argv.slice(1));
    return;
  }
  const [file, outdir, ...rest] = argv;
  if (file === undefined || outdir === undefined) {
    throw new Error(
      'bend-emit refuses: usage: bend-emit <model.bend> <outdir> --fns a,b --laws x,y [--machine name:init=f,step=g,input=T[,handled=h]] [--cap N] [--manifest path]',
    );
  }
  const opt = (name: string): string => {
    const i = rest.indexOf(name);
    return i >= 0 ? (rest[i + 1] ?? '') : '';
  };
  const opts = (name: string): string[] => {
    const values: string[] = [];
    for (let i = 0; i < rest.length; i++) {
      if (rest[i] === name) {
        const val = rest[i + 1];
        if (val === undefined || val === '') {
          throw new Error(`bend-emit refuses: ${name} requires a value`);
        }
        values.push(val);
        i++;
      }
    }
    return values;
  };
  const book = await checkBendFile(file);
  const moduleKind = opt('--rust-module') ? 'rust-module' : opt('--ts-module') ? 'ts-module' : 'go-module';
  const modulePath = opt(`--${moduleKind}`);
  const manifestPath = opt('--manifest');
  const modelName = opt('--model-name') || basename(file);
  const stem = basename(file, '.bend');
  const machines = opts('--machine').map(parseMachineSpec);
  const filteredFlags = rest.filter((arg, i) => {
    if (arg === modulePath) return false;
    if (manifestPath && (arg === '--manifest' || rest[i - 1] === '--manifest')) return false;
    return true;
  });
  const emission = book.emit({
    target: modulePath
      ? {
          kind: moduleKind,
          modelName,
          flags: filteredFlags,
        }
      : { kind: 'all', stem },
    modelName,
    functions: opt('--fns').split(',').filter(Boolean),
    laws: opt('--laws').split(',').filter(Boolean),
    machines,
    cap: Number(opt('--cap') || '400'),
    listLength: Number(opt('--len') || '3'),
    adtDepth: Number(opt('--adt-depth') || '2'),
    properties: Number(opt('--props') || '200'),
    propertyLength: Number(opt('--prop-len') || '12'),
    noCorpus: opt('--no-corpus').split(',').filter(Boolean),
  });
  if (manifestPath) {
    mkdirSync(dirname(manifestPath), { recursive: true });
    writeFileSync(manifestPath, `${JSON.stringify(emission.manifest, null, 2)}\n`);
  }
  const counts = `${emission.functions} fns, ${emission.laws} laws`;
  if (emission.kind === 'module') {
    writeFileSync(modulePath, emission.source);
    console.log(`bend-emit: ${modulePath}: ${counts}`);
    return;
  }
  mkdirSync(outdir, { recursive: true });
  writeFileSync(join(outdir, `${stem}.rs`), emission.rust);
  writeFileSync(join(outdir, `${stem}.goose`), emission.goose);
  writeFileSync(join(outdir, `${stem}.go`), emission.go);
  writeFileSync(join(outdir, `${stem}.ts`), emission.typescript);
  writeFileSync(join(outdir, `run_${stem}.ts`), emission.runner);
  console.log(
    `bend-emit: ${stem}: ${counts}, ${emission.cases} corpus cases + ${emission.properties} seeded properties per law -> ${outdir}/${stem}.{rs,goose,go,ts} + run_${stem}.ts`,
  );
}
