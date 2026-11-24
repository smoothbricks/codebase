import { mkdirSync, writeFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { checkBendFile } from './emit.js';

export async function run(argv: readonly string[]): Promise<void> {
  const [file, outdir, ...rest] = argv;
  if (file === undefined || outdir === undefined) {
    throw new Error(
      'bend-emit refuses: usage: bend-emit <model.bend> <outdir> --fns a,b --laws x,y [--cap N] [--manifest path]',
    );
  }
  const opt = (name: string): string => {
    const i = rest.indexOf(name);
    return i >= 0 ? (rest[i + 1] ?? '') : '';
  };
  const book = await checkBendFile(file);
  const moduleKind = opt('--rust-module') ? 'rust-module' : opt('--ts-module') ? 'ts-module' : 'go-module';
  const modulePath = opt(`--${moduleKind}`);
  const manifestPath = opt('--manifest');
  const modelName = opt('--model-name') || basename(file);
  const stem = basename(file, '.bend');
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
