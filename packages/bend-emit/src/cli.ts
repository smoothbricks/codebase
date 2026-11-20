import { mkdirSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { checkBendFile } from './emit.js';

export async function run(argv: readonly string[]): Promise<void> {
  const [file, outdir, ...rest] = argv;
  if (file === undefined || outdir === undefined) {
    throw new Error('bend-emit refuses: usage: bend-emit <model.bend> <outdir> --fns a,b --laws x,y [--cap N]');
  }
  const opt = (name: string): string => {
    const i = rest.indexOf(name);
    return i >= 0 ? (rest[i + 1] ?? '') : '';
  };
  const book = await checkBendFile(file);
  const moduleKind = opt('--rust-module') ? 'rust-module' : opt('--ts-module') ? 'ts-module' : 'go-module';
  const modulePath = opt(`--${moduleKind}`);
  const stem = basename(file, '.bend');
  const emission = book.emit({
    target: modulePath
      ? {
          kind: moduleKind,
          modelName: opt('--model-name') || basename(file),
          flags: rest.filter((arg) => arg !== modulePath),
        }
      : { kind: 'all', stem },
    functions: opt('--fns').split(',').filter(Boolean),
    laws: opt('--laws').split(',').filter(Boolean),
    cap: Number(opt('--cap') || '400'),
    listLength: Number(opt('--len') || '3'),
    properties: Number(opt('--props') || '200'),
    propertyLength: Number(opt('--prop-len') || '12'),
    noCorpus: opt('--no-corpus').split(',').filter(Boolean),
  });
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
