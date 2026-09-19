export interface TypeScriptEmitOptions {
  kind?: 'library' | 'javascript' | 'tests';
  executableOutputs?: string[];
  cwd: string;
  tsConfig: string;
}
