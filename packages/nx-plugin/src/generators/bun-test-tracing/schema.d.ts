export interface BunTestTracingGeneratorSchema {
  project: string;
  opContextModule: string;
  opContextExport?: string;
  spanContextModule: string;
  spanContextExport?: string;
  tracerModule?: string;
}
