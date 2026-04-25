import { tool } from '@opencode-ai/plugin';

export const z: typeof tool.schema = tool.schema;

export type ToolArgsShape = Parameters<typeof tool>[0]['args'];
export type ToolExecute<Shape extends ToolArgsShape> = ReturnType<typeof tool<Shape>>['execute'];
export type ToolInput<Shape extends ToolArgsShape> = Parameters<ToolExecute<Shape>>[0];
export type ZString = ReturnType<typeof z.string>;
export type ZNumber = ReturnType<typeof z.number>;
export type ZOptionalString = ReturnType<typeof z.optional<ZString>>;

export interface InternalTool<Shape extends ToolArgsShape = ToolArgsShape> {
  name: string;
  description: string;
  parameters: { shape: Shape };
  execute: (args: ToolInput<Shape>) => Promise<{ title: string; output: string; metadata: Record<string, unknown> }>;
}

export function defineTool<Shape extends ToolArgsShape>(toolDef: InternalTool<Shape>): InternalTool<Shape> {
  return toolDef;
}
