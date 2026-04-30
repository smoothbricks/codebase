export interface CreatePackageGeneratorSchema {
  name: string;
  variant: 'ts-lib' | 'ts-zig';
  public?: boolean;
}
