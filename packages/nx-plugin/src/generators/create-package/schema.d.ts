export interface CreatePackageGeneratorSchema {
  name: string;
  variant: 'ts-lib' | 'rust-crate';
  public?: boolean;
  wasm?: boolean;
}
