// Compile-negative contracts run with the real exported declarations and all
// third-party dependencies installed. They must not leak Node/Bun ambient types.
export function rejectServerGlobals(): void {
  // @ts-expect-error Node Buffer is not part of the browser/isomorphic contract.
  void Buffer;
  // @ts-expect-error Node process is not part of the browser/isomorphic contract.
  void process;
  // @ts-expect-error Bun is not part of the browser/isomorphic contract.
  void Bun;
  // @ts-expect-error CommonJS require is not provided to browser consumers.
  void require;
  // @ts-expect-error Node file globals are not provided to browser consumers.
  void __dirname;
}
