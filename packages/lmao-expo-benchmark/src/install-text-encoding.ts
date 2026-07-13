import 'fast-text-encoding';

function supportsFatalDecoder(): boolean {
  try {
    new TextDecoder('utf-8', { fatal: true });
    return true;
  } catch {
    return false;
  }
}

if (!supportsFatalDecoder()) {
  const TextDecoderWithoutFatal = globalThis.TextDecoder;
  globalThis.TextDecoder = class FatalCompatibleTextDecoder extends TextDecoderWithoutFatal {
    constructor(label?: string, options?: TextDecoderOptions) {
      super(label, options === undefined ? undefined : { ...options, fatal: false });
    }
  };
}
