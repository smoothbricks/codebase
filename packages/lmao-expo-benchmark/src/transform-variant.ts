declare const process: {
  readonly env: {
    readonly EXPO_PUBLIC_LMAO_BENCH_TRANSFORM?: string;
  };
};

export type TransformVariant = 'off' | 'on';

export function getTransformVariant(): TransformVariant {
  const variant = process.env.EXPO_PUBLIC_LMAO_BENCH_TRANSFORM;

  if (variant !== 'off' && variant !== 'on') {
    throw new Error('The Metro build did not inject a valid LMAO benchmark transform variant.');
  }

  return variant;
}
