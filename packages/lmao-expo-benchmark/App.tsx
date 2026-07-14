import { useEffect, useState } from 'react';
import { SafeAreaView, StyleSheet, Text, View } from 'react-native';

import { getBenchmarkMode } from './src/benchmark-mode';
import { observeDynamicFunctionCalls } from './src/dynamic-function-counter';
import { getTransformVariant } from './src/transform-variant';

declare const HermesInternal: object | undefined;

let platformScenarioPromise: Promise<string> | undefined;

function detectRuntime(): { readonly platform: string; readonly engine: string } {
  const isWeb = typeof document === 'object';
  const isHermes = typeof HermesInternal === 'object';

  return {
    platform: isWeb ? 'web' : 'native',
    engine: isHermes ? 'hermes' : isWeb ? 'browser' : 'javascript',
  };
}

async function executePlatformScenario(): Promise<string> {
  const runtime = detectRuntime();
  const mode = getBenchmarkMode();
  const functionCounter = mode === 'diagnostic' ? observeDynamicFunctionCalls() : undefined;
  const now = () => performance.now();

  try {
    // This outer timestamp is the only meaningful static-import boundary: the imported
    // modules evaluate their complete dependency graph before either promise resolves.
    // Cold runs require a fresh page/process; async web import and synchronous Hermes
    // require are intentionally compared only within their own engine.
    const moduleInitializationStartedAt = now();
    const [{ createTraceRoot }, { runPlatformScenario }] = await Promise.all([
      import('@smoothbricks/lmao/es'),
      import('../lmao/benchmarks/plugin-scenario/platform'),
    ]);
    const moduleInitializationMs = now() - moduleInitializationStartedAt;
    const result = runPlatformScenario({
      now,
      createTraceRoot,
      platform: runtime.platform,
      engine: runtime.engine,
      variant: getTransformVariant(),
      mode,
      ...(mode === 'cold' ? { moduleInitializationMs } : {}),
      ...(functionCounter === undefined ? {} : { getDynamicFunctionCallCount: functionCounter.readForScenario }),
    });
    const resultJson = JSON.stringify(result);
    console.log(`LMAO_BENCH_RESULT ${resultJson}`);
    return resultJson;
  } finally {
    functionCounter?.restore();
  }
}

function runOnceAfterMount(): Promise<string> {
  platformScenarioPromise ??= executePlatformScenario();
  return platformScenarioPromise;
}

export default function App() {
  const [resultText, setResultText] = useState('LMAO benchmark pending');

  useEffect(() => {
    let mounted = true;

    runOnceAfterMount().then(
      (resultJson) => {
        if (mounted) {
          setResultText(`LMAO benchmark complete\n${resultJson}`);
        }
      },
      (error: unknown) => {
        if (mounted) {
          const errorText = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
          setResultText(`LMAO benchmark failed\n${errorText}`);
        }
      },
    );

    return () => {
      mounted = false;
    };
  }, []);

  return (
    <SafeAreaView style={styles.safeArea}>
      <View style={styles.container}>
        <Text selectable testID="lmao-benchmark-result" style={styles.result}>
          {resultText}
        </Text>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safeArea: {
    backgroundColor: '#0b1020',
    flex: 1,
  },
  container: {
    flex: 1,
    justifyContent: 'center',
    padding: 24,
  },
  result: {
    color: '#f8fafc',
    fontFamily: 'monospace',
    fontSize: 12,
    lineHeight: 18,
  },
});
