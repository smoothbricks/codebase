import { useEffect, useState } from 'react';
import { SafeAreaView, StyleSheet, Text, View } from 'react-native';

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
  const functionCounter = observeDynamicFunctionCalls();

  try {
    // Static loading would compile LMAO plans before the Function observer can measure them.
    const { createTraceRoot } = await import('@smoothbricks/lmao/es');
    const { runPlatformScenario } = await import('../lmao/benchmarks/plugin-scenario/platform');
    const result = await runPlatformScenario({
      now: () => performance.now(),
      createTraceRoot,
      platform: runtime.platform,
      engine: runtime.engine,
      variant: getTransformVariant(),
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
