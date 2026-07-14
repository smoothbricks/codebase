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
    // Module initialization includes the selected platform backend. Web compiles and
    // instantiates the WASM allocator here; native loads the JavaScript trace root.
    // Cold runs require a fresh page/process, so compare startup only within one engine.
    const moduleInitializationStartedAt = now();
    const platformRuntimeModule =
      runtime.platform === 'web' ? import('./src/platform-runtime.web') : import('./src/platform-runtime.native');
    const [{ runPlatformScenario }, { createPlatformRuntime, runPlatformSuperblockBenchmark }] = await Promise.all([
      import('../lmao/benchmarks/plugin-scenario/platform'),
      platformRuntimeModule,
    ]);
    const scenarioRuntime = await createPlatformRuntime();
    const moduleInitializationMs = now() - moduleInitializationStartedAt;
    const result = runPlatformScenario({
      now,
      ...scenarioRuntime,
      platform: runtime.platform,
      engine: runtime.engine,
      variant: getTransformVariant(),
      mode,
      ...(mode === 'cold' ? { moduleInitializationMs } : {}),
      ...(functionCounter === undefined ? {} : { getDynamicFunctionCallCount: functionCounter.readForScenario }),
    });
    const superblockResult = mode === 'steady' ? runPlatformSuperblockBenchmark(scenarioRuntime) : undefined;
    const resultJson = JSON.stringify(result);
    console.log(`LMAO_BENCH_RESULT ${resultJson}`);
    if (superblockResult !== undefined) {
      const superblockJson = JSON.stringify(superblockResult);
      console.log(`LMAO_SUPERBLOCK_RESULT ${superblockJson}`);
      return `${resultJson}\nLMAO_SUPERBLOCK_RESULT ${superblockJson}`;
    }
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
          const errorText = error instanceof Error ? (error.stack ?? `${error.name}: ${error.message}`) : String(error);
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
