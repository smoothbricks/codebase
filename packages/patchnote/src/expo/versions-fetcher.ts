/**
 * Fetch Expo recommended package versions for a specific SDK
 */

import type { ExpoPackageVersions } from '../types.js';

/**
 * Fetch Expo recommended versions for a specific SDK
 *
 * Uses the Expo versions endpoint which provides compatibility information
 */
export async function fetchExpoVersions(sdkVersion: string): Promise<ExpoPackageVersions> {
  try {
    // Expo provides versioning information via their GitHub repository
    // Format: https://raw.githubusercontent.com/expo/expo/sdk-XX/packages/expo/bundledNativeModules.json
    const majorVersion = sdkVersion.split('.')[0];
    const url = `https://raw.githubusercontent.com/expo/expo/sdk-${majorVersion}/packages/expo/bundledNativeModules.json`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch Expo versions: ${response.statusText}`);
    }

    const bundledModules = (await response.json()) as Record<string, string>;

    // Also fetch React/React Native versions from the SDK
    const packageJsonUrl = `https://raw.githubusercontent.com/expo/expo/sdk-${majorVersion}/packages/expo/package.json`;
    const packageJsonResponse = await fetch(packageJsonUrl);

    let reactVersion = '19.0.0'; // fallback
    let reactNativeVersion = '0.76.0'; // fallback

    if (packageJsonResponse.ok) {
      const packageJson = (await packageJsonResponse.json()) as {
        peerDependencies?: { react?: string; 'react-native'?: string };
      };
      reactVersion = packageJson.peerDependencies?.react || reactVersion;
      reactNativeVersion = packageJson.peerDependencies?.['react-native'] || reactNativeVersion;
    }

    return {
      sdkVersion,
      packages: {
        react: reactVersion.replace(/^[\^~]/, ''),
        'react-native': reactNativeVersion.replace(/^[\^~]/, ''),
        expo: `~${sdkVersion}`,
        ...bundledModules,
      },
    };
  } catch (error) {
    throw new Error(`Failed to fetch Expo versions for SDK ${sdkVersion}: ${error}`);
  }
}

/**
 * Get critical packages that should be pinned in syncpack
 *
 * These are packages where version mismatches can cause issues
 */
export function getCriticalPackages(): string[] {
  return [
    'react',
    'react-native',
    'expo',
    '@types/react',
    '@types/react-native',
    // Expo modules that should match bundled versions
    'expo-modules-core',
    'expo-updates',
    'expo-splash-screen',
    'expo-status-bar',
  ];
}

/**
 * Filter Expo package versions to only critical packages
 */
export function filterCriticalVersions(expoVersions: ExpoPackageVersions): Record<string, string> {
  const critical = getCriticalPackages();
  const filtered: Record<string, string> = {};

  for (const pkg of critical) {
    if (expoVersions.packages[pkg]) {
      filtered[pkg] = expoVersions.packages[pkg];
    }
  }

  return filtered;
}
