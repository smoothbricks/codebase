/**
 * Expo SDK version checking utilities
 */

import { readFile } from 'node:fs/promises';
import type { ExpoSDKVersion } from '../types.js';

/**
 * Get current Expo SDK version from package.json
 */
export async function getCurrentExpoSDK(packageJsonPath: string): Promise<string | null> {
  try {
    const content = await readFile(packageJsonPath, 'utf-8');
    const packageJson = JSON.parse(content);

    if (!packageJson || typeof packageJson !== 'object') {
      console.error(`Invalid package.json format at ${packageJsonPath}`);
      return null;
    }

    // Check dependencies and devDependencies
    const expoVersion = packageJson.dependencies?.expo || packageJson.devDependencies?.expo;

    if (!expoVersion) {
      return null;
    }

    // Remove semver prefixes (^, ~, etc.)
    return expoVersion.replace(/^[\^~]/, '');
  } catch (error) {
    console.error(`Failed to read package.json at ${packageJsonPath}:`, error);
    return null;
  }
}

/**
 * Fetch latest Expo SDK version from npm registry
 */
export async function getLatestExpoSDK(): Promise<ExpoSDKVersion> {
  try {
    const response = await fetch('https://registry.npmjs.org/expo/latest');
    if (!response.ok) {
      throw new Error(`Failed to fetch Expo version: ${response.statusText}`);
    }

    const data = await response.json();
    const version = (data as { version: string }).version;

    // Extract SDK version from package version
    // Expo versions are like "~52.0.0" or "52.0.0"
    const sdkVersion = version.replace(/^~/, '');

    return {
      version: sdkVersion,
      isLatest: true,
      changelogUrl: `https://expo.dev/changelog/${sdkVersion.split('.')[0]}`,
    };
  } catch (error) {
    throw new Error(`Failed to fetch latest Expo SDK: ${error}`);
  }
}

/**
 * Compare two Expo SDK versions
 * Returns: -1 if a < b, 0 if equal, 1 if a > b
 */
export function compareVersions(a: string, b: string): number {
  const partsA = a.split('.').map(Number);
  const partsB = b.split('.').map(Number);

  for (let i = 0; i < Math.max(partsA.length, partsB.length); i++) {
    const numA = partsA[i] || 0;
    const numB = partsB[i] || 0;

    if (numA < numB) return -1;
    if (numA > numB) return 1;
  }

  return 0;
}

/**
 * Check if a new Expo SDK version is available
 */
export async function checkForExpoUpdate(
  packageJsonPath: string,
): Promise<{ hasUpdate: boolean; current: string | null; latest: ExpoSDKVersion }> {
  const current = await getCurrentExpoSDK(packageJsonPath);
  const latest = await getLatestExpoSDK();

  if (!current) {
    return {
      hasUpdate: false,
      current: null,
      latest,
    };
  }

  const hasUpdate = compareVersions(current, latest.version) < 0;

  return {
    hasUpdate,
    current,
    latest,
  };
}
