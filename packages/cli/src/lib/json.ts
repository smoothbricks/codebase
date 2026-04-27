import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { hasOwn, hasOwnString, isRecord } from '@smoothbricks/validation';

export { hasOwn, hasOwnString, isRecord };

export function stringProperty(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

export function recordProperty(record: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = record[key];
  return isRecord(value) ? value : null;
}

export function getOrCreateRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = record[key];
  if (isRecord(value)) {
    return value;
  }
  const next: Record<string, unknown> = {};
  record[key] = next;
  return next;
}

export function setStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (record[key] === value) {
    return false;
  }
  record[key] = value;
  return true;
}

export function setMissingStringProperty(record: Record<string, unknown>, key: string, value: string): boolean {
  if (typeof record[key] === 'string') {
    return false;
  }
  record[key] = value;
  return true;
}

export function requiredJsonObject(path: string): Record<string, unknown> {
  const json = readJsonObject(path);
  if (!json) {
    throw new Error(`${path} not found or invalid`);
  }
  return json;
}

export function readJsonObject(path: string): Record<string, unknown> | null {
  const json = readJson(path);
  return isRecord(json) ? json : null;
}

export function writeJsonObject(path: string, value: Record<string, unknown>): void {
  writeFileSync(path, jsonObjectText(value));
}

export function jsonObjectText(value: Record<string, unknown>): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

export function readJson(path: string): unknown {
  if (!existsSync(path)) {
    return null;
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}
