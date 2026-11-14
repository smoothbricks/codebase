/**
 * Publishing the canonical currency list (specs/money/00-money.md §2): the fiat entries come from ISO 4217 list one,
 * the crypto entries are this package's own, and a publish run refuses every change the list's append-only rule
 * forbids. The decisions are pure; `scripts/publish-currencies.ts` is the I/O shell.
 */

import { err, ok, type Result } from './result.js';

export type CurrencyType = 'fiat' | 'crypto';

/** The canonical variant rule of a currency's cells (spec §8), frozen with its base scale. */
export type CanonicalPolicy = 'base' | 'minimal';

/** One entry of the published list: plain data. */
export interface PublishedCurrency {
  readonly id: number;
  readonly code: string;
  readonly name: string;
  readonly type: CurrencyType;
  readonly minorDecimals: number;
  readonly baseDecimals: number;
  readonly runtimeDecimals: number;
  readonly wide: boolean;
  readonly policy: CanonicalPolicy;
  readonly withdrawn?: true;
}

/** A currency of ISO 4217 list one with numeric minor units. */
export interface IsoCurrency {
  readonly id: number;
  readonly code: string;
  readonly name: string;
  readonly minorDecimals: number;
}

/** Every fiat currency is stored and computed at six decimals: sub-cent prices fit, and a value fits 64 bits. */
export const FIAT_DECIMALS = 6;

/** The crypto assets of the list. Their ids are this list's own range, from 1000. */
export const CRYPTO_CURRENCIES: readonly PublishedCurrency[] = [
  {
    id: 1000,
    code: 'BTC',
    name: 'Bitcoin',
    type: 'crypto',
    minorDecimals: 8,
    baseDecimals: 11,
    runtimeDecimals: 11,
    wide: false,
    policy: 'base',
  },
  {
    id: 1001,
    code: 'USDC',
    name: 'USD Coin',
    type: 'crypto',
    minorDecimals: 6,
    baseDecimals: 6,
    runtimeDecimals: 6,
    wide: false,
    policy: 'base',
  },
  {
    id: 1002,
    code: 'ETH',
    name: 'Ether',
    type: 'crypto',
    minorDecimals: 18,
    baseDecimals: 18,
    runtimeDecimals: 18,
    wide: true,
    policy: 'minimal',
  },
];

/** The fiat entry of an ISO currency. */
export function fiatEntry(iso: IsoCurrency): PublishedCurrency {
  return {
    id: iso.id,
    code: iso.code,
    name: iso.name,
    type: 'fiat',
    minorDecimals: iso.minorDecimals,
    baseDecimals: FIAT_DECIMALS,
    runtimeDecimals: FIAT_DECIMALS,
    wide: false,
    policy: 'base',
  };
}

/**
 * The currencies of ISO 4217 list one (the XML the maintenance agency publishes), one per alphabetic code. An entry
 * whose minor unit is not a number (gold, the SDR, the testing code) is not money of account and is skipped.
 */
export function parseIso4217(
  xml: string,
): Result<{ readonly published: string; readonly currencies: IsoCurrency[] }, string> {
  const published = /<ISO_4217 Pblshd="(\d{4}-\d{2}-\d{2})">/.exec(xml)?.[1];
  if (published === undefined) return err('no <ISO_4217 Pblshd="YYYY-MM-DD"> root');
  const byCode = new Map<string, IsoCurrency>();
  for (const entry of xml.matchAll(/<CcyNtry>([\s\S]*?)<\/CcyNtry>/g)) {
    const field = (tag: string) => new RegExp(`<${tag}(?: [^>]*)?>([^<]*)</${tag}>`).exec(entry[1])?.[1]?.trim();
    const code = field('Ccy');
    if (code === undefined) continue;
    const number = field('CcyNbr');
    const minor = field('CcyMnrUnts');
    const name = field('CcyNm');
    if (number === undefined || minor === undefined || name === undefined || !/^\d{3}$/.test(number)) {
      return err(`entry ${code} lacks its number, minor unit or name`);
    }
    if (!/^\d+$/.test(minor)) continue;
    const iso: IsoCurrency = { id: Number(number), code, name, minorDecimals: Number(minor) };
    const seen = byCode.get(code);
    if (seen !== undefined && (seen.id !== iso.id || seen.minorDecimals !== iso.minorDecimals)) {
      return err(`code ${code} appears with two numbers or minor units`);
    }
    byCode.set(code, iso);
  }
  return ok({ published, currencies: [...byCode.values()] });
}

/** Why a publish run is refused: a change the append-only rule forbids, or an entry the cell cannot address. */
export interface ListViolation {
  readonly code:
    | 'ID_REUSED'
    | 'CODE_CHANGED'
    | 'TYPE_CHANGED'
    | 'BASE_CHANGED'
    | 'POLICY_CHANGED'
    | 'RUNTIME_LOWERED'
    | 'WIDE_TURNED_OFF'
    | 'DUPLICATE_CODE'
    | 'ID_OUT_OF_RANGE'
    | 'SCALE_OUT_OF_RANGE';
  readonly id: number;
  readonly currency: string;
  readonly detail: string;
}

/** The largest currency id a cell holds (spec §8: bits 0–23). */
export const MAX_CURRENCY_ID = 0xff_ffff;

/**
 * The next published list: every published entry kept (one the proposal lacks is marked withdrawn), the proposal's
 * mutable fields taken (name, minor unit, a raised runtime scale, `wide` turned on), new entries appended. Every other
 * change is refused by name. The list is ordered by id.
 */
export function publishCurrencyList(
  published: readonly PublishedCurrency[],
  proposal: readonly PublishedCurrency[],
): Result<PublishedCurrency[], ListViolation[]> {
  const violations: ListViolation[] = [];
  const refuse = (code: ListViolation['code'], entry: PublishedCurrency, detail: string) =>
    violations.push({ code, id: entry.id, currency: entry.code, detail });
  const proposed = new Map(proposal.map((entry) => [entry.id, entry]));
  const next: PublishedCurrency[] = [];
  for (const old of published) {
    const now = proposed.get(old.id);
    proposed.delete(old.id);
    if (now === undefined) {
      next.push(old.withdrawn === true ? old : { ...old, withdrawn: true });
      continue;
    }
    if (now.code !== old.code) refuse('CODE_CHANGED', old, `${old.code} -> ${now.code}`);
    if (now.type !== old.type) refuse('TYPE_CHANGED', old, `${old.type} -> ${now.type}`);
    if (now.baseDecimals !== old.baseDecimals)
      refuse('BASE_CHANGED', old, `${old.baseDecimals} -> ${now.baseDecimals}`);
    if (now.policy !== old.policy) refuse('POLICY_CHANGED', old, `${old.policy} -> ${now.policy}`);
    if (now.runtimeDecimals < old.runtimeDecimals) {
      refuse('RUNTIME_LOWERED', old, `${old.runtimeDecimals} -> ${now.runtimeDecimals}`);
    }
    if (old.wide && !now.wide) refuse('WIDE_TURNED_OFF', old, 'wide -> narrow');
    next.push(now);
  }
  next.push(...proposed.values());
  const codes = new Map<string, number>();
  for (const entry of next) {
    const other = codes.get(entry.code);
    if (other !== undefined && entry.withdrawn !== true) refuse('DUPLICATE_CODE', entry, `also id ${other}`);
    if (entry.withdrawn !== true) codes.set(entry.code, entry.id);
    if (!Number.isInteger(entry.id) || entry.id < 0 || entry.id > MAX_CURRENCY_ID) {
      refuse('ID_OUT_OF_RANGE', entry, `id ${entry.id} is outside 0..${MAX_CURRENCY_ID}`);
    }
    const scales = entry.minorDecimals <= entry.runtimeDecimals && entry.baseDecimals <= entry.runtimeDecimals;
    if (!scales || entry.runtimeDecimals - entry.baseDecimals > 31) {
      refuse('SCALE_OUT_OF_RANGE', entry, 'needs minor <= runtime, base <= runtime <= base + 31');
    }
  }
  next.sort((a, b) => a.id - b.id);
  return violations.length > 0 ? err(violations) : ok(next);
}

/** The generated module's source for a published list. */
export function renderCurrencyList(list: readonly PublishedCurrency[], isoPublished: string): string {
  const text = (value: string) => (value.includes("'") && !value.includes('"') ? `"${value}"` : `'${value}'`);
  const rows = list.map((entry) => {
    const fields = [
      `id: ${entry.id}`,
      `code: ${text(entry.code)}`,
      `name: ${text(entry.name)}`,
      `type: '${entry.type}'`,
      `minorDecimals: ${entry.minorDecimals}`,
      `baseDecimals: ${entry.baseDecimals}`,
      `runtimeDecimals: ${entry.runtimeDecimals}`,
      `wide: ${entry.wide}`,
      `policy: '${entry.policy}'`,
      ...(entry.withdrawn === true ? ['withdrawn: true'] : []),
    ];
    return `  {\n${fields.map((field) => `    ${field},`).join('\n')}\n  },`;
  });
  return [
    '// Generated by packages/money/scripts/publish-currencies.ts from ISO 4217 list one',
    `// (published ${isoPublished}) and this package's crypto entries. Append-only: change the script's inputs, never`,
    '// this file.',
    "import type { PublishedCurrency } from './publish.js';",
    '',
    'export const CURRENCY_LIST = [',
    ...rows,
    '] as const satisfies readonly PublishedCurrency[];',
    '',
  ].join('\n');
}
