import { expect } from 'bun:test';
import * as model from './cache-key-model.js';

/** One model item projected onto a concrete key input or non-input file across two checkouts. */
export interface ProjectedKeyItem {
  /** Stable filename-safe identifier unique to this model `Item`. */
  readonly slug: string;
  /** True when the item appears in the model's `xs` list (must be inside the real key's input set). */
  readonly inKey: boolean;
  /** Bytes `String(model.look(item, e1))` for checkout 1. */
  readonly bytes1: string;
  /** Bytes `String(model.look(item, e2))` for checkout 2. */
  readonly bytes2: string;
}

/** One seeded draw of `(xs, e1, e2)` from `cache-key-model.ts`, projected for differential evaluation. */
export interface DrawnKeyCase {
  readonly entries: readonly ProjectedKeyItem[];
  readonly modelEqual: boolean;
}

/** Coverage counters proving the differential draw exercised both `key_is_pure` and `key_moves_with_input`. */
export interface DifferentialKeyLawSummary {
  readonly total: number;
  readonly agreed: number;
  readonly agreedWithNonInputDiff: number;
  readonly differed: number;
}

function listToArray<T>(items: model.List<T>): T[] {
  const out: T[] = [];
  for (let at = items; at.$ === 'Con'; at = at.tail) out.push(at.head);
  return out;
}

/** Injects every model `Item` onto a distinct filename-safe slug. */
function itemSlug(item: model.Item): string {
  switch (item.$) {
    case 'ISrc':
      return `isrc-${item.lang.$.toLowerCase()}`;
    case 'IRun':
      return `irun-${item.n}`;
    case 'IOut':
      return `iout-${item.p}`;
  }
}

/**
 * Differentially binds a real cache key function to `specs/cowshed/laws/cache_key.bend`:
 * - `key_is_pure` (:790): the key is a function of the bytes of the items it lists, and of nothing else.
 * - `key_moves_with_input` (:828): a key moves when any listed item's bytes differ.
 *
 * Uses the model's own generators and seeds (`SEED_KEY_IS_PURE`, `SEED_KEY_MOVES_WITH_INPUT`) so every draw checks
 * both the model laws and exact equivalence:
 * `realKey(checkout1) === realKey(checkout2)` iff `model.eq_b(model.key(e1, xs), model.key(e2, xs))`.
 */
export async function assertRealKeyMatchesCacheKeyLaws(options: {
  readonly name: string;
  readonly drawsPerSeed: number;
  readonly evaluate: (drawn: DrawnKeyCase) => Promise<readonly [string, string]> | readonly [string, string];
}): Promise<DifferentialKeyLawSummary> {
  const streams: readonly { readonly label: string; readonly seed: bigint; readonly maxLen: number }[] = [
    { label: 'key_is_pure', seed: model.SEED_KEY_IS_PURE, maxLen: model.PROP_LEN_KEY_IS_PURE },
    {
      label: 'key_moves_with_input',
      seed: model.SEED_KEY_MOVES_WITH_INPUT,
      maxLen: model.PROP_LEN_KEY_MOVES_WITH_INPUT,
    },
  ];

  let total = 0;
  let agreed = 0;
  let agreedWithNonInputDiff = 0;
  let differed = 0;

  for (const stream of streams) {
    const rng = new model.Rng(stream.seed);
    for (let index = 0; index < options.drawsPerSeed; index += 1) {
      const xs = model.gen_ListItem(rng, stream.maxLen);
      const e1 = model.gen_ListRow(rng, stream.maxLen);
      const e2 = model.gen_ListRow(rng, stream.maxLen);

      const [pureHolds, pureShown] = model.law_key_is_pure(xs, e1, e2);
      expect(pureHolds, `${stream.label}#${index} model law_key_is_pure: ${pureShown}`).toBe(true);
      const [movesHolds, movesShown] = model.law_key_moves_with_input(xs, e1, e2);
      expect(movesHolds, `${stream.label}#${index} model law_key_moves_with_input: ${movesShown}`).toBe(true);

      const xsItems = listToArray(xs);
      const xsSlugs = new Set(xsItems.map(itemSlug));
      const allItems = new Map<string, model.Item>();
      for (const item of xsItems) allItems.set(itemSlug(item), item);
      for (const row of listToArray(e1)) allItems.set(itemSlug(row.i), row.i);
      for (const row of listToArray(e2)) allItems.set(itemSlug(row.i), row.i);

      const entries: ProjectedKeyItem[] = [...allItems.entries()].map(([slug, item]) => ({
        slug,
        inKey: xsSlugs.has(slug),
        bytes1: String(model.look(item, e1)),
        bytes2: String(model.look(item, e2)),
      }));

      const modelEqual = model.eq_b(model.key(e1, xs), model.key(e2, xs));
      expect(modelEqual).toBe(model.agree_b(e1, e2, xs));
      expect(modelEqual).toBe(!model.differs_b(e1, e2, xs));

      const [key1, key2] = await options.evaluate({ entries, modelEqual });
      const realEqual = key1 === key2;

      if (realEqual !== modelEqual) {
        throw new Error(
          `${options.name} disagreed with cache_key.bend on ${stream.label}#${index}: ` +
            `realEqual=${realEqual} (key1=${key1}, key2=${key2}), modelEqual=${modelEqual}, ` +
            `xs=${model.show_ListItem(xs)}, e1=${model.show_ListRow(e1)}, e2=${model.show_ListRow(e2)}`,
        );
      }

      total += 1;
      if (modelEqual) {
        agreed += 1;
        if (entries.some((entry) => !entry.inKey && entry.bytes1 !== entry.bytes2)) {
          agreedWithNonInputDiff += 1;
        }
      } else {
        differed += 1;
      }
    }
  }

  // Non-vacuity: both key_is_pure (with differing non-input files) and key_moves_with_input must fire.
  expect(agreed, `${options.name}: expected agreeing draws for key_is_pure`).toBeGreaterThan(0);
  expect(
    agreedWithNonInputDiff,
    `${options.name}: expected agreeing draws with differing non-input files for key_is_pure`,
  ).toBeGreaterThan(0);
  expect(differed, `${options.name}: expected differing draws for key_moves_with_input`).toBeGreaterThan(0);

  return { total, agreed, agreedWithNonInputDiff, differed };
}
