/**
 * Publishes the canonical currency list: reads ISO 4217 list one (a path argument, or the maintenance agency's URL),
 * merges it with the package's crypto entries under the append-only rule, and writes the generated module.
 *
 *   bun scripts/publish-currencies.ts [list-one.xml]
 */

import { CURRENCY_LIST } from '../src/currency-list.generated.js';
import { CRYPTO_CURRENCIES, fiatEntry, parseIso4217, publishCurrencyList, renderCurrencyList } from '../src/publish.js';

const ISO_LIST_ONE =
  'https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml';

const source = process.argv[2];
const xml = source === undefined ? await (await fetch(ISO_LIST_ONE)).text() : await Bun.file(source).text();
const iso = parseIso4217(xml);
if (!iso.ok) {
  console.error(`ISO 4217 list one: ${iso.error}`);
  process.exit(1);
}
const next = publishCurrencyList(CURRENCY_LIST, [...iso.value.currencies.map(fiatEntry), ...CRYPTO_CURRENCIES]);
if (!next.ok) {
  for (const v of next.error) console.error(`${v.code} ${v.currency} (${v.id}): ${v.detail}`);
  process.exit(1);
}
await Bun.write(
  new URL('../src/currency-list.generated.ts', import.meta.url),
  renderCurrencyList(next.value, iso.value.published),
);
console.log(`published ${next.value.length} currencies (ISO 4217 list one of ${iso.value.published})`);
