// Generates real TS-SDK serialization fixtures. DBOS TS (`@dbos-inc/dbos-sdk` 4.21.x) encodes with
// `superjson` wrapped in a `{__dbos_serializer:"superjson"}` envelope (see ts/src/serialization.ts),
// and a legacy `DBOSJSON` format using a Date/BigInt replacer (serialization = NULL). We reproduce
// both byte-for-byte using the same `superjson` library.
import superjson from 'superjson';
import { writeFileSync } from 'fs';

const SJ_KEY = '__dbos_serializer', SJ_VAL = 'superjson';
const dbosjson = (v) => JSON.stringify({ ...superjson.serialize(v), [SJ_KEY]: SJ_VAL });

function DBOSReplacer(key, value) {
  const a = this[key];
  if (a instanceof Date) return { dbos_type: 'dbos_Date', dbos_data: a.toISOString() };
  if (typeof a === 'bigint') return { dbos_type: 'dbos_BigInt', dbos_data: a.toString() };
  return value;
}
const legacy = (v) => JSON.stringify(v, DBOSReplacer);

const date = new Date('2024-01-02T03:04:05.000Z');
const fx = {
  // SuperJSON envelope rows (serialization = 'js_superjson')
  sj_int: dbosjson(42),
  sj_string: dbosjson('hello'),
  sj_bool: dbosjson(true),
  sj_null: dbosjson(null),
  sj_array: dbosjson([1, 2, 3]),
  sj_object: dbosjson({ a: 1, b: { c: 'x' }, d: [true, null] }),
  sj_date: dbosjson(date),
  sj_bigint: dbosjson(9007199254740993n),
  sj_map: dbosjson(new Map([['a', 1], ['b', 2]])),
  sj_set: dbosjson(new Set([1, 2, 3])),
  sj_object_with_date: dbosjson({ when: date, n: 5 }),
  sj_undefined_field: dbosjson({ a: 1, b: undefined }),
  // Legacy DBOSJSON rows (serialization = NULL)
  legacy_int: legacy(42),
  legacy_object_with_date: legacy({ when: date }),
  legacy_object_with_bigint: legacy({ big: 123456789012345n }),
};

const out = new URL('../../crates/dbos/tests/fixtures/ts_serialization.json', import.meta.url);
writeFileSync(out, JSON.stringify(fx, null, 2) + '\n');
console.log(`superjson ${superjson.constructor?.name ? 'instance' : ''} wrote ${Object.keys(fx).length} fixtures`);
