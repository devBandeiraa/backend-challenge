type Json = null | boolean | string | number | Json[] | { [key: string]: Json };

/**
 * JSON canônico: chaves de objeto em ordem lexicográfica (code unit), sem espaços,
 * propriedades `undefined` omitidas. Arrays preservam a ordem (ela é semântica).
 * Numbers são rejeitados: dinheiro e ids trafegam como string; um number aqui é bug
 * (e serialização de float não é canônica entre runtimes).
 */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(normalize(value));
}

function normalize(value: unknown): Json {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') throw new TypeError('numbers are not allowed in canonical payloads');
  if (Array.isArray(value)) return value.map(normalize);
  if (typeof value === 'object') {
    const out: { [key: string]: Json } = {};
    for (const key of Object.keys(value as object).sort()) {
      const v = (value as Record<string, unknown>)[key];
      if (v !== undefined) out[key] = normalize(v);
    }
    return out;
  }
  throw new TypeError(`unsupported value in canonical payload: ${typeof value}`);
}
