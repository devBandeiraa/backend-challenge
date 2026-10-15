import { describe, expect, test } from 'bun:test';
import { canonicalJson } from '../../../src/application/idempotency/canonical-json';
import { hashWagerPayload, type WagerBusinessPayload } from '../../../src/application/idempotency/payload-hash';
import { Money } from '../../../src/domain/shared/money';

const payload: WagerBusinessPayload = {
  providerId: 'provider-a',
  externalTransactionId: 'transaction-123',
  playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
  walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
  roundId: 'round-987',
  gameId: 'fortune-chimp',
  kind: 'BET',
  money: { amount: '25.00', currency: 'BRL' },
};

describe('canonicalJson', () => {
  test('ordena chaves recursivamente e omite undefined', () => {
    expect(canonicalJson({ b: 1 === 1, a: { d: 'x', c: null }, e: undefined })).toBe('{"a":{"c":null,"d":"x"},"b":true}');
  });

  test('rejeita number (dinheiro nunca é number)', () => {
    expect(() => canonicalJson({ amount: 25 })).toThrow(TypeError);
  });
});

describe('payloadHash', () => {
  test('é SHA-256 hex e determinístico', () => {
    const h = hashWagerPayload(payload);
    expect(h).toMatch(/^[0-9a-f]{64}$/);
    expect(hashWagerPayload({ ...payload })).toBe(h);
  });

  test('independe da ordem das chaves de entrada', () => {
    const reordered = Object.fromEntries(Object.entries(payload).reverse()) as unknown as WagerBusinessPayload;
    expect(hashWagerPayload(reordered)).toBe(hashWagerPayload(payload));
  });

  test('"25.5" e "25.50" normalizados por Money geram o mesmo hash', () => {
    const a = { ...payload, money: Money.from({ amount: '25.5', currency: 'BRL' }).toJSON() };
    const b = { ...payload, money: Money.from({ amount: '25.50', currency: 'BRL' }).toJSON() };
    expect(hashWagerPayload(a)).toBe(hashWagerPayload(b));
  });

  test('ignora campos de transporte que não fazem parte do subconjunto de negócio', () => {
    const withTransport = { ...payload, idempotencyKey: 'other', messageId: 'm-1', occurredAt: 'now' } as WagerBusinessPayload;
    expect(hashWagerPayload(withTransport)).toBe(hashWagerPayload(payload));
  });

  test.each<[string, Partial<WagerBusinessPayload>]>([
    ['valor', { money: { amount: '26.00', currency: 'BRL' } }],
    ['moeda', { money: { amount: '25.00', currency: 'USD' } }],
    ['kind', { kind: 'WIN' }],
    ['wallet', { walletId: '0192f291-27dd-7d3f-8071-5f8685deef38' }],
    ['rodada', { roundId: 'round-988' }],
    ['referência', { referenceExternalTransactionId: 'bet-1' }],
  ])('mesma idempotency key com %s divergente produz hash diferente (conflito, não replay)', (_label, change) => {
    expect(hashWagerPayload({ ...payload, ...change })).not.toBe(hashWagerPayload(payload));
  });
});
