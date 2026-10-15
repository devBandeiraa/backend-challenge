import { describe, expect, test } from 'bun:test';
import { assertConsumerTiming, worstCaseMessageProcessingMs } from '../../../src/infrastructure/config';

const db = { url: '', poolMax: 10, lockTimeoutMs: 5_000, uowMaxAttempts: 5 };
const sqs = (visibilityTimeoutSeconds: number) => ({ visibilityTimeoutSeconds }) as never;

describe('validação de timing do consumer no startup', () => {
  test('pior caso = (tentativas + gravação do FAILED) × lock_timeout + backoff entre tentativas', () => {
    // 6 × 5000 + (10 + 20 + 40 + 80)
    expect(worstCaseMessageProcessingMs(db)).toBe(30_150);
  });

  test('defaults (60 s) passam; 30 s (abaixo de 1,5 × 30,15 s) é recusado na subida', () => {
    expect(() => assertConsumerTiming({ db, sqs: sqs(60) })).not.toThrow();
    expect(() => assertConsumerTiming({ db, sqs: sqs(30) })).toThrow(/need at least 46 s/);
  });

  test('acompanha a configuração: lock_timeout maior exige visibility maior', () => {
    expect(() => assertConsumerTiming({ db: { ...db, lockTimeoutMs: 10_000 }, sqs: sqs(60) })).toThrow();
    expect(() => assertConsumerTiming({ db: { ...db, lockTimeoutMs: 10_000 }, sqs: sqs(91) })).not.toThrow();
  });
});
