import { describe, expect, test } from 'bun:test';
import { assertConsumerTiming, maxMessageProcessingMs, worstCaseMessageProcessingMs } from '../../../src/infrastructure/config';

const db = { url: '', poolMax: 10, lockTimeoutMs: 5_000, statementTimeoutMs: 8_000, uowMaxAttempts: 5 };
const sqs = (visibilityTimeoutSeconds: number) => ({ visibilityTimeoutSeconds }) as never;
const BACKOFF = 10 + 20 + 40 + 80;

describe('validação de timing do consumer no startup', () => {
  test('pior caso = max(caminho do lock, caminho da instrução lenta) + backoff entre tentativas', () => {
    // lock: (5 + 1) × 5000 = 30000; instrução: (5 − 1) × 5000 + 2 × 8000 = 36000
    expect(worstCaseMessageProcessingMs(db)).toBe(36_000 + BACKOFF);
    // com statement_timeout > lock_timeout (validado na subida) o caminho da instrução nunca fica abaixo do lock
    expect(worstCaseMessageProcessingMs({ ...db, statementTimeoutMs: 5_001 })).toBeGreaterThanOrEqual(30_000 + BACKOFF);
  });

  test('defaults (60 s) passam; 50 s (abaixo de 1,5 × 36,15 s) é recusado na subida', () => {
    expect(() => assertConsumerTiming({ db, sqs: sqs(60) })).not.toThrow();
    expect(() => assertConsumerTiming({ db, sqs: sqs(50) })).toThrow(/need at least 55 s/);
  });

  test('acompanha a configuração: lock_timeout e statement_timeout maiores exigem visibility maior', () => {
    const slow = { ...db, lockTimeoutMs: 10_000, statementTimeoutMs: 12_000 }; // 4×10 + 2×12 = 64 s
    expect(() => assertConsumerTiming({ db: slow, sqs: sqs(60) })).toThrow();
    expect(() => assertConsumerTiming({ db: slow, sqs: sqs(97) })).not.toThrow();
  });

  test('o teto do heartbeat é o mesmo pior caso × 1,5', () => {
    expect(maxMessageProcessingMs(db)).toBe(Math.ceil((36_000 + BACKOFF) * 1.5));
  });
});
