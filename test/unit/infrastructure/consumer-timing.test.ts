import { describe, expect, test } from 'bun:test';
import {
  assertConsumerTiming,
  maxMessageProcessingMs,
  requiredShutdownGraceMs,
  worstCaseMessageProcessingMs,
} from '../../../src/infrastructure/config';

const db = { url: '', poolMax: 10, lockTimeoutMs: 5_000, statementTimeoutMs: 15_000, uowMaxAttempts: 5 };
// defaults do compose: long poll de 5 s, SHUTDOWN_GRACE_MS 60 s
const sqs = (visibilityTimeoutSeconds: number, shutdownGraceMs = 60_000, waitTimeSeconds = 5) =>
  ({ visibilityTimeoutSeconds, shutdownGraceMs, waitTimeSeconds }) as never;
const BACKOFF = 10 + 20 + 40 + 80;

describe('validação de timing do consumer no startup', () => {
  test('pior caso = max(caminho do lock, caminho da instrução travada) + backoff entre tentativas', () => {
    // lock: (5 + 1) × 5000 = 30000; instrução: (5 − 1) × 5000 + 2 × 15000 = 50000
    expect(worstCaseMessageProcessingMs(db)).toBe(50_000 + BACKOFF);
    // com statement_timeout > 2 × lock_timeout (validado na subida) o caminho da instrução domina
    expect(worstCaseMessageProcessingMs({ ...db, statementTimeoutMs: 10_001 })).toBeGreaterThanOrEqual(30_000 + BACKOFF);
  });

  test('defaults (90 s) passam; 75 s (abaixo de 1,5 × 50,15 s) é recusado na subida', () => {
    expect(() => assertConsumerTiming({ db, sqs: sqs(90) })).not.toThrow();
    expect(() => assertConsumerTiming({ db, sqs: sqs(75) })).toThrow(/need at least 76 s/);
  });

  test('acompanha a configuração: lock_timeout e statement_timeout maiores exigem visibility maior', () => {
    const slow = { ...db, lockTimeoutMs: 10_000, statementTimeoutMs: 25_000 }; // 4×10 + 2×25 = 90 s
    expect(() => assertConsumerTiming({ db: slow, sqs: sqs(120, 100_000) })).toThrow(/VISIBILITY/);
    expect(() => assertConsumerTiming({ db: slow, sqs: sqs(136, 100_000) })).not.toThrow();
    expect(() => assertConsumerTiming({ db: slow, sqs: sqs(136, 60_000) })).toThrow(/SHUTDOWN_GRACE_MS/); // 5 + 90,15 s
  });

  test('prazo de shutdown >= long poll em andamento + pior caso: 5 s + 50,15 s = 55,15 s', () => {
    expect(requiredShutdownGraceMs(db, 5)).toBe(5_000 + 50_000 + BACKOFF);
    expect(() => assertConsumerTiming({ db, sqs: sqs(90, 60_000, 5) })).not.toThrow();
    expect(() => assertConsumerTiming({ db, sqs: sqs(90, 20_000, 5) })).toThrow(/SHUTDOWN_GRACE_MS=20000 .* need at least 55150 ms/);
    // long poll maior exige prazo maior
    expect(() => assertConsumerTiming({ db, sqs: sqs(90, 60_000, 20) })).toThrow(/need at least 70150 ms/);
  });

  test('o teto do heartbeat é o mesmo pior caso × 1,5', () => {
    expect(maxMessageProcessingMs(db)).toBe(Math.ceil((50_000 + BACKOFF) * 1.5));
  });
});
