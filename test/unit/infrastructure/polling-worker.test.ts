import { describe, expect, test } from 'bun:test';
import { createLogger } from '../../../src/infrastructure/observability/logger';
import { PollingWorker } from '../../../src/infrastructure/workers/polling-worker';

function capturingLogger() {
  const lines: Record<string, any>[] = [];
  const logger = createLogger({}, { write: (l: string) => void lines.push(JSON.parse(l)) });
  logger.level = 'info';
  return { logger, lines };
}

describe('PollingWorker com falhas seguidas (ex.: banco fora)', () => {
  test('intervalo dobra até o teto; erro logado uma vez por janela; volta ao normal logada uma vez', async () => {
    const { logger, lines } = capturingLogger();
    const FAILURES = 6;
    const calls: number[] = [];
    let recovered!: () => void;
    const done = new Promise<void>((r) => (recovered = r));
    const worker = new PollingWorker(
      'flaky',
      async () => {
        calls.push(performance.now());
        if (calls.length <= FAILURES) throw new Error(`database unavailable (${calls.length})`);
        recovered();
        return 0;
      },
      10,
      logger,
      { maxBackoffMs: 80, errorLogEveryMs: 60_000 },
    );
    worker.start();
    await done;
    await worker.stop();

    // espera entre tentativas: 10, 20, 40, 80, 80, 80 ms (backoff com teto), não 10 ms fixos
    const gaps = calls.slice(1, FAILURES + 1).map((t, i) => t - calls[i]!);
    expect(gaps[1]!).toBeGreaterThanOrEqual(18);
    expect(gaps[2]!).toBeGreaterThanOrEqual(36);
    expect(gaps[3]!).toBeGreaterThanOrEqual(72);
    expect(Math.max(...gaps)).toBeLessThan(80 + 150); // teto respeitado (com folga de agendamento)

    // 6 falhas → 1 linha de erro (as outras 5 caem na mesma janela e são suprimidas)
    const errors = lines.filter((l) => l.msg === 'flaky tick failed');
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ level: 'error', consecutiveFailures: 1 });
    const back = lines.filter((l) => l.msg === 'flaky recovered');
    expect(back).toHaveLength(1);
    expect(back[0]).toMatchObject({ failedTicks: FAILURES });
  });

  test('falha prolongada: um erro por janela, com o número de falhas suprimidas desde o anterior', async () => {
    const { logger, lines } = capturingLogger();
    let calls = 0;
    const worker = new PollingWorker(
      'down',
      async () => {
        calls += 1;
        throw new Error('still down');
      },
      5,
      logger,
      { maxBackoffMs: 5, errorLogEveryMs: 100 },
    );
    worker.start();
    await Bun.sleep(350);
    await worker.stop();

    const errors = lines.filter((l) => l.msg === 'down tick failed');
    expect(calls).toBeGreaterThan(20);
    expect(errors.length).toBeGreaterThanOrEqual(2);
    expect(errors.length).toBeLessThanOrEqual(5); // ~350 ms / janela de 100 ms, não uma linha por falha
    expect(errors.slice(1).every((e) => e.suppressedSinceLastLog > 0)).toBe(true);
  });
});
