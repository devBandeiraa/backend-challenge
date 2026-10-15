import type { Logger } from '../observability/logger';

export interface PollingWorkerOptions {
  /** Teto do intervalo entre tentativas enquanto o tick falha (backoff exponencial a partir de intervalMs). */
  maxBackoffMs?: number;
  /** Com falhas seguidas, no máximo uma linha de erro por janela; as demais viram um contador. */
  errorLogEveryMs?: number;
}

/**
 * Loop de polling com parada cooperativa. Enquanto há trabalho (tick > 0) roda em sequência;
 * sem trabalho, dorme `intervalMs`. stop() interrompe o sono e espera o tick corrente terminar:
 * nunca abandona uma transação no meio.
 *
 * Falha (ex.: banco fora): o intervalo dobra a cada falha seguida até maxBackoffMs, e o log de erro
 * sai na 1ª falha e depois no máximo uma vez por janela, com quantas foram suprimidas. Sem isso, N
 * réplicas × workers martelariam o banco e inundariam o log (dezenas de linhas por segundo) durante
 * uma indisponibilidade. A volta ao normal é logada uma vez.
 */
export class PollingWorker {
  private running = false;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;
  private consecutiveFailures = 0;
  private suppressedErrors = 0;
  private lastErrorLogAt = 0;
  private readonly maxBackoffMs: number;
  private readonly errorLogEveryMs: number;

  constructor(
    readonly name: string,
    private readonly tick: () => Promise<number>,
    private readonly intervalMs: number,
    private readonly logger: Logger,
    options: PollingWorkerOptions = {},
  ) {
    this.maxBackoffMs = Math.max(intervalMs, options.maxBackoffMs ?? 30_000);
    this.errorLogEveryMs = options.errorLogEveryMs ?? 30_000;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.loop;
    this.loop = undefined;
  }

  private async run(): Promise<void> {
    while (this.running) {
      let worked = 0;
      let failed = false;
      try {
        worked = await this.tick();
        this.onSuccess();
      } catch (error) {
        failed = true;
        this.onFailure(error);
      }
      if ((worked === 0 || failed) && this.running) await this.sleep(failed ? this.backoffMs() : this.intervalMs);
    }
  }

  /** intervalMs × 2^(falhas − 1), com teto. */
  private backoffMs(): number {
    return Math.min(this.maxBackoffMs, this.intervalMs * 2 ** Math.min(30, this.consecutiveFailures - 1));
  }

  private onFailure(error: unknown): void {
    this.consecutiveFailures += 1;
    const now = Date.now();
    if (this.consecutiveFailures === 1 || now - this.lastErrorLogAt >= this.errorLogEveryMs) {
      this.logger.error({
        msg: `${this.name} tick failed`,
        consecutiveFailures: this.consecutiveFailures,
        suppressedSinceLastLog: this.suppressedErrors,
        nextAttemptInMs: this.backoffMs(),
        err: error,
      });
      this.lastErrorLogAt = now;
      this.suppressedErrors = 0;
    } else {
      this.suppressedErrors += 1;
    }
  }

  private onSuccess(): void {
    if (this.consecutiveFailures === 0) return;
    this.logger.info({ msg: `${this.name} recovered`, failedTicks: this.consecutiveFailures });
    this.consecutiveFailures = 0;
    this.suppressedErrors = 0;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.wake = undefined;
        resolve();
      }, ms);
      this.wake = () => {
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
    });
  }
}
