import type { Logger } from '../observability/logger';

/**
 * Loop de polling com parada cooperativa. Enquanto há trabalho (tick > 0) roda em sequência;
 * sem trabalho, dorme `intervalMs`. stop() interrompe o sono e espera o tick corrente terminar:
 * nunca abandona uma transação no meio.
 */
export class PollingWorker {
  private running = false;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;

  constructor(
    readonly name: string,
    private readonly tick: () => Promise<number>,
    private readonly intervalMs: number,
    private readonly logger: Logger,
  ) {}

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
      try {
        worked = await this.tick();
      } catch (error) {
        this.logger.error({ msg: `${this.name} tick failed`, err: error });
      }
      if (worked === 0 && this.running) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, this.intervalMs);
          this.wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        this.wake = undefined;
      }
    }
  }
}
