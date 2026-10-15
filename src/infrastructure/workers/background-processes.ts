import type { BeforeApplicationShutdown, OnApplicationBootstrap } from '@nestjs/common';
import type { WagerTransactionConsumer } from '../../interfaces/sqs/wager-transaction.consumer';
import type { AppConfig } from '../config';
import type { Logger } from '../observability/logger';
import type { PollingWorker } from './polling-worker';

/**
 * Liga consumer e workers conforme APP_ROLES e os desliga no shutdown do Nest (SIGTERM via
 * enableShutdownHooks). beforeApplicationShutdown roda ANTES de o ORM fechar o pool, então as
 * transações em andamento terminam com conexão disponível.
 * Ordem de parada: primeiro o consumer (para de puxar trabalho novo), depois os workers.
 */
export class BackgroundProcesses implements OnApplicationBootstrap, BeforeApplicationShutdown {
  constructor(
    private readonly config: AppConfig,
    private readonly consumer: WagerTransactionConsumer | undefined,
    private readonly workers: PollingWorker[],
    private readonly logger: Logger,
  ) {}

  onApplicationBootstrap(): void {
    if (this.consumer) this.consumer.start();
    for (const w of this.workers) w.start();
    this.logger.info({
      msg: 'background processes started',
      roles: [...this.config.roles],
      consumer: Boolean(this.consumer),
      workers: this.workers.map((w) => w.name),
    });
  }

  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.logger.info({ msg: 'shutting down background processes', signal });
    await this.consumer?.stop();
    await Promise.all(this.workers.map((w) => w.stop()));
    this.logger.info({ msg: 'background processes stopped' });
  }
}
