import { type INestApplication, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { MikroORM } from '@mikro-orm/postgresql';
import { CLOCK, type Clock, ID_GENERATOR, type IdGenerator, UNIT_OF_WORK, type UnitOfWork } from './application/ports';
import { WalletQueries } from './application/queries';
import { ProcessWagerTransactionUseCase } from './application/wagering/process-wager-transaction';
import { ResolvePendingReferencesUseCase } from './application/wagering/resolve-pending-references';
import type { PendingReferencePolicy } from './application/wagering/settle-transaction';
import { OpenWalletUseCase } from './application/wallets/open-wallet';
import { ReconcileWalletUseCase } from './application/wallets/reconcile-wallet';
import { ExponentialBackoff } from './domain/messaging/backoff-policy';
import { APP_CONFIG, type AppConfig, assertConsumerTiming, loadConfig, maxMessageProcessingMs } from './infrastructure/config';
import { createSqsClient } from './infrastructure/messaging/sqs/sqs-config';
import { SQS_GATEWAY, SqsGateway } from './infrastructure/messaging/sqs/sqs-gateway';
import { enrichContext, runWithContext } from './infrastructure/observability/context';
import { createLogger, type Logger, PinoNestLogger } from './infrastructure/observability/logger';
import { AppMetrics, METRICS } from './infrastructure/observability/metrics';
import { APP_APPLICATION_NAME_PREFIX } from './infrastructure/persistence/database-urls';
import { buildOrmConfig } from './infrastructure/persistence/mikro-orm.config';
import { MikroOrmUnitOfWork } from './infrastructure/persistence/mikro-orm-unit-of-work';
import { isTransientDbError } from './infrastructure/persistence/pg-errors';
import { SystemClock, UuidV7Generator } from './infrastructure/system';
import { BackgroundProcesses } from './infrastructure/workers/background-processes';
import { OutboxPublisher } from './infrastructure/workers/outbox-publisher';
import { PollingWorker } from './infrastructure/workers/polling-worker';
import { ProviderAuthGuard } from './interfaces/http/auth.guard';
import { correlationMiddleware, requestLogMiddleware } from './interfaces/http/correlation';
import { HttpErrorFilter } from './interfaces/http/error-filter';
import { HealthController } from './interfaces/http/health.controller';
import { MetricsController } from './interfaces/http/metrics.controller';
import { TransactionsController } from './interfaces/http/transactions.controller';
import { WalletsController } from './interfaces/http/wallets.controller';
import { WagerTransactionConsumer } from './interfaces/sqs/wager-transaction.consumer';

export const LOGGER = Symbol('Logger');
export const NEST_LOGGER = Symbol('NestLogger');
const PENDING_POLICY = Symbol('PendingReferencePolicy');

// Use cases são classes puras (sem decorators do Nest): o Nest só as monta via factory.
// Isso mantém application/ e domain/ independentes do framework.
const useCases = [
  {
    provide: OpenWalletUseCase,
    useFactory: (uow: UnitOfWork, clock: Clock, ids: IdGenerator) => new OpenWalletUseCase(uow, clock, ids),
    inject: [UNIT_OF_WORK, CLOCK, ID_GENERATOR],
  },
  {
    provide: ProcessWagerTransactionUseCase,
    useFactory: (uow: UnitOfWork, clock: Clock, ids: IdGenerator, policy: PendingReferencePolicy) =>
      new ProcessWagerTransactionUseCase(uow, clock, ids, policy),
    inject: [UNIT_OF_WORK, CLOCK, ID_GENERATOR, PENDING_POLICY],
  },
  {
    provide: ResolvePendingReferencesUseCase,
    useFactory: (uow: UnitOfWork, clock: Clock, ids: IdGenerator, policy: PendingReferencePolicy) =>
      new ResolvePendingReferencesUseCase(uow, clock, ids, policy, isTransientDbError, {
        run: (fn) => runWithContext({}, fn),
        annotate: (fields) => enrichContext(fields),
      }),
    inject: [UNIT_OF_WORK, CLOCK, ID_GENERATOR, PENDING_POLICY],
  },
  { provide: WalletQueries, useFactory: (uow: UnitOfWork) => new WalletQueries(uow), inject: [UNIT_OF_WORK] },
  {
    provide: ReconcileWalletUseCase,
    useFactory: (uow: UnitOfWork, metrics: AppMetrics, logger: Logger) =>
      new ReconcileWalletUseCase(uow, {
        // Divergência: log de erro (sem saldos completos, só a diferença) + métrica. Nada é corrigido.
        onMismatch: (r) => {
          metrics.reconciliationMismatches.inc();
          logger.error({
            msg: 'reconciliation mismatch',
            walletId: r.walletId,
            difference: r.difference.amount,
            currency: r.difference.currency,
            walletVersion: r.walletVersion,
            ledgerVersion: r.ledgerVersion,
            checkedEntries: r.checkedEntries,
          });
        },
      }),
    inject: [UNIT_OF_WORK, METRICS, LOGGER],
  },
];

const messaging = [
  {
    provide: SQS_GATEWAY,
    // Resolve as URLs na subida: sem SQS a instância não sobe (o compose reinicia até o emulador responder).
    useFactory: (config: AppConfig) =>
      SqsGateway.connect(createSqsClient(config.sqs.endpoint), {
        input: config.sqs.inputQueue,
        deadLetter: config.sqs.deadLetterQueue,
        events: config.sqs.eventsQueue,
      }),
    inject: [APP_CONFIG],
  },
  {
    provide: BackgroundProcesses,
    useFactory: (
      config: AppConfig,
      orm: MikroORM,
      gateway: SqsGateway,
      metrics: AppMetrics,
      logger: Logger,
      processTransaction: ProcessWagerTransactionUseCase,
      resolvePending: ResolvePendingReferencesUseCase,
    ) => {
      if (config.roles.has('consumer')) assertConsumerTiming(config); // falha na subida, não em produção
      const consumer = config.roles.has('consumer')
        ? new WagerTransactionConsumer(gateway, processTransaction, metrics, logger.child({ component: 'sqs-consumer' }), {
            consumerName: config.sqs.consumerName,
            maxAttempts: config.sqs.maxAttempts,
            waitTimeSeconds: config.sqs.waitTimeSeconds,
            visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds,
            batchSize: config.sqs.batchSize,
            shutdownGraceMs: config.sqs.shutdownGraceMs,
            maxProcessingMs: maxMessageProcessingMs(config.db),
          })
        : undefined;

      const workers: PollingWorker[] = [];
      if (config.roles.has('outbox')) {
        const publisher = new OutboxPublisher(orm, gateway, metrics, logger.child({ component: 'outbox' }), {
          batchSize: config.outbox.batchSize,
          statementTimeoutMs: config.db.statementTimeoutMs,
        });
        let lastLagRefresh = 0;
        workers.push(
          new PollingWorker(
            'outbox-publisher',
            async () => {
              const { published, failed } = await publisher.publishOnce();
              if (Date.now() - lastLagRefresh > 1_000) {
                lastLagRefresh = Date.now();
                await publisher.refreshLag();
              }
              return published + failed > 0 && failed === 0 ? published : 0;
            },
            config.outbox.intervalMs,
            logger,
          ),
        );
      }
      if (config.roles.has('pending-reference')) {
        workers.push(
          new PollingWorker(
            'pending-reference',
            async () => {
              const results = await resolvePending.runOnce(config.pendingReference.batchSize);
              for (const r of results) {
                metrics.pendingReferenceResolutions.inc({ result: r.status });
                // com correlationId/transactionId/walletId/providerId da linha (escopo de log próprio)
                runWithContext(r.trace ?? { transactionId: r.transactionId }, () =>
                  logger.info({ msg: 'pending reference attempt', status: r.status }),
                );
              }
              return results.length;
            },
            config.pendingReference.intervalMs,
            logger,
          ),
        );
      }
      return new BackgroundProcesses(config, consumer, workers, logger);
    },
    inject: [
      APP_CONFIG,
      MikroORM,
      SQS_GATEWAY,
      METRICS,
      LOGGER,
      ProcessWagerTransactionUseCase,
      ResolvePendingReferencesUseCase,
    ],
  },
];

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      useFactory: () => {
        const { db, instanceId } = loadConfig();
        // Conexão que cai (ociosa ou em uso) é descartada pelo pool; aqui só fica registrada.
        const poolLogger = createLogger({ instanceId, component: 'db-pool' });
        return buildOrmConfig({
          clientUrl: db.url,
          poolMax: db.poolMax,
          connectTimeoutMs: db.connectTimeoutMs,
          acquireTimeoutMs: db.acquireTimeoutMs,
          queryTimeoutMs: db.queryTimeoutMs,
          applicationName: `${APP_APPLICATION_NAME_PREFIX}${instanceId}`.slice(0, 63),
          onConnectionError: (error, state) => poolLogger.warn({ msg: 'database connection error; client discarded', state, err: error }),
        });
      },
    }),
  ],
  controllers: [HealthController, MetricsController, WalletsController, TransactionsController],
  providers: [
    { provide: APP_CONFIG, useFactory: loadConfig },
    { provide: LOGGER, useFactory: (config: AppConfig) => createLogger({ instanceId: config.instanceId }), inject: [APP_CONFIG] },
    { provide: METRICS, useFactory: () => new AppMetrics({ defaultMetrics: true }) },
    { provide: CLOCK, useClass: SystemClock },
    { provide: ID_GENERATOR, useClass: UuidV7Generator },
    {
      provide: PENDING_POLICY,
      useFactory: ({ pendingReference: p }: AppConfig): PendingReferencePolicy => ({
        maxAttempts: p.maxAttempts,
        backoff: new ExponentialBackoff({ baseMs: p.baseDelayMs, factor: 2, maxMs: p.maxDelayMs, jitterRatio: 0.2 }),
      }),
      inject: [APP_CONFIG],
    },
    {
      provide: UNIT_OF_WORK,
      useFactory: (orm: MikroORM, config: AppConfig, metrics: AppMetrics, logger: Logger) =>
        new MikroOrmUnitOfWork(
          orm,
          { maxAttempts: config.db.uowMaxAttempts, lockTimeoutMs: config.db.lockTimeoutMs, statementTimeoutMs: config.db.statementTimeoutMs },
          {
            onRetry: ({ attempt, sqlState, constraint }) => {
              metrics.lockConflicts.inc({ sqlstate: sqlState ?? 'unknown' });
              logger.warn({ msg: 'transaction retry after concurrency conflict', attempt, sqlState, constraint });
            },
            onExhausted: ({ attempts, sqlState }) => {
              metrics.retriesExhausted.inc();
              logger.error({ msg: 'transaction retries exhausted', attempts, sqlState });
            },
          },
        ),
      inject: [MikroORM, APP_CONFIG, METRICS, LOGGER],
    },
    ...useCases,
    ...messaging,
    { provide: APP_GUARD, useClass: ProviderAuthGuard },
    { provide: NEST_LOGGER, useFactory: (logger: Logger) => new PinoNestLogger(logger.child({ component: 'nest' })), inject: [LOGGER] },
  ],
})
export class AppModule {}

/** Configuração HTTP compartilhada entre main.ts e os testes e2e (mesmo comportamento). */
export function configureHttp(app: INestApplication): void {
  const { instanceId } = app.get<AppConfig>(APP_CONFIG);
  app.use((_req: unknown, res: { setHeader(k: string, v: string): void }, next: () => void) => {
    // Diagnóstico de balanceamento: qual réplica atendeu (usado no teste multi-instância).
    res.setHeader('X-Instance-Id', instanceId);
    next();
  });
  const logger = app.get<Logger>(LOGGER);
  app.use(correlationMiddleware);
  app.use(requestLogMiddleware(logger));
  app.useGlobalFilters(new HttpErrorFilter(logger));
  app.enableShutdownHooks();
}
