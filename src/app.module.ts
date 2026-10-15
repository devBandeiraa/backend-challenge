import { type INestApplication, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { MikroORM } from '@mikro-orm/postgresql';
import { CLOCK, type Clock, ID_GENERATOR, type IdGenerator, UNIT_OF_WORK, type UnitOfWork } from './application/ports';
import { WalletQueries } from './application/queries';
import { ProcessWagerTransactionUseCase } from './application/wagering/process-wager-transaction';
import { OpenWalletUseCase } from './application/wallets/open-wallet';
import { APP_CONFIG, type AppConfig, loadConfig } from './infrastructure/config';
import { AppMetrics, METRICS } from './infrastructure/observability/metrics';
import { buildOrmConfig } from './infrastructure/persistence/mikro-orm.config';
import { MikroOrmUnitOfWork } from './infrastructure/persistence/mikro-orm-unit-of-work';
import { SystemClock, UuidV7Generator } from './infrastructure/system';
import { ProviderAuthGuard } from './interfaces/http/auth.guard';
import { correlationMiddleware } from './interfaces/http/correlation';
import { HttpErrorFilter } from './interfaces/http/error-filter';
import { HealthController } from './interfaces/http/health.controller';
import { MetricsController } from './interfaces/http/metrics.controller';
import { TransactionsController } from './interfaces/http/transactions.controller';
import { WalletsController } from './interfaces/http/wallets.controller';

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
    useFactory: (uow: UnitOfWork, clock: Clock, ids: IdGenerator) => new ProcessWagerTransactionUseCase(uow, clock, ids),
    inject: [UNIT_OF_WORK, CLOCK, ID_GENERATOR],
  },
  { provide: WalletQueries, useFactory: (uow: UnitOfWork) => new WalletQueries(uow), inject: [UNIT_OF_WORK] },
];

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      useFactory: () => {
        const { db } = loadConfig();
        return buildOrmConfig({ clientUrl: db.url, poolMax: db.poolMax });
      },
    }),
  ],
  controllers: [HealthController, MetricsController, WalletsController, TransactionsController],
  providers: [
    { provide: APP_CONFIG, useFactory: loadConfig },
    { provide: METRICS, useFactory: () => new AppMetrics({ defaultMetrics: true }) },
    { provide: CLOCK, useClass: SystemClock },
    { provide: ID_GENERATOR, useClass: UuidV7Generator },
    {
      provide: UNIT_OF_WORK,
      useFactory: (orm: MikroORM, config: AppConfig, metrics: AppMetrics) =>
        new MikroOrmUnitOfWork(
          orm,
          { maxAttempts: config.db.uowMaxAttempts, lockTimeoutMs: config.db.lockTimeoutMs },
          {
            onRetry: ({ sqlState }) => metrics.lockConflicts.inc({ sqlstate: sqlState ?? 'unknown' }),
            onExhausted: () => metrics.retriesExhausted.inc(),
          },
        ),
      inject: [MikroORM, APP_CONFIG, METRICS],
    },
    ...useCases,
    { provide: APP_GUARD, useClass: ProviderAuthGuard },
  ],
})
export class AppModule {}

/** Configuração HTTP compartilhada entre main.ts e os testes e2e (mesmo comportamento). */
export function configureHttp(app: INestApplication): void {
  app.use(correlationMiddleware);
  app.useGlobalFilters(new HttpErrorFilter());
  app.enableShutdownHooks();
}
