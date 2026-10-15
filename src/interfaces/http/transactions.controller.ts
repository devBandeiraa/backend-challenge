import { Body, Controller, Get, Headers, Inject, Param, Post, Req, Res } from '@nestjs/common';
import type { Request, Response } from 'express';
import { WalletQueries } from '../../application/queries';
import { ProcessWagerTransactionUseCase, type WagerTransactionOutcome } from '../../application/wagering/process-wager-transaction';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import { type AppMetrics, METRICS } from '../../infrastructure/observability/metrics';
import { correlationIdOf } from './correlation';
import { idempotencyKeyHeader, parse, submitTransactionBody, text, uuidParam } from './validation';

/**
 * O status vem do RESULTADO e é o mesmo no replay ("requisição idêntica → mesma resposta"):
 *   PROCESSED → 200 · PENDING_REFERENCE → 202 · REJECTED/FAILED → 422 (com failureCode)
 * `idempotentReplay` no corpo distingue a primeira execução do replay.
 */
const STATUS_BY_OUTCOME: Record<WagerTransactionStatus, number> = {
  [WagerTransactionStatus.Processed]: 200,
  [WagerTransactionStatus.PendingReference]: 202,
  [WagerTransactionStatus.Rejected]: 422,
  [WagerTransactionStatus.Failed]: 422,
  [WagerTransactionStatus.Pending]: 500, // nunca confirmado: PENDING só existe dentro da transação SQL
};

@Controller()
export class TransactionsController {
  constructor(
    private readonly processTransaction: ProcessWagerTransactionUseCase,
    private readonly queries: WalletQueries,
    @Inject(METRICS) private readonly metrics: AppMetrics,
  ) {}

  @Post('wagering/transactions')
  async submit(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const startedAt = performance.now();
    const key = parse(idempotencyKeyHeader, idempotencyKey, 'Idempotency-Key header');
    const input = parse(submitTransactionBody, body, 'body');
    const correlationId = correlationIdOf(req);
    const outcome = await this.processTransaction.execute({
      ...input,
      idempotencyKey: key,
      correlationId,
      causationId: correlationId,
    });
    this.metrics.recordOutcome('http', outcome, startedAt);
    res.status(STATUS_BY_OUTCOME[outcome.status]);
    return present(outcome);
  }

  @Get('wagering/transactions/:transactionId')
  get(@Param('transactionId') transactionId: string) {
    return this.queries.getTransaction(parse(uuidParam, transactionId, 'transactionId'));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  getByExternalId(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    return this.queries.getTransactionByExternalId(
      parse(text(64), providerId, 'providerId'),
      parse(text(128), externalTransactionId, 'externalTransactionId'),
    );
  }
}

function present(outcome: WagerTransactionOutcome) {
  return {
    transactionId: outcome.transactionId,
    status: outcome.status,
    balance: outcome.balance ?? null,
    ...(outcome.failureCode ? { failureCode: outcome.failureCode } : {}),
    idempotentReplay: outcome.idempotentReplay,
  };
}
