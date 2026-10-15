import { DomainError } from '../../domain/shared/domain-error';
import { Money, type MoneyProps } from '../../domain/shared/money';
import type { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { SubmittableKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import { IdempotencyConflictError, UnknownWalletError, ValidationError } from '../errors';
import { hashWagerPayload } from '../idempotency/payload-hash';
import type { Clock, IdGenerator, TransactionScope, UnitOfWork } from '../ports';
import { TransactionSettler } from './settle-transaction';

/** Contrato único de entrada: HTTP e SQS montam este comando (mesmo use case). */
export interface SubmitWagerTransactionCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  /** União de literais ("BET" | ...): é o formato que chega pelo HTTP/SQS. */
  kind: `${SubmittableKind}`;
  money: MoneyProps;
  referenceExternalTransactionId?: string | undefined;
  correlationId: string;
  /** messageId (SQS) ou request id (HTTP). */
  causationId?: string | undefined;
}

export interface WagerTransactionOutcome {
  transactionId: string;
  status: WagerTransactionStatus;
  /** Saldo observado quando a transação ficou terminal (moeda da wallet). Ausente em PENDING_REFERENCE/FAILED. */
  balance: MoneyProps | undefined;
  failureCode: FailureCode | undefined;
  idempotentReplay: boolean;
}

/** Hook para o consumer SQS gravar a inbox na MESMA transação (Fase 2). */
export type ScopeHook = (scope: TransactionScope, outcome: { transactionId: string | undefined }) => Promise<void>;

export class ProcessWagerTransactionUseCase {
  private readonly settler: TransactionSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {
    this.settler = new TransactionSettler(clock, ids);
  }

  async execute(command: SubmitWagerTransactionCommand, afterSettle?: ScopeHook): Promise<WagerTransactionOutcome> {
    const money = this.parseMoney(command.money);
    // Hash calculado sobre o Money normalizado: "25.5" e "25.50" são o mesmo payload.
    const payloadHash = hashWagerPayload({ ...command, money: money.toJSON() });

    return this.uow.run(async (scope) => {
      // 1) Existência sem lock. Wallets nunca são apagadas (trigger), então o resultado não fica obsoleto.
      //    Sem efeito financeiro: a redelivery apenas recalcula o mesmo resultado.
      const exists = await scope.wallets.findById(command.walletId);
      if (!exists) throw new UnknownWalletError(command.walletId);

      // 2) Insert-first: a unicidade no banco decide quem é o "dono" da operação.
      //    Duplicatas concorrentes ESPERAM no índice único até a original confirmar.
      const tx = this.buildTransaction(command, money, payloadHash);
      const inserted = await scope.transactions.insertIfAbsent(tx);
      if (!inserted) {
        const outcome = await this.replay(scope, command, payloadHash);
        await afterSettle?.(scope, { transactionId: outcome.transactionId });
        return outcome;
      }

      // 3) Lock por wallet (nunca global) só para quem vai de fato mover saldo.
      const wallet = await scope.wallets.lockById(command.walletId);
      if (!wallet) throw new Error(`wallet ${command.walletId} vanished under lock`); // impossível: sem DELETE

      await this.settler.settle(scope, tx, wallet, { correlationId: command.correlationId, causationId: command.causationId });
      await afterSettle?.(scope, { transactionId: tx.id });
      return toOutcome(tx, false);
    });
  }

  private async replay(
    scope: TransactionScope,
    command: SubmitWagerTransactionCommand,
    payloadHash: string,
  ): Promise<WagerTransactionOutcome> {
    const byKey = await scope.transactions.findByIdempotencyKey(command.idempotencyKey);
    if (byKey) {
      if (!byKey.matchesPayload(payloadHash)) {
        throw new IdempotencyConflictError(
          'IDEMPOTENCY_KEY_PAYLOAD_MISMATCH',
          'Idempotency-Key already used with a different payload',
          byKey.id,
        );
      }
      return toOutcome(byKey, true);
    }
    const byExternal = await scope.transactions.findByExternalId(command.providerId, command.externalTransactionId);
    if (byExternal) {
      throw new IdempotencyConflictError(
        'EXTERNAL_TRANSACTION_ID_CONFLICT',
        'externalTransactionId already submitted under a different Idempotency-Key',
        byExternal.id,
      );
    }
    // ON CONFLICT só não insere se a linha conflitante estiver confirmada; sumir é impossível (sem DELETE).
    throw new Error('insert conflicted but no conflicting transaction was found');
  }

  private buildTransaction(command: SubmitWagerTransactionCommand, money: Money, payloadHash: string): WagerTransaction {
    try {
      return WagerTransaction.create({
        id: this.ids.next(),
        providerId: command.providerId,
        externalTransactionId: command.externalTransactionId,
        idempotencyKey: command.idempotencyKey,
        payloadHash,
        walletId: command.walletId,
        playerId: command.playerId,
        roundId: command.roundId,
        gameId: command.gameId,
        kind: command.kind as SubmittableKind,
        money,
        referenceExternalTransactionId: command.referenceExternalTransactionId,
        correlationId: command.correlationId,
        createdAt: this.clock.now(),
      });
    } catch (error) {
      if (error instanceof DomainError) throw new ValidationError(error.code, error.message);
      throw error;
    }
  }

  private parseMoney(props: MoneyProps): Money {
    try {
      return Money.from(props);
    } catch (error) {
      if (error instanceof DomainError) throw new ValidationError(error.code, error.message);
      throw error;
    }
  }
}

export function toOutcome(tx: WagerTransaction, idempotentReplay: boolean): WagerTransactionOutcome {
  return {
    transactionId: tx.id,
    status: tx.status,
    balance: tx.result?.balance.toJSON(),
    failureCode: tx.failureCode,
    idempotentReplay,
  };
}
