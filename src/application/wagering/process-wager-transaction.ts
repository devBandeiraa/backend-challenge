import { InboxMessage } from '../../domain/messaging/inbox-message';
import { DomainError } from '../../domain/shared/domain-error';
import { Money, type MoneyProps } from '../../domain/shared/money';
import { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { SubmittableKind, WagerTransactionStatus } from '../../domain/wagering/wager-transaction-kind';
import { IdempotencyConflictError, InboxPayloadMismatchError, UnknownWalletError, ValidationError } from '../errors';
import { hashWagerPayload } from '../idempotency/payload-hash';
import type { Clock, IdGenerator, TransactionScope, UnitOfWork } from '../ports';
import { DEFAULT_PENDING_REFERENCE_POLICY, type PendingReferencePolicy, TransactionSettler } from './settle-transaction';

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
  /** União de literais ("PROCESSED" | ...): formato do contrato de saída. */
  status: `${WagerTransactionStatus}`;
  /** Saldo observado quando a transação ficou terminal (moeda da wallet). Ausente em PENDING_REFERENCE/FAILED. */
  balance: MoneyProps | undefined;
  failureCode: FailureCode | undefined;
  idempotentReplay: boolean;
}

/** Identidade de transporte da mensagem SQS, deduplicada pela inbox. */
export interface InboundMessage {
  consumerName: string;
  messageId: string;
  /** Hash do conteúdo da mensagem: mesmo messageId com conteúdo diferente é erro do produtor. */
  payloadHash: string;
}

export interface MessageOutcome extends WagerTransactionOutcome {
  /** A inbox já tinha este messageId (redelivery): nenhum efeito novo foi aplicado. */
  inboxDuplicate: boolean;
}

export class ProcessWagerTransactionUseCase {
  private readonly settler: TransactionSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    pendingPolicy: PendingReferencePolicy = DEFAULT_PENDING_REFERENCE_POLICY,
  ) {
    this.settler = new TransactionSettler(clock, ids, pendingPolicy);
  }

  /** Entrada HTTP. */
  async execute(command: SubmitWagerTransactionCommand): Promise<WagerTransactionOutcome> {
    const { money, payloadHash } = this.prepare(command);
    return this.uow.run((scope) => this.process(scope, command, money, payloadHash));
  }

  /**
   * Entrada SQS: inbox + transação + wallet + ledger + outbox na MESMA transação SQL.
   * Duas camadas de dedup: a inbox (transporte, por messageId) e a idempotency key (negócio).
   * Uma mensagem reenviada pelo SQS cai na inbox; a mesma operação reenviada pelo provedor
   * com outro messageId cai na idempotency key. Nenhuma das duas reaplica efeito.
   */
  async executeFromMessage(command: SubmitWagerTransactionCommand, message: InboundMessage): Promise<MessageOutcome> {
    const { money, payloadHash } = this.prepare(command);
    return this.uow.run(async (scope) => {
      const inbox = InboxMessage.receive({ ...message, receivedAt: this.clock.now() });
      const existing = await scope.inbox.insertIfAbsent(inbox);
      if (existing && !existing.matchesPayload(message.payloadHash)) {
        throw new InboxPayloadMismatchError(message.messageId);
      }
      // Mesmo numa redelivery seguimos pelo caminho normal: o insert-first da transação devolve
      // o resultado original (replay) sem efeito. Assim o ack sempre carrega o resultado real.
      const outcome = await this.process(scope, command, money, payloadHash);
      if (!existing) {
        inbox.markProcessed(this.clock.now(), outcome.transactionId);
        await scope.inbox.save(inbox);
      }
      return { ...outcome, inboxDuplicate: existing !== undefined };
    });
  }

  private async process(
    scope: TransactionScope,
    command: SubmitWagerTransactionCommand,
    money: Money,
    payloadHash: string,
  ): Promise<WagerTransactionOutcome> {
    // 1) Existência sem lock. Wallets nunca são apagadas (trigger), então o resultado não fica obsoleto.
    //    Sem efeito financeiro: a redelivery apenas recalcula o mesmo resultado.
    const exists = await scope.wallets.findById(command.walletId);
    if (!exists) throw new UnknownWalletError(command.walletId);

    // 2) Insert-first: a unicidade no banco decide quem é o "dono" da operação.
    //    Duplicatas concorrentes ESPERAM no índice único até a original confirmar.
    const tx = this.buildTransaction(command, money, payloadHash);
    const inserted = await scope.transactions.insertIfAbsent(tx);
    if (!inserted) return this.replay(scope, command, payloadHash);

    // 3) Lock por wallet (nunca global) só para quem vai de fato mover saldo.
    const wallet = await scope.wallets.lockById(command.walletId);
    if (!wallet) throw new Error(`wallet ${command.walletId} vanished under lock`); // impossível: sem DELETE

    await this.settler.settle(scope, tx, wallet, { correlationId: command.correlationId, causationId: command.causationId });
    return toOutcome(tx, false);
  }

  private prepare(command: SubmitWagerTransactionCommand) {
    const money = this.parseMoney(command.money);
    // Hash sobre o Money validado: com escala fixa, cada valor tem uma única grafia no contrato.
    const payloadHash = hashWagerPayload({ ...command, money: money.toJSON() });
    return { money, payloadHash };
  }

  private async replay(
    scope: TransactionScope,
    command: SubmitWagerTransactionCommand,
    payloadHash: string,
  ): Promise<WagerTransactionOutcome> {
    // A key tem escopo por provedor: a mesma key de outro provedor é outra operação.
    const byKey = await scope.transactions.findByIdempotencyKey(command.providerId, command.idempotencyKey);
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

  buildTransaction(command: SubmitWagerTransactionCommand, money: Money, payloadHash: string): WagerTransaction {
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

  /**
   * Registra FAILED (PROCESSING_FAILED) numa transação SEPARADA, curta e best-effort, quando o
   * consumer desiste da mensagem (retries transitórios esgotados ou erro inesperado), ANTES de
   * movê-la para a DLQ. Nada financeiro é aplicado: a transação fica terminal e auditável, e o
   * replay da mesma key devolve FAILED.
   * Não trava a wallet (só a FK pega KEY SHARE), então funciona mesmo com a wallet disputada.
   * Devolve undefined se não havia o que gravar (wallet inexistente, payload inválido, key já usada).
   */
  async recordFailure(command: SubmitWagerTransactionCommand, message?: InboundMessage): Promise<string | undefined> {
    let prepared: { money: Money; payloadHash: string };
    try {
      prepared = this.prepare(command);
    } catch {
      return undefined;
    }
    return this.uow.run(async (scope) => {
      if (!(await scope.wallets.findById(command.walletId))) return undefined;
      let tx: WagerTransaction;
      try {
        tx = this.buildTransaction(command, prepared.money, prepared.payloadHash);
      } catch {
        return undefined;
      }
      tx.fail(FailureCode.ProcessingFailed, this.clock.now());
      const inserted = await scope.transactions.insertIfAbsent(tx);
      if (!inserted) return undefined; // já existe (ex.: PENDING_REFERENCE ou terminal): não sobrescreve
      if (message) {
        const inbox = InboxMessage.receive({ ...message, receivedAt: this.clock.now() });
        inbox.markProcessed(this.clock.now(), tx.id);
        await scope.inbox.insertIfAbsent(inbox);
      }
      return tx.id;
    });
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
