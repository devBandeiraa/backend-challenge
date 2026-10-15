import { InvalidEntityError } from '../shared/domain-error';

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
}

export interface InboxMessageState extends ReceiveInboxProps {
  transactionId: string | undefined;
  processedAt: Date | undefined;
}

/** Deduplicação de transporte por (consumerName, messageId), na mesma transação do efeito. */
export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _transactionId: string | undefined,
    private _processedAt: Date | undefined,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    if (!props.messageId || props.messageId.length > 128) throw new InvalidEntityError('invalid messageId');
    if (!props.consumerName || props.consumerName.length > 64) throw new InvalidEntityError('invalid consumerName');
    return new InboxMessage(props.messageId, props.consumerName, props.payloadHash, props.receivedAt, undefined, undefined);
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.transactionId,
      state.processedAt,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get transactionId(): string | undefined {
    return this._transactionId;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  /** Mesma mensagem (messageId) com conteúdo diferente é erro permanente do produtor. */
  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  markProcessed(at: Date, transactionId?: string): void {
    if (this.isProcessed()) return;
    this._processedAt = at;
    this._transactionId = transactionId;
  }
}
