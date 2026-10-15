import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';

export interface QueueNames {
  input: string;
  deadLetter: string;
  events: string;
}

export interface ReceivedMessage {
  sqsMessageId: string;
  receiptHandle: string;
  body: string;
  groupId: string | undefined;
  /** 1 na primeira entrega; cresce a cada redelivery (inclusive após crash sem ack). */
  receiveCount: number;
}

export interface OutgoingEvent {
  id: string; // eventId → MessageDeduplicationId
  groupId: string; // aggregateId → MessageGroupId
  eventType: string;
  body: string;
}

/** Adaptador fino sobre o SDK: só operações que a aplicação usa, com nomes do domínio de mensageria. */
export class SqsGateway {
  private constructor(
    private readonly client: SQSClient,
    readonly urls: { input: string; deadLetter: string; events: string },
  ) {}

  static async connect(client: SQSClient, names: QueueNames): Promise<SqsGateway> {
    const url = async (QueueName: string) => (await client.send(new GetQueueUrlCommand({ QueueName }))).QueueUrl!;
    return new SqsGateway(client, {
      input: await url(names.input),
      deadLetter: await url(names.deadLetter),
      events: await url(names.events),
    });
  }

  async receive(options: {
    maxMessages: number;
    waitTimeSeconds: number;
    visibilityTimeoutSeconds: number;
    abortSignal?: AbortSignal;
  }): Promise<ReceivedMessage[]> {
    const result = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.urls.input,
        MaxNumberOfMessages: Math.min(10, options.maxMessages),
        WaitTimeSeconds: options.waitTimeSeconds,
        VisibilityTimeout: options.visibilityTimeoutSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }),
      { abortSignal: options.abortSignal },
    );
    return (result.Messages ?? []).map(toReceived);
  }

  async ack(message: ReceivedMessage): Promise<void> {
    await this.client.send(new DeleteMessageCommand({ QueueUrl: this.urls.input, ReceiptHandle: message.receiptHandle }));
  }

  /** Devolve a mensagem à fila após `seconds` (0 = imediatamente, usado no shutdown). */
  async changeVisibility(message: ReceivedMessage, seconds: number): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.urls.input,
        ReceiptHandle: message.receiptHandle,
        VisibilityTimeout: Math.max(0, Math.min(43_200, Math.floor(seconds))),
      }),
    );
  }

  /**
   * Move explicitamente para a DLQ: envia (mesmo corpo, mesmo grupo, dedup pelo id SQS original,
   * motivo em atributos) e só então apaga da origem. Crash entre os dois passos → redelivery →
   * novo envio deduplicado pela FIFO (mesmo MessageDeduplicationId dentro de 5 min).
   */
  async moveToDeadLetter(message: ReceivedMessage, reason: string, detail?: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.urls.deadLetter,
        MessageBody: message.body,
        MessageGroupId: message.groupId ?? 'dead-letter',
        MessageDeduplicationId: message.sqsMessageId,
        MessageAttributes: {
          failureReason: { DataType: 'String', StringValue: reason },
          originalMessageId: { DataType: 'String', StringValue: message.sqsMessageId },
          receiveCount: { DataType: 'Number', StringValue: String(message.receiveCount) },
          ...(detail ? { failureDetail: { DataType: 'String', StringValue: detail.slice(0, 256) } } : {}),
        },
      }),
    );
    await this.ack(message);
  }

  /** Publica até 10 eventos por chamada; devolve quais ids falharam (o resto foi aceito). */
  async publishEvents(events: OutgoingEvent[]): Promise<Map<string, string>> {
    const failed = new Map<string, string>();
    for (let i = 0; i < events.length; i += 10) {
      const chunk = events.slice(i, i + 10);
      const result = await this.client.send(
        new SendMessageBatchCommand({
          QueueUrl: this.urls.events,
          Entries: chunk.map((e) => ({
            Id: e.id.replace(/[^A-Za-z0-9_-]/g, '_'),
            MessageBody: e.body,
            MessageGroupId: e.groupId,
            MessageDeduplicationId: e.id,
            MessageAttributes: { eventType: { DataType: 'String', StringValue: e.eventType } },
          })),
        }),
      );
      for (const f of result.Failed ?? []) {
        const original = chunk.find((e) => e.id.replace(/[^A-Za-z0-9_-]/g, '_') === f.Id);
        if (original) failed.set(original.id, `${f.Code}: ${f.Message ?? ''}`);
      }
    }
    return failed;
  }

  /** Readiness: a fila de entrada responde (com timeout curto). */
  async ping(timeoutMs = 1_000): Promise<void> {
    await this.client.send(new GetQueueAttributesCommand({ QueueUrl: this.urls.input, AttributeNames: ['QueueArn'] }), {
      abortSignal: AbortSignal.timeout(timeoutMs),
    });
  }

  /** Produtor (usado por testes e ferramentas). */
  async sendToInput(body: string, groupId: string, deduplicationId: string): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.urls.input,
        MessageBody: body,
        MessageGroupId: groupId,
        MessageDeduplicationId: deduplicationId,
      }),
    );
  }
}

function toReceived(m: Message): ReceivedMessage {
  return {
    sqsMessageId: m.MessageId!,
    receiptHandle: m.ReceiptHandle!,
    body: m.Body ?? '',
    groupId: m.Attributes?.MessageGroupId,
    receiveCount: Number(m.Attributes?.ApproximateReceiveCount ?? '1'),
  };
}

export const SQS_GATEWAY = Symbol('SqsGateway');
