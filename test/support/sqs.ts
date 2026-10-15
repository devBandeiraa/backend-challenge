import { DeleteMessageCommand, PurgeQueueCommand, ReceiveMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs/sqs-config';
import { SqsGateway } from '../../src/infrastructure/messaging/sqs/sqs-gateway';
import { provisionQueues } from '../../src/infrastructure/messaging/sqs/provision-queues';

/**
 * Filas REAIS (LocalStack) exclusivas por arquivo de teste: nada de compartilhar fila com outro
 * teste ou com réplicas do compose. maxReceiveCount do redrive configurável.
 */
export async function createTestQueues(label: string, maxReceiveCount = 10) {
  const client = createSqsClient(process.env.SQS_ENDPOINT ?? 'http://localhost:4566');
  const suffix = `${label}-${crypto.randomUUID().slice(0, 8)}`;
  const names = { input: `t-${suffix}-in.fifo`, deadLetter: `t-${suffix}-dlq.fifo`, events: `t-${suffix}-ev.fifo` };
  await provisionQueues(client, names, maxReceiveCount);
  const gateway = await SqsGateway.connect(client, names);
  return { client, names, gateway };
}

export interface RawMessage {
  body: string;
  attributes: Record<string, string>;
}

/** Drena (recebe e apaga) tudo que chegar numa fila até ficar `idleMs` sem mensagens novas. */
export async function drain(client: SQSClient, queueUrl: string, idleMs = 1_500): Promise<RawMessage[]> {
  const out: RawMessage[] = [];
  let idleSince = Date.now();
  while (Date.now() - idleSince < idleMs) {
    const res = await client.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'] }),
    );
    for (const m of res.Messages ?? []) {
      out.push({
        body: m.Body ?? '',
        attributes: Object.fromEntries(Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue ?? ''])),
      });
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
      idleSince = Date.now();
    }
  }
  return out;
}

export async function purge(client: SQSClient, queueUrl: string) {
  await client.send(new PurgeQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
}

/** Mensagem no formato do enunciado (seção 10). */
export function wagerMessage(data: Record<string, unknown>, messageId = `msg-${crypto.randomUUID()}`, extra: Record<string, unknown> = {}) {
  return {
    messageId,
    body: JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data, ...extra }),
  };
}

export async function waitFor<T>(probe: () => Promise<T | undefined>, timeoutMs = 15_000, intervalMs = 100): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined && value !== false) return value;
    if (Date.now() > deadline) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(intervalMs);
  }
}

export { SqsGateway };
