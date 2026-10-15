import { CreateQueueCommand, GetQueueAttributesCommand, SetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient, DEFAULT_QUEUE_NAMES, type SqsQueueNames } from './sqs-config';

export interface ProvisionedQueues {
  inputUrl: string;
  deadLetterUrl: string;
  eventsUrl: string;
}

/**
 * Provisionamento CONVERGENTE e idempotente: CreateQueue só com os atributos imutáveis da FIFO
 * (CreateQueue com atributos diferentes de uma fila existente falha), depois SetQueueAttributes
 * com os mutáveis. Rodar de novo após mudar a configuração ajusta as filas existentes.
 * É código da aplicação, e não init script do emulador, para funcionar igual em LocalStack e
 * MiniStack e servir de fonte única para compose e testes.
 */
export async function provisionQueues(
  client: SQSClient,
  names: SqsQueueNames = DEFAULT_QUEUE_NAMES,
  maxReceiveCount = Number(process.env.SQS_MAX_RECEIVE_COUNT ?? 5),
  visibilityTimeoutSeconds = Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS ?? 90),
): Promise<ProvisionedQueues> {
  const fifo = { FifoQueue: 'true', ContentBasedDeduplication: 'false' };
  const create = async (QueueName: string) => (await client.send(new CreateQueueCommand({ QueueName, Attributes: fifo }))).QueueUrl!;
  const set = (QueueUrl: string, Attributes: Record<string, string>) =>
    client.send(new SetQueueAttributesCommand({ QueueUrl, Attributes }));

  const deadLetterUrl = await create(names.deadLetter);
  await set(deadLetterUrl, { MessageRetentionPeriod: '1209600' });
  const dlqArn = await client.send(new GetQueueAttributesCommand({ QueueUrl: deadLetterUrl, AttributeNames: ['QueueArn'] }));

  // Redrive é a rede de segurança (crash loop / poison message). O consumer move para a DLQ
  // explicitamente antes disso (no 5º recebimento); o redrive só agiria no 6º.
  const inputUrl = await create(names.input);
  await set(inputUrl, {
    VisibilityTimeout: String(visibilityTimeoutSeconds),
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn.Attributes?.QueueArn, maxReceiveCount: String(maxReceiveCount) }),
  });

  const eventsUrl = await create(names.events);
  return { inputUrl, deadLetterUrl, eventsUrl };
}

if (import.meta.main) {
  const urls = await provisionQueues(createSqsClient());
  console.log(JSON.stringify({ msg: 'queues provisioned', ...urls }));
}
