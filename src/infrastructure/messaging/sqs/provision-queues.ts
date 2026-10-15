import { CreateQueueCommand, GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient, DEFAULT_QUEUE_NAMES, type SqsQueueNames } from './sqs-config';

export interface ProvisionedQueues {
  inputUrl: string;
  deadLetterUrl: string;
  eventsUrl: string;
}

/**
 * Cria as filas de forma idempotente (CreateQueue com os mesmos atributos é no-op).
 * É código da aplicação, e não init script do emulador, para funcionar igual em
 * LocalStack e MiniStack e servir de fonte única para compose e testes.
 */
export async function provisionQueues(
  client: SQSClient,
  names: SqsQueueNames = DEFAULT_QUEUE_NAMES,
  maxReceiveCount = Number(process.env.SQS_MAX_RECEIVE_COUNT ?? 5),
): Promise<ProvisionedQueues> {
  const fifo = { FifoQueue: 'true', ContentBasedDeduplication: 'false' };

  const dlq = await client.send(
    new CreateQueueCommand({ QueueName: names.deadLetter, Attributes: { ...fifo, MessageRetentionPeriod: '1209600' } }),
  );
  const dlqArn = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }),
  );

  // Redrive é a rede de segurança (crash loop / poison message). O consumer move para a DLQ
  // explicitamente antes disso nos erros permanentes e quando esgota retries transitórios.
  const input = await client.send(
    new CreateQueueCommand({
      QueueName: names.input,
      Attributes: {
        ...fifo,
        VisibilityTimeout: '30',
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: dlqArn.Attributes?.QueueArn,
          maxReceiveCount: String(maxReceiveCount),
        }),
      },
    }),
  );

  const events = await client.send(new CreateQueueCommand({ QueueName: names.events, Attributes: fifo }));

  return { inputUrl: input.QueueUrl!, deadLetterUrl: dlq.QueueUrl!, eventsUrl: events.QueueUrl! };
}

if (import.meta.main) {
  const urls = await provisionQueues(createSqsClient());
  console.log(JSON.stringify({ msg: 'queues provisioned', ...urls }));
}
