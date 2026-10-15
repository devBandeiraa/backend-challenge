import { SQSClient } from '@aws-sdk/client-sqs';

export interface SqsQueueNames {
  input: string;
  deadLetter: string;
  events: string;
}

export const DEFAULT_QUEUE_NAMES: SqsQueueNames = {
  input: process.env.SQS_INPUT_QUEUE ?? 'wager-transactions.fifo',
  deadLetter: process.env.SQS_DLQ ?? 'wager-transactions-dlq.fifo',
  events: process.env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
};

export function createSqsClient(endpoint = process.env.SQS_ENDPOINT ?? 'http://localhost:4566'): SQSClient {
  return new SQSClient({
    // LocalStack devolve QueueUrl com host localhost.localstack.cloud (inalcançável de dentro do
    // container). Usar sempre o endpoint configurado torna a URL só um identificador da fila.
    useQueueUrlAsEndpoint: false,
    endpoint,
    region: process.env.AWS_REGION ?? 'us-east-1',
    // Emuladores aceitam qualquer credencial; em AWS real vem da cadeia padrão (IAM role).
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}
