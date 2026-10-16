/**
 * Roda DENTRO de uma réplica do compose (docker exec ... bun -e), não no host: envia mensagens ao SQS e mede,
 * pelo próprio banco, quando todas ficaram PROCESSED. Assim a medição não passa pelo proxy de portas do Docker
 * Desktop (que derruba conexões do host com ECONNRESET aos 30 s; ver docs/load-test-analysis.md).
 *
 * Protocolo por stdin/stdout (uma linha JSON cada):
 *   stdin  1: { queue, messages: [{ body, groupId }], keys: string[] }
 *   stdout   : { phase: 'ready' }
 *   stdin  2: go
 *   stdout   : { phase: 'done', allProcessedMs, processed }
 * Usa só o que a imagem da app já tem: @aws-sdk/client-sqs e o Bun.SQL, com SQS_ENDPOINT/DATABASE_URL da réplica.
 */
import { GetQueueUrlCommand, SendMessageBatchCommand, SQSClient } from '@aws-sdk/client-sqs';
import { SQL } from 'bun';

const lines = console[Symbol.asyncIterator]();
const input = JSON.parse((await lines.next()).value as string) as { queue: string; messages: { body: string; groupId: string }[]; keys: string[] };
const sqs = new SQSClient({ endpoint: process.env.SQS_ENDPOINT, region: process.env.AWS_REGION ?? 'us-east-1', useQueueUrlAsEndpoint: false });
const db = new SQL(process.env.DATABASE_URL!);
const queueUrl = (await sqs.send(new GetQueueUrlCommand({ QueueName: input.queue }))).QueueUrl!;
console.log(JSON.stringify({ phase: 'ready' }));

await lines.next(); // "go": o host acabou de mandar SIGTERM para a vítima
const startedAt = performance.now();
const batches: Promise<unknown>[] = [];
for (let i = 0; i < input.messages.length; i += 10) {
  const entries = input.messages.slice(i, i + 10).map((m, j) => ({
    Id: `m${i + j}`,
    MessageBody: m.body,
    MessageGroupId: m.groupId,
    MessageDeduplicationId: crypto.randomUUID(),
  }));
  batches.push(sqs.send(new SendMessageBatchCommand({ QueueUrl: queueUrl, Entries: entries })));
}
await Promise.all(batches);

const keys = input.keys.join(',');
let processed = 0;
while (performance.now() - startedAt < 60_000) {
  const [row] = await db`SELECT count(*)::int AS n FROM wager_transactions
                          WHERE idempotency_key = ANY(string_to_array(${keys}, ',')) AND status = 'PROCESSED'`;
  processed = row.n;
  if (processed === input.keys.length) break;
  await Bun.sleep(50);
}
console.log(JSON.stringify({ phase: 'done', allProcessedMs: Math.round(performance.now() - startedAt), processed }));
await db.close();
process.exit(0);
