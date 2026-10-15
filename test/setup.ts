import { setDefaultTimeout } from 'bun:test';

// Integração/concorrência com Postgres e SQS reais: long polling e backoff em segundos.
setDefaultTimeout(60_000);
