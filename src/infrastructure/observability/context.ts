import { AsyncLocalStorage } from 'node:async_hooks';

/** Campos de rastreio que acompanham automaticamente toda linha de log da mesma requisição/mensagem. */
export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const storage = new AsyncLocalStorage<LogContext>();

/** Abre um escopo (requisição HTTP, mensagem SQS, item do worker) herdando o escopo atual. */
export function runWithContext<T>(context: LogContext, fn: () => T): T {
  return storage.run({ ...storage.getStore(), ...context }, fn);
}

/** Acrescenta campos ao escopo corrente à medida que ficam conhecidos (ex.: transactionId). */
export function enrichContext(fields: LogContext): void {
  const store = storage.getStore();
  if (!store) return;
  for (const [k, v] of Object.entries(fields)) if (v !== undefined) (store as Record<string, unknown>)[k] = v;
}

export function currentContext(): LogContext {
  return storage.getStore() ?? {};
}
