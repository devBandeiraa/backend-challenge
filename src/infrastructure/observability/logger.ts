import pino from 'pino';

/**
 * Logs JSON estruturados. Campos de rastreio (correlationId, messageId, transactionId, walletId,
 * providerId) vão como propriedades; payload financeiro completo NUNCA é logado.
 */
export type Logger = pino.Logger;

export function createLogger(bindings: Record<string, unknown> = {}): Logger {
  return pino({
    level: process.env.LOG_LEVEL ?? 'info',
    base: { service: 'wagering-processor', instanceId: process.env.INSTANCE_ID ?? process.env.HOSTNAME, ...bindings },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: { level: (label) => ({ level: label }) },
    // Defesa em profundidade: mesmo que alguém logue o objeto inteiro, estes campos saem mascarados.
    redact: { paths: ['money', 'data.money', '*.money', 'balance', '*.balance', 'body', 'payload'], censor: '[redacted]' },
  });
}
