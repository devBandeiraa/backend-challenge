import type { LoggerService } from '@nestjs/common';
import pino from 'pino';
import { currentContext } from './context';

/**
 * Logs JSON estruturados (pino). Toda linha carrega, via AsyncLocalStorage, os campos de rastreio
 * do escopo corrente: correlationId, messageId, transactionId, walletId, providerId.
 * Payload financeiro completo NUNCA é logado: os call sites logam ids e status, e `redact`
 * mascara campos monetários caso alguém logue um objeto inteiro por engano.
 */
export type Logger = pino.Logger;

export const REDACTED_PATHS = [
  'money',
  '*.money',
  'balance',
  '*.balance',
  'amount',
  '*.amount',
  'storedBalance',
  'calculatedBalance',
  'body',
  'payload',
  'data',
];

export function createLogger(bindings: Record<string, unknown> = {}, destination?: pino.DestinationStream): Logger {
  return pino(
    {
      level: process.env.LOG_LEVEL ?? 'info',
      base: { service: 'wagering-processor', ...bindings },
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: { level: (label) => ({ level: label }) },
      mixin: () => ({ ...currentContext() }),
      redact: { paths: REDACTED_PATHS, censor: '[redacted]' },
    },
    destination,
  );
}

/** Redireciona os logs do próprio Nest (bootstrap, rotas, exceções) para o mesmo pino JSON. */
export class PinoNestLogger implements LoggerService {
  constructor(private readonly logger: Logger) {}
  log(message: unknown, context?: string) {
    this.logger.info({ context }, String(message));
  }
  error(message: unknown, trace?: string, context?: string) {
    this.logger.error({ context, trace }, String(message));
  }
  warn(message: unknown, context?: string) {
    this.logger.warn({ context }, String(message));
  }
  debug(message: unknown, context?: string) {
    this.logger.debug({ context }, String(message));
  }
  verbose(message: unknown, context?: string) {
    this.logger.trace({ context }, String(message));
  }
}
