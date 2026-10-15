import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException } from '@nestjs/common';
import type { Request, Response } from 'express';
import { ApplicationError, type ErrorCategory, TransientInfrastructureError, UnknownWalletError } from '../../application/errors';
import type { Logger } from '../../infrastructure/observability/logger';
import { classifyPgError } from '../../infrastructure/persistence/pg-errors';
import { correlationIdOf } from './correlation';

/**
 * Mapeamento ÚNICO categoria → status para todos os endpoints. O provedor decide se reenvia
 * pelo status, sem interpretar mensagem:
 *   400 payload inválido        → corrigir; reenviar igual não adianta
 *   404 recurso inexistente      (consultas)
 *   409 conflito                 → idempotency key/externalId já usados com outro payload; wallet duplicada
 *   422 rejeição de negócio      → terminal para aquela key; corpo traz failureCode
 *   503 falha transitória        → reenviar com a MESMA Idempotency-Key (Retry-After)
 *   500 bug                      → não classificado
 * 200/202 vêm do resultado da transação (ver controller), não deste filtro.
 */
const STATUS_BY_CATEGORY: Record<ErrorCategory, number> = {
  validation: 400,
  not_found: 404,
  conflict: 409,
  business: 422,
  transient: 503,
};

@Catch()
export class HttpErrorFilter implements ExceptionFilter {
  constructor(private readonly logger: Logger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const response = http.getResponse<Response>();
    const correlationId = correlationIdOf(http.getRequest<Request>());

    // Wallet inexistente numa submissão é rejeição de negócio: MESMO formato de corpo do 422 de
    // transação e sem campos voláteis, para o replay devolver exatamente a mesma resposta.
    // (correlationId segue no header X-Correlation-Id.)
    if (exception instanceof UnknownWalletError) {
      response.status(422).json({
        transactionId: null,
        status: 'REJECTED',
        balance: null,
        failureCode: exception.code,
        idempotentReplay: false,
      });
      return;
    }

    const error = this.normalize(exception);
    if (error.status >= 500) {
      // correlationId/walletId/... vêm do contexto da requisição (mixin do logger).
      this.logger.error({ msg: 'request failed', code: error.code, err: exception });
    }
    if (error.status === 503) response.setHeader('Retry-After', '1');
    response.status(error.status).json({
      error: { code: error.code, message: error.message, ...(error.details !== undefined ? { details: error.details } : {}) },
      correlationId,
    });
  }

  private normalize(exception: unknown): { status: number; code: string; message: string; details?: unknown } {
    if (exception instanceof ApplicationError) {
      return {
        status: STATUS_BY_CATEGORY[exception.category],
        code: exception.code,
        message: exception.message,
        details: 'details' in exception ? (exception as { details?: unknown }).details : undefined,
      };
    }
    if (exception instanceof HttpException) {
      // JSON malformado, rota inexistente, etc. (gerados pelo próprio Nest/Express).
      const status = exception.getStatus();
      return { status, code: status === 400 ? 'INVALID_PAYLOAD' : `HTTP_${status}`, message: exception.message };
    }
    if (classifyPgError(exception) !== 'other') {
      const transient = new TransientInfrastructureError('temporarily unavailable');
      return { status: 503, code: transient.code, message: transient.message };
    }
    return { status: 500, code: 'INTERNAL_ERROR', message: 'internal error' };
  }
}
