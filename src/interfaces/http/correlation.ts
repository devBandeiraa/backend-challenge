import type { NextFunction, Request, Response } from 'express';
import { runWithContext } from '../../infrastructure/observability/context';
import type { Logger } from '../../infrastructure/observability/logger';

const HEADER = 'x-correlation-id';
const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

type WithCorrelation = Request & { correlationId?: string };

/**
 * Propaga X-Correlation-Id (ou X-Request-Id do nginx) ou gera um novo, e abre o escopo de log
 * da requisição. Valor externo só é aceito se tiver formato seguro: ele vai para logs, eventos e
 * colunas varchar(128).
 */
export function correlationMiddleware(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.header(HEADER) ?? req.header('x-request-id');
  const correlationId = incoming && SAFE_ID.test(incoming) ? incoming : Bun.randomUUIDv7();
  (req as WithCorrelation).correlationId = correlationId;
  res.setHeader('X-Correlation-Id', correlationId);
  runWithContext({ correlationId }, () => next());
}

export function correlationIdOf(req: Request): string {
  return (req as WithCorrelation).correlationId ?? 'unknown';
}

/**
 * Uma linha por requisição ao terminar: método, rota, status, duração e o contexto (ids).
 * Sem corpo, sem query string: nada de payload financeiro.
 */
export function requestLogMiddleware(logger: Logger) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const startedAt = performance.now();
    res.on('finish', () => {
      if (req.path.startsWith('/health') || req.path === '/metrics') return; // ruído de probes/scrape
      const durationMs = Math.round((performance.now() - startedAt) * 10) / 10;
      const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';
      logger[level]({ msg: 'http request', method: req.method, path: req.path, status: res.statusCode, durationMs });
    });
    next();
  };
}
