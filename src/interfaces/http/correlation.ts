import type { NextFunction, Request, Response } from 'express';

const HEADER = 'x-correlation-id';
const SAFE_ID = /^[A-Za-z0-9._:-]{1,128}$/;

type WithCorrelation = Request & { correlationId?: string };

/**
 * Propaga X-Correlation-Id (ou X-Request-Id do nginx) ou gera um novo. Valor externo só é
 * aceito se tiver formato seguro: ele vai para logs, eventos e colunas varchar(128).
 */
export function correlationMiddleware(req: Request, res: Response, next: NextFunction): void {
  const incoming = req.header(HEADER) ?? req.header('x-request-id');
  const correlationId = incoming && SAFE_ID.test(incoming) ? incoming : Bun.randomUUIDv7();
  (req as WithCorrelation).correlationId = correlationId;
  res.setHeader('X-Correlation-Id', correlationId);
  next();
}

export function correlationIdOf(req: Request): string {
  return (req as WithCorrelation).correlationId ?? 'unknown';
}
