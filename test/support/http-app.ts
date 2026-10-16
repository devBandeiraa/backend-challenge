import 'reflect-metadata';
import type { INestApplication } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { AppModule, configureHttp, LOGGER } from '../../src/app.module';
import { UNIT_OF_WORK, type UnitOfWork } from '../../src/application/ports';
import { createLogger } from '../../src/infrastructure/observability/logger';
import { ensureMigrated } from './db';

export interface StartOptions {
  /** Sobrescreve a configuração SÓ durante a criação (loadConfig é lido no bootstrap). */
  env?: Record<string, string>;
  /** Captura as linhas JSON de log da app (para testes de observabilidade). */
  logLines?: string[];
  /** Injeção de falha: recebe a unit of work REAL da app (a mesma instância usada pelos use cases). */
  patchUnitOfWork?: (uow: UnitOfWork) => void;
}

/**
 * Sobe a app Nest real (mesmo AppModule e mesma configuração HTTP do main.ts) numa porta efêmera.
 * Por padrão sobe só o papel HTTP (consumer/workers têm testes próprios) e com logs silenciados.
 */
export async function startHttpApp(
  options: StartOptions | Record<string, string> = {},
): Promise<{ app: INestApplication; baseUrl: string; close: () => Promise<void> }> {
  const opts: StartOptions =
    'env' in options || 'logLines' in options || 'patchUnitOfWork' in options ? (options as StartOptions) : { env: options as Record<string, string> };
  await ensureMigrated();
  const overrides = { APP_ROLES: 'http', LOG_LEVEL: 'silent', ...opts.env };
  const previous = Object.fromEntries(Object.keys(overrides).map((k) => [k, process.env[k]]));
  Object.assign(process.env, overrides);
  let app: INestApplication;
  try {
    const builder = Test.createTestingModule({ imports: [AppModule] });
    if (opts.logLines) {
      const lines = opts.logLines;
      const logger = createLogger({ test: true }, { write: (line: string) => void lines.push(line) });
      logger.level = 'info';
      builder.overrideProvider(LOGGER).useValue(logger);
    }
    const moduleRef = await builder.compile();
    opts.patchUnitOfWork?.(moduleRef.get<UnitOfWork>(UNIT_OF_WORK));
    app = moduleRef.createNestApplication({ logger: ['error'] });
    configureHttp(app);
    await app.listen(0);
  } finally {
    for (const [k, v] of Object.entries(previous)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
  const address = app.getHttpServer().address() as { port: number };
  return { app, baseUrl: `http://127.0.0.1:${address.port}`, close: () => app.close() };
}

export interface HttpResult<T = any> {
  status: number;
  body: T;
  headers: Headers;
}

export async function http<T = any>(
  baseUrl: string,
  method: string,
  path: string,
  options: { body?: unknown; rawBody?: string; headers?: Record<string, string> } = {},
): Promise<HttpResult<T>> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...options.headers },
    body: options.rawBody ?? (options.body !== undefined ? JSON.stringify(options.body) : undefined),
  });
  const text = await response.text();
  let body: unknown = text;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    // corpo não-JSON (ex.: /metrics)
  }
  return { status: response.status, body: body as T, headers: response.headers };
}

export function parseLogLines(lines: string[]): Record<string, any>[] {
  return lines.flatMap((l) => l.split('\n').filter(Boolean)).map((l) => JSON.parse(l));
}
