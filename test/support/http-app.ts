import 'reflect-metadata';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureHttp } from '../../src/app.module';
import { ensureMigrated } from './db';

/**
 * Sobe a app Nest real (mesma configuração do main.ts) numa porta efêmera.
 * `env` sobrescreve a configuração SÓ durante a criação (loadConfig é lido no bootstrap)
 * e é restaurado em seguida, para não vazar para outros arquivos de teste.
 * Por padrão sobe apenas o papel HTTP: consumer/workers têm testes próprios.
 */
export async function startHttpApp(
  env: Record<string, string> = {},
): Promise<{ app: INestApplication; baseUrl: string; close: () => Promise<void> }> {
  await ensureMigrated();
  const overrides = { APP_ROLES: 'http', ...env };
  const previous = Object.fromEntries(Object.keys(overrides).map((k) => [k, process.env[k]]));
  Object.assign(process.env, overrides);
  let app: INestApplication;
  try {
    app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
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
