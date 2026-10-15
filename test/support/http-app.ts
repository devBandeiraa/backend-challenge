import 'reflect-metadata';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureHttp } from '../../src/app.module';
import { ensureMigrated } from './db';

/** Sobe a app Nest real (mesma configuração do main.ts) numa porta efêmera. */
export async function startHttpApp(): Promise<{ app: INestApplication; baseUrl: string; close: () => Promise<void> }> {
  await ensureMigrated();
  const app = await NestFactory.create(AppModule, { logger: ['error', 'warn'] });
  configureHttp(app);
  await app.listen(0);
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
  return { status: response.status, body: (text ? JSON.parse(text) : undefined) as T, headers: response.headers };
}
