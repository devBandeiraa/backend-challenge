import { Controller, Get, HttpCode, ServiceUnavailableException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { Public } from './auth.guard';

@Public()
@Controller('health')
export class HealthController {
  constructor(private readonly orm: MikroORM) {}

  /** Liveness: só o processo. Não consulta dependências, senão uma queda do banco reiniciaria todas as réplicas. */
  @Get('live')
  @HttpCode(200)
  live() {
    return { status: 'ok' };
  }

  /** Readiness: tira a instância do balanceamento se o Postgres não responde. (SQS entra na Fase 2.) */
  @Get('ready')
  async ready() {
    try {
      await this.orm.em.fork().execute('SELECT 1');
      return { status: 'ok', checks: { postgres: 'up' } };
    } catch {
      throw new ServiceUnavailableException({ status: 'unavailable', checks: { postgres: 'down' } });
    }
  }
}
