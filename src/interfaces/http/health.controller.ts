import { Controller, Get, HttpCode, ServiceUnavailableException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';

@Controller('health')
export class HealthController {
  // Injeção por tipo: valida que o Bun emite design:paramtypes (emitDecoratorMetadata).
  constructor(private readonly orm: MikroORM) {}

  @Get('live')
  @HttpCode(200)
  live() {
    return { status: 'ok' };
  }

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
