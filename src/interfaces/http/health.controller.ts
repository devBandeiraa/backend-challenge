import { Controller, Get, HttpCode, Inject, ServiceUnavailableException } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import { SQS_GATEWAY, type SqsGateway } from '../../infrastructure/messaging/sqs/sqs-gateway';
import { Public } from './auth.guard';

@Public()
@Controller('health')
export class HealthController {
  constructor(
    private readonly orm: MikroORM,
    @Inject(SQS_GATEWAY) private readonly sqs: SqsGateway,
  ) {}

  /** Liveness: só o processo. Não consulta dependências, senão uma queda do banco reiniciaria todas as réplicas. */
  @Get('live')
  @HttpCode(200)
  live() {
    return { status: 'ok' };
  }

  /** Readiness: PostgreSQL e SQS alcançáveis (checagens em paralelo, timeout curto cada). */
  @Get('ready')
  async ready() {
    const [postgres, sqs] = await Promise.all([
      this.orm.em
        .fork()
        .execute('SELECT 1')
        .then(() => 'up' as const)
        .catch(() => 'down' as const),
      this.sqs
        .ping(1_000)
        .then(() => 'up' as const)
        .catch(() => 'down' as const),
    ]);
    const checks = { postgres, sqs };
    if (postgres === 'down' || sqs === 'down') throw new ServiceUnavailableException({ status: 'unavailable', checks });
    return { status: 'ok', checks };
  }
}
