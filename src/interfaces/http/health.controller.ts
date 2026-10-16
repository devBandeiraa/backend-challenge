import { Controller, Get, HttpCode, Inject, Res } from '@nestjs/common';
import { MikroORM } from '@mikro-orm/postgresql';
import type { Response } from 'express';
import { SQS_GATEWAY, type SqsGateway } from '../../infrastructure/messaging/sqs/sqs-gateway';
import { Public } from './auth.guard';

const READY_CHECK_TIMEOUT_MS = 1_000;

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`readiness check timed out after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

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

  /**
   * Readiness: PostgreSQL e SQS alcançáveis (checagens em paralelo, timeout curto cada).
   * Em 503 o corpo diz QUAL dependência caiu: o operador (e o balanceador) não precisam adivinhar.
   * Responde direto, sem passar pelo filtro de erro (que só serializaria a mensagem da exceção).
   */
  @Get('ready')
  async ready(@Res({ passthrough: true }) res: Response) {
    const [postgres, sqs] = await Promise.all([
      // Timeout curto: com o banco fora (ou o pool esgotado), readiness responde 503 em vez de travar.
      withTimeout(this.orm.em.fork().execute('SELECT 1'), READY_CHECK_TIMEOUT_MS)
        .then(() => 'up' as const)
        .catch(() => 'down' as const),
      this.sqs
        .ping(READY_CHECK_TIMEOUT_MS)
        .then(() => 'up' as const)
        .catch(() => 'down' as const),
    ]);
    const checks = { postgres, sqs };
    if (postgres === 'down' || sqs === 'down') {
      res.status(503).setHeader('Retry-After', '1');
      return { status: 'unavailable', checks };
    }
    return { status: 'ok', checks };
  }
}
