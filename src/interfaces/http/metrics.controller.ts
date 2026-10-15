import { Controller, Get, Header, Inject } from '@nestjs/common';
import { type AppMetrics, METRICS } from '../../infrastructure/observability/metrics';
import { Public } from './auth.guard';

@Public()
@Controller('metrics')
export class MetricsController {
  constructor(@Inject(METRICS) private readonly metrics: AppMetrics) {}

  @Get()
  @Header('Content-Type', 'text/plain; version=0.0.4')
  scrape(): Promise<string> {
    return this.metrics.registry.metrics();
  }
}
