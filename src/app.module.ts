import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { buildOrmConfig } from './infrastructure/persistence/mikro-orm.config';
import { HealthController } from './interfaces/http/health.controller';

@Module({
  imports: [MikroOrmModule.forRoot(buildOrmConfig())],
  controllers: [HealthController],
})
export class AppModule {}
