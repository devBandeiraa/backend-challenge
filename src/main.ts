import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureHttp, NEST_LOGGER } from './app.module';

async function bootstrap() {
  // bufferLogs: logs do bootstrap ficam retidos até o logger JSON (pino) assumir.
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(NEST_LOGGER));
  configureHttp(app);
  await app.listen(Number(process.env.PORT ?? 3000));
}

await bootstrap();
