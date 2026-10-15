import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureHttp } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  configureHttp(app);
  await app.listen(Number(process.env.PORT ?? 3000));
}

await bootstrap();
