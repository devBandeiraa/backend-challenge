import type { Clock, IdGenerator } from '../application/ports';

export class SystemClock implements Clock {
  now(): Date {
    return new Date();
  }
}

/** UUIDv7 nativo do Bun: ordenável por tempo (localidade de índice) e sem coordenação entre instâncias. */
export class UuidV7Generator implements IdGenerator {
  next(): string {
    return Bun.randomUUIDv7();
  }
}
