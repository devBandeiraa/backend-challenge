export interface BackoffOptions {
  baseMs: number;
  factor: number;
  maxMs: number;
  /** Fração do atraso que é aleatória (0 = determinístico). Evita que N instâncias acordem juntas. */
  jitterRatio: number;
}

/**
 * Backoff exponencial com teto e "equal jitter": atraso ∈ [d·(1−j), d], d = min(max, base·factor^(n−1)).
 * O piso garante que o retry nunca fique imediato mesmo com jitter.
 */
export class ExponentialBackoff {
  constructor(private readonly options: BackoffOptions) {
    if (options.baseMs <= 0 || options.factor < 1 || options.maxMs < options.baseMs) {
      throw new Error('invalid backoff options');
    }
    if (options.jitterRatio < 0 || options.jitterRatio > 1) throw new Error('jitterRatio must be in [0,1]');
  }

  /** attempt começa em 1. random é injetável para testes determinísticos. */
  delayMs(attempt: number, random: () => number = Math.random): number {
    const n = Math.max(1, Math.floor(attempt));
    const ceiling = Math.min(this.options.maxMs, this.options.baseMs * this.options.factor ** (n - 1));
    const jitter = ceiling * this.options.jitterRatio * random();
    return Math.round(ceiling - jitter);
  }

  nextAttemptAt(now: Date, attempt: number, random?: () => number): Date {
    return new Date(now.getTime() + this.delayMs(attempt, random));
  }
}

/** Outbox: nunca desiste (decisão G), teto de 5 min. */
export const OUTBOX_BACKOFF = new ExponentialBackoff({ baseMs: 1_000, factor: 2, maxMs: 300_000, jitterRatio: 0.2 });

/**
 * PENDING_REFERENCE: 10 tentativas, base 500 ms, ×2, teto 30 s ⇒ ~3 min de janela.
 * Fora de ordem em provedores é questão de segundos; minutos cobrem atraso de fila sem
 * prender a transação indefinidamente.
 */
export const PENDING_REFERENCE_BACKOFF = new ExponentialBackoff({ baseMs: 500, factor: 2, maxMs: 30_000, jitterRatio: 0.2 });
export const PENDING_REFERENCE_MAX_ATTEMPTS = 10;
