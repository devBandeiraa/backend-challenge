import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './domain-error';

/** DTO de contrato: amount é sempre string decimal, nunca number. */
export interface MoneyProps {
  amount: string;
  currency: string;
}

export const MONEY_SCALE = 2;

// Construtor isolado: não altera a configuração global do decimal.js usada por outras libs.
// Precisão 40 >> 19 dígitos de NUMERIC(19,2), então add/subtract são sempre exatos.
const D = Decimal.clone({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN, toExpNeg: -40, toExpPos: 40 });

// Até 17 dígitos inteiros (NUMERIC(19,2)), sem zeros à esquerda, 1 ou 2 casas opcionais.
// Rejeita por construção: sinal, notação científica, NaN/Infinity, vazio, espaços, ".5", "5.".
const AMOUNT_PATTERN = /^(0|[1-9]\d{0,16})(\.\d{1,2})?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;
const MAX_ABS = new D('99999999999999999.99');

/**
 * Valor monetário imutável com escala fixa de 2 casas.
 *
 * Entrada aceita 0–2 casas ("25", "25.5", "25.50") e normaliza para 2; mais de 2 casas é
 * REJEITADO, nunca arredondado: arredondar dinheiro em silêncio esconde erro do provedor.
 * Valores negativos só surgem por aritmética (negate/subtract), nunca pela entrada.
 */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  static from(props: MoneyProps): Money {
    if (props === null || typeof props !== 'object') throw new InvalidMoneyError('money must be an object');
    const { amount, currency } = props;
    if (typeof amount !== 'string' || !AMOUNT_PATTERN.test(amount)) {
      throw new InvalidMoneyError(
        `invalid amount ${JSON.stringify(amount)}: expected non-negative decimal string with up to ${MONEY_SCALE} places`,
      );
    }
    return new Money(new D(amount), Money.assertCurrency(currency));
  }

  static zero(currency: string): Money {
    return new Money(new D(0), Money.assertCurrency(currency));
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.of(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.of(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return Money.of(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  /** Sempre 2 casas; -0 normalizado para "0.00". */
  get amount(): string {
    return (this.value.isZero() ? new D(0) : this.value).toFixed(MONEY_SCALE);
  }

  toJSON(): MoneyProps {
    return { amount: this.amount, currency: this.currency };
  }

  toString(): string {
    return `${this.amount} ${this.currency}`;
  }

  private static of(value: Decimal, currency: string): Money {
    if (value.decimalPlaces() > MONEY_SCALE) throw new InvalidMoneyError('scale overflow'); // inalcançável com add/sub
    if (value.abs().greaterThan(MAX_ABS)) throw new InvalidMoneyError('amount exceeds NUMERIC(19,2) range');
    return new Money(value, currency);
  }

  private static assertCurrency(currency: unknown): string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`invalid currency ${JSON.stringify(currency)}: expected ISO-4217 alpha code`);
    }
    return currency;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(`cannot combine ${this.currency} with ${other.currency}`);
    }
  }
}
