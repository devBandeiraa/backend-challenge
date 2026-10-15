import Decimal from 'decimal.js';
import { isAcceptedCurrency } from './currency';
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

// Até 17 dígitos inteiros (NUMERIC(19,2)), sem zeros à esquerda, EXATAMENTE 2 casas.
// Rejeita por construção: "25", "25.5", sinal, notação científica, NaN/Infinity, vazio, espaços, ".50".
const AMOUNT_PATTERN = /^(0|[1-9]\d{0,16})\.\d{2}$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;
const MAX_ABS = new D('99999999999999999.99');

/**
 * Valor monetário imutável com escala fixa de 2 casas.
 *
 * Contrato de escala fixa (README): amount tem EXATAMENTE 2 casas ("25.00"); "25" e "25.5"
 * são rejeitados. Mais de 2 casas também, nunca arredondado: arredondar dinheiro em silêncio
 * esconde erro do provedor. Como há uma única grafia válida por valor, o payloadHash não
 * depende de normalização.
 * Valores negativos só surgem por aritmética (negate/subtract), nunca pela entrada.
 */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  /** Entrada (contratos HTTP/SQS, criação de wallet): moeda precisa estar na lista ISO-4217 aceita. */
  static from(props: MoneyProps): Money {
    return new Money(Money.parseAmount(props), Money.assertAcceptedCurrency(props.currency));
  }

  /**
   * Reidratação do que já foi persistido: mesmo formato exato de valor, mas a moeda só é conferida
   * no formato. Um dado gravado continua legível mesmo que a lista de moedas aceitas mude depois.
   */
  static rehydrate(props: MoneyProps): Money {
    const amount = Money.parseAmount(props);
    if (typeof props.currency !== 'string' || !CURRENCY_PATTERN.test(props.currency)) {
      throw new InvalidMoneyError(`invalid persisted currency ${JSON.stringify(props.currency)}`);
    }
    return new Money(amount, props.currency);
  }

  static zero(currency: string): Money {
    return new Money(new D(0), Money.assertAcceptedCurrency(currency));
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

  /** A soma cabe na faixa persistível (NUMERIC(19,2))? Permite à política rejeitar antes de `add` lançar. */
  canAdd(other: Money): boolean {
    this.assertSameCurrency(other);
    return !this.value.plus(other.value).abs().greaterThan(MAX_ABS);
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

  private static parseAmount(props: MoneyProps): Decimal {
    if (props === null || typeof props !== 'object') throw new InvalidMoneyError('money must be an object');
    const { amount } = props;
    if (typeof amount !== 'string' || !AMOUNT_PATTERN.test(amount)) {
      throw new InvalidMoneyError(
        `invalid amount ${JSON.stringify(amount)}: expected non-negative decimal string with exactly ${MONEY_SCALE} places`,
      );
    }
    return new D(amount);
  }

  private static assertAcceptedCurrency(currency: unknown): string {
    if (!isAcceptedCurrency(currency)) {
      throw new InvalidMoneyError(
        `invalid currency ${JSON.stringify(currency)}: expected an ISO-4217 code with 2 minor-unit digits (see currency.ts)`,
      );
    }
    return currency;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(`cannot combine ${this.currency} with ${other.currency}`);
    }
  }
}
