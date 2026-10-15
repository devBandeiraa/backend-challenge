import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../../src/domain/shared/domain-error';
import { Money } from '../../../src/domain/shared/money';
import { brl, usd } from '../../support/builders';

describe('Money: entrada', () => {
  test.each([
    ['25.00', '25.00'],
    ['25.5', '25.50'],
    ['25', '25.00'],
    ['0', '0.00'],
    ['0.01', '0.01'],
    ['99999999999999999.99', '99999999999999999.99'], // limite de NUMERIC(19,2)
  ])('aceita %p e normaliza para escala 2 (%p)', (input, expected) => {
    expect(Money.from({ amount: input, currency: 'BRL' }).amount).toBe(expected);
  });

  test.each([
    ['', 'vazio'],
    ['NaN', 'NaN'],
    ['Infinity', 'Infinity'],
    ['-Infinity', '-Infinity'],
    ['1e3', 'notação científica'],
    ['1E-2', 'notação científica'],
    ['10.005', 'mais de 2 casas (não arredonda)'],
    ['0.001', 'mais de 2 casas'],
    ['-1.00', 'negativo em contrato de entrada'],
    ['-0.00', 'zero negativo'],
    ['+1.00', 'sinal explícito'],
    ['01.00', 'zero à esquerda'],
    ['.50', 'sem parte inteira'],
    ['5.', 'ponto sem casas'],
    [' 1.00', 'espaço'],
    ['1,00', 'vírgula decimal'],
    ['0x10', 'hexadecimal'],
    ['100000000000000000.00', 'excede NUMERIC(19,2)'],
  ])('rejeita %p (%s)', (input) => {
    expect(() => Money.from({ amount: input, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each([[25], [25.5], [null], [undefined], [{}]])('rejeita amount não-string %p (nunca number)', (amount) => {
    expect(() => Money.from({ amount: amount as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each([['brl'], ['BR'], ['BRLL'], [''], ['12A']])('rejeita moeda %p', (currency) => {
    expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
  });
});

describe('Money: aritmética exata', () => {
  test('0.10 + 0.20 = 0.30 (sem erro de ponto flutuante)', () => {
    expect(brl('0.10').add(brl('0.20')).amount).toBe('0.30');
  });

  test('soma de 1000 × 0.01 é exatamente 10.00', () => {
    let total = Money.zero('BRL');
    for (let i = 0; i < 1000; i++) total = total.add(brl('0.01'));
    expect(total.amount).toBe('10.00');
  });

  test('valores grandes mantêm todos os dígitos', () => {
    expect(brl('99999999999999998.99').add(brl('1.00')).amount).toBe('99999999999999999.99');
  });

  test('overflow do intervalo do banco é rejeitado, não truncado', () => {
    expect(() => brl('99999999999999999.99').add(brl('0.01'))).toThrow(InvalidMoneyError);
  });

  test('subtract pode produzir negativo (aritmética interna) e negate inverte', () => {
    const diff = brl('10.00').subtract(brl('25.50'));
    expect(diff.amount).toBe('-15.50');
    expect(diff.isNegative()).toBe(true);
    expect(diff.negate().amount).toBe('15.50');
    expect(brl('0.00').negate().amount).toBe('0.00'); // sem "-0.00"
  });

  test('imutável: operações retornam novas instâncias', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.amount).toBe('10.00');
    expect(b).not.toBe(a);
  });

  test('predicados e comparação', () => {
    expect(brl('0.00').isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('5.00').isLessThan(brl('5.01'))).toBe(true);
    expect(brl('5.00').isLessThan(brl('5.00'))).toBe(false);
    expect(brl('5').equals(brl('5.00'))).toBe(true);
    expect(brl('5.00').equals(usd('5.00'))).toBe(false);
  });

  test('serialização: toJSON é MoneyProps com string; toString legível', () => {
    expect(JSON.stringify(brl('25.5'))).toBe('{"amount":"25.50","currency":"BRL"}');
    expect(brl('25.5').toString()).toBe('25.50 BRL');
  });
});

describe('Money: conflito de moeda', () => {
  test.each([
    ['add', (a: Money, b: Money) => a.add(b)],
    ['subtract', (a: Money, b: Money) => a.subtract(b)],
    ['isLessThan', (a: Money, b: Money) => a.isLessThan(b)],
  ])('%s entre BRL e USD lança CurrencyMismatchError', (_name, op) => {
    expect(() => op(brl('1.00'), usd('1.00'))).toThrow(CurrencyMismatchError);
  });
});
