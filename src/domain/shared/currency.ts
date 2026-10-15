/**
 * Moedas aceitas na ENTRADA: códigos ISO-4217 de moedas correntes cuja unidade menor tem 2 casas,
 * compatíveis com a escala fixa de 2 casas do Money.
 *
 * Fora da lista, de propósito:
 *  - unidade menor ≠ 2 (JPY, KRW, CLP, VND... com 0 casas; KWD, BHD, JOD, OMR, TND... com 3):
 *    2 casas fixas criariam centavos inexistentes ou perderiam milésimos;
 *  - códigos de fundos, metais, unidades de títulos, DES, teste e "sem moeda" (BOV, XAU, XDR, XTS, XXX...);
 *  - códigos retirados (HRK, SLL, ZWL, ANG, BGN, CUC...).
 * Revisada em 10/2026. Mudar a lista é mudança de código versionada; dados já persistidos continuam
 * legíveis porque a reidratação (Money.rehydrate) valida só o formato.
 */
export const ISO_4217_TWO_DECIMAL_CURRENCIES: ReadonlySet<string> = new Set([
  'AED', 'AFN', 'ALL', 'AMD', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BMD', 'BND', 'BOB', 'BRL',
  'BSD', 'BTN', 'BWP', 'BYN', 'BZD', 'CAD', 'CDF', 'CHF', 'CNY', 'COP', 'CRC', 'CUP', 'CVE', 'CZK', 'DKK', 'DOP',
  'DZD', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GTQ', 'GYD', 'HKD', 'HNL',
  'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IRR', 'JMD', 'KES', 'KGS', 'KHR', 'KPW', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR',
  'LRD', 'LSL', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRU', 'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN',
  'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'QAR', 'RON', 'RSD', 'RUB',
  'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLE', 'SOS', 'SRD', 'SSP', 'STN', 'SVC', 'SYP', 'SZL', 'THB',
  'TJS', 'TMT', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'USD', 'UYU', 'UZS', 'VED', 'VES', 'WST', 'XCD', 'XCG',
  'YER', 'ZAR', 'ZMW', 'ZWG',
]);

export function isAcceptedCurrency(code: unknown): code is string {
  return typeof code === 'string' && ISO_4217_TWO_DECIMAL_CURRENCIES.has(code);
}
