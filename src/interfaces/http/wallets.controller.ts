import { Body, Controller, Get, HttpCode, Param, Post, Query, Req } from '@nestjs/common';
import type { Request } from 'express';
import { WalletQueries } from '../../application/queries';
import { OpenWalletUseCase } from '../../application/wallets/open-wallet';
import { correlationIdOf } from './correlation';
import { ledgerQuery, openWalletBody, parse, uuidParam } from './validation';

@Controller('wallets')
export class WalletsController {
  constructor(
    private readonly openWallet: OpenWalletUseCase,
    private readonly queries: WalletQueries,
  ) {}

  @Post()
  @HttpCode(201)
  create(@Body() body: unknown, @Req() req: Request) {
    const input = parse(openWalletBody, body, 'body');
    return this.openWallet.execute({ ...input, correlationId: correlationIdOf(req) });
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: string) {
    return this.queries.getWallet(parse(uuidParam, walletId, 'walletId'));
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query() query: unknown) {
    const { cursor, limit } = parse(ledgerQuery, query, 'query');
    return this.queries.getLedger(parse(uuidParam, walletId, 'walletId'), { cursor, limit });
  }
}
