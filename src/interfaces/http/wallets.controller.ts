import { Body, Controller, Get, HttpCode, Param, Post, Query, Req } from '@nestjs/common';
import type { Request } from 'express';
import { WalletQueries } from '../../application/queries';
import { ReconcileWalletUseCase } from '../../application/wallets/reconcile-wallet';
import { OpenWalletUseCase } from '../../application/wallets/open-wallet';
import { enrichContext } from '../../infrastructure/observability/context';
import { correlationIdOf } from './correlation';
import { ledgerQuery, openWalletBody, parse, uuidParam } from './validation';

@Controller('wallets')
export class WalletsController {
  constructor(
    private readonly openWallet: OpenWalletUseCase,
    private readonly queries: WalletQueries,
    private readonly reconcileWallet: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown, @Req() req: Request) {
    const input = parse(openWalletBody, body, 'body');
    const wallet = await this.openWallet.execute({ ...input, correlationId: correlationIdOf(req) });
    enrichContext({ walletId: wallet.id });
    return wallet;
  }

  @Get(':walletId')
  get(@Param('walletId') walletId: string) {
    return this.queries.getWallet(this.walletParam(walletId));
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query() query: unknown) {
    const { cursor, limit } = parse(ledgerQuery, query, 'query');
    return this.queries.getLedger(this.walletParam(walletId), { cursor, limit });
  }

  /** Não corrige nada: compara saldo materializado com o ledger, loga, mede e sinaliza. */
  @Post(':walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId') walletId: string) {
    return this.reconcileWallet.execute(this.walletParam(walletId));
  }

  private walletParam(walletId: string): string {
    const id = parse(uuidParam, walletId, 'walletId');
    enrichContext({ walletId: id });
    return id;
  }
}
