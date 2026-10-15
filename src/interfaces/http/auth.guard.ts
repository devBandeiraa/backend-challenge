import { type CanActivate, type ExecutionContext, Injectable, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

export const IS_PUBLIC = 'isPublic';
/** Marca rotas que nunca exigem autenticação (health checks). */
export const Public = () => SetMetadata(IS_PUBLIC, true);

/**
 * PONTO DE EXTENSÃO, intencionalmente no-op (autenticação fora do escopo, ver ARCHITECTURE.md).
 *
 * Desenho previsto com Keycloak/OIDC:
 *  1. Cada provedor é um client confidencial no realm (client_credentials); recebe um JWT curto.
 *  2. Este guard valida assinatura via JWKS do realm (cache + rotação por `kid`), `iss`, `aud`
 *     e `exp`, e extrai o providerId do claim `azp`/client_id.
 *  3. Autorização: `body.providerId` (e o prefixo da Idempotency-Key) precisa ser igual ao
 *     provider do token. Divergência → 403, ANTES de tocar no banco.
 *  4. Rotas @Public (health) continuam abertas. Mensagens SQS são canal interno confiável,
 *     mas a identidade do provedor nelas passa pelas mesmas validações de domínio.
 *
 * O guard já está registrado globalmente; trocar a implementação não altera controllers.
 */
@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    return true; // no-op: aqui entraria a validação do JWT descrita acima
  }
}
