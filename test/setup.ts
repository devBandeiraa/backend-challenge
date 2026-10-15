// Timeout: passado por --timeout nos scripts (bun run test). setDefaultTimeout no preload só
// vale para o primeiro arquivo da execução, então não é usado aqui.

// Os testes do host medem a outbox e as pendências diretamente; réplicas do compose rodando
// teriam publishers/workers concorrendo pelas mesmas linhas e tornariam as medições inválidas.
if (process.env.MULTI_INSTANCE !== '1') {
  const replicas = await fetch('http://localhost:8088/health/live', { signal: AbortSignal.timeout(300) }).catch(() => undefined);
  if (replicas?.ok) {
    throw new Error('app replicas are running: stop them with "docker compose stop app nginx" (bun run test:multi does it)');
  }
}

export {};
