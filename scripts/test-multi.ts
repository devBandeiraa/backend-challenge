/**
 * Sobe o stack completo (3 réplicas + nginx), roda o teste multi-instância e desliga as réplicas
 * no final, qualquer que seja o resultado: os testes de integração do host (`bun test`) não podem
 * concorrer com publishers/workers das réplicas pela mesma outbox e pelas mesmas pendências.
 */
const run = (cmd: string[], env: Record<string, string> = {}) =>
  Bun.spawn(cmd, { stdout: 'inherit', stderr: 'inherit', env: { ...process.env, ...env } }).exited;

let code = await run(['docker', 'compose', 'up', '-d', '--build', '--wait']);
if (code === 0) {
  code = await run(['bun', 'test', '--timeout', '60000', './test/e2e/multi-instance.e2e.ts'], { MULTI_INSTANCE: '1' });
}
await run(['docker', 'compose', 'stop', 'app', 'nginx']);
process.exit(code);

export {};
