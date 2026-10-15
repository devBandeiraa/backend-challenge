FROM oven/bun:1.4.2-alpine

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY tsconfig.json ./
COPY src ./src

ENV NODE_ENV=production PORT=3000
EXPOSE 3000
USER bun
# Bun executa TypeScript direto; sem etapa de build. SIGTERM chega ao processo (PID 1 via exec form).
CMD ["bun", "run", "src/main.ts"]
