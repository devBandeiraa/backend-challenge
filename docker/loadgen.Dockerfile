FROM oven/bun:1.4.2-alpine

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts

USER bun
CMD ["bun", "run", "scripts/load-runner.ts"]
