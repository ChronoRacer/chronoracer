FROM node:26-alpine AS build
RUN npm install -g corepack && corepack enable
COPY core/proto /app/core/proto
COPY infra/contract/sensors.json /app/infra/contract/sensors.json

WORKDIR /app/telemetry

COPY telemetry/package.json telemetry/pnpm-lock.yaml telemetry/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY telemetry/buf.gen.yaml ./
COPY telemetry/tsconfig.server.json telemetry/tsconfig.client.json telemetry/vite.config.ts ./
COPY telemetry/src ./src

RUN pnpm run build

FROM node:26-alpine AS runtime
ENV NODE_ENV=production
ENV TELEMETRY_SENSORS=*
RUN npm install -g corepack && corepack enable
WORKDIR /app/telemetry

COPY telemetry/package.json telemetry/pnpm-lock.yaml telemetry/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --prod

COPY --from=build /app/telemetry/dist ./dist

USER node
EXPOSE 8080
CMD ["node", "dist/server/index.js"]
