FROM node:26-alpine AS build

RUN corepack enable

WORKDIR /app/telemetry

COPY telemetry/package.json telemetry/pnpm-lock.yaml telemetry/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY core/proto/telemetry_v1.proto /app/core/proto/telemetry_v1.proto
COPY telemetry/scripts ./scripts
COPY telemetry/tsconfig.json ./
COPY telemetry/src ./src

RUN pnpm run build

FROM node:26-alpine AS runtime

RUN corepack enable

ENV NODE_ENV=production
WORKDIR /app/telemetry

COPY telemetry/package.json telemetry/pnpm-lock.yaml telemetry/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile --prod

COPY --from=build /app/telemetry/dist ./dist

USER node
EXPOSE 8080
CMD ["node", "dist/index.js"]
