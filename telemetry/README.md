# Telemetry

Contract and runtime shape: [Telemetry](../docs/component-telemetry.md).

Code layout, routes, gaps, limits, and tuning variables: [IMPLEMENTATION.md](IMPLEMENTATION.md).

Package manager is `pnpm`.

```bash
pnpm install
pnpm build
pnpm test
CORE_TELEMETRY_URL=http://127.0.0.1:50051 TELEMETRY_AUTH_DISABLED=1 pnpm start
```


Modules:
- env: module for securely validating environment variables. Using zod and @t3-oss/env-core.
- store: module for storing data in RAM. Using denque (Double-Ended Queue)