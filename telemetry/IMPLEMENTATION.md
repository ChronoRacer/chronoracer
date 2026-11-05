# Telemetry implementation

Behavioral contract, Terraform-injected variables, Cloud Run shape, and fan-out rules:
[Telemetry](../docs/component-telemetry.md). Core stream fields and loss:
[Core telemetry stream](../docs/component-core.md#telemetry-stream).

This file owns the code layout, build, routes, preview semantics, hard limits, and tuning variables of the
monolith: one process with the HTTP + WebSocket UI and a single Core `Watch`.

## Stack

| Piece            | Choice                                                                 |
|------------------|------------------------------------------------------------------------|
| Runtime          | Node 22+, TypeScript, pnpm                                             |
| HTTP / WebSocket | Hono (`@hono/node-server`, `@hono/node-ws`) + `ws`                     |
| UI build         | Vite + React, static assets served by the same process                 |
| Charts           | uPlot                                                                  |
| Live instruments | Motion (`motion/react`) tweens on sensor-shaped SVG/CSS widgets        |
| Track map        | Leaflet over the [GUGiK orthophoto](https://www.geoportal.gov.pl/) WMTS (Web Mercator, open data, Poland only, no key) |
| Core client      | `@grpc/grpc-js` + nice-grpc (h2c only; the browser never talks to Core) |
| Kafka producer   | `@platformatic/kafka`, pure TypeScript, no native addon; driver messages only |
| Proto tooling    | Buf (`buf generate` → `src/server/gen`, plus `buf lint` / breaking)    |
| Auth             | `@hono/oidc-auth`: Auth0 authorization code + PKCE, HS256 session cookie |
| Tests            | Vitest                                                                 |

## Tree

```text
telemetry/
  IMPLEMENTATION.md
  README.md
  Dockerfile
  package.json  pnpm-lock.yaml  pnpm-workspace.yaml
  tsconfig.server.json        # src/server + src/shared → dist/
  tsconfig.client.json        # typecheck of src/client + src/shared (Vite builds it)
  vite.config.ts
  vitest.config.ts
  buf.gen.yaml                # ts-proto + nice-grpc → src/server/gen

  src/
    shared/
      protocol.ts             # browser WebSocket protocol; the only definition, used by server and client
    server/
      index.ts                # entry: env, store, hub, HTTP, Core watch, graceful shutdown
      http.ts                 # Hono app: routes below, WS upgrade, static UI
      log.ts                  # JSON log lines with Cloud Logging `severity`
      json.ts                 # JSON with bigint as decimal strings
      env/
        schema.ts             # zod schema of every variable
        index.ts              # loadEnv(): validated TelemetryEnv; OAuth required unless disabled
        types.ts
      auth/
        index.ts              # OIDC config, /auth routes, requireSession, readSession, requireSameOrigin (WS)
      core/
        grpc-address.ts       # CORE_TELEMETRY_URL → plaintext dns:/// target
        watch.ts              # the single Watch loop: reconnect, backoff, race_id check
        types.ts              # aliases of the generated proto types
      store/
        PreviewStore.ts       # min/max buckets with hard caps, gap markers, changes for the hub
        types.ts  constants.ts  index.ts
      analysis/
        schema.ts             # zod mirror of the analysis ingest bodies
        AnalysisStore.ts      # per-prediction state, recent alerts, delta for the hub
        routes.ts             # /analysis/v1 POST routes: bearer token, body limit, validation
      driver/
        schema.ts             # zod validation of a driver message request
        DriverMessages.ts     # shared recent history with delivery status, changes for the hub
        publisher.ts          # Kafka producer on the private listener
        routes.ts             # POST /api/driver/messages: 202, then produce in the background
      fanout/
        hub.ts                # per-tick snapshot / delta push, backpressure
        ws-protocol.ts        # encode server messages, validate client messages
      gen/                    # generated only — do not edit
    client/                   # Vite React app
      sensors/
        catalog.ts            # import of the sensor contract
        layout.ts             # dashboard zones (incl. hidden sensors), status labels, sensors missing from the contract
        tones.ts              # color thresholds and nominal bands (UI judgement, not contract)
      lib/
        series-store.ts       # per-sensor series, change notifications, chart window edge, sample pairing
        trail.ts              # map trail for the page's lifetime (survives tab switches)
        fanout-tick.ts        # server push interval from the snapshot, as React context
        driver.ts             # driver message kinds' labels, presets, send, history merge
        format.ts  svg.ts     # readout formatting; gauge geometry
      hooks/
        useTelemetrySocket.ts # WebSocket → store; connection, gaps, sensor list
        useSensor.ts          # one sensor's newest value, age, staleness, range check, tone
        useSmoothedNumber.ts  # MotionValue glide + DOM binders (no per-frame React); wrap-aware heading
      components/
        TopBar.tsx            # one row: brand, tabs, connection, gaps, driver strip, logout
        ConnectionStatus.tsx  GapMarkers.tsx
        LiveView.tsx          # LIVE grid
        DrivingZone.tsx       # speed dial, pedals, steering, wheel speeds, g-force
        PowertrainZone.tsx    # rpm dial, EV/engine mode, throttle and load
        VehicleZone.tsx       # energy (HV SOC, fuel, 12 V) and temperatures
        GpsTrack.tsx          # orthophoto map: car, heading, trail, follow
        ServiceZone.tsx       # CHARTS tab: temperatures, mixture, trims, MAF, ignition; generic sensor card
        instruments.tsx       # shared card, readout, bars, temperature bar
        PredictionsPanel.tsx  # ANALYSIS cards per prediction_id
        AlertsPanel.tsx       # ANALYSIS alert list
        AlertToast.tsx        # corner popup for a new alert
        DriverPanel.tsx       # top-bar strip + composer and shared history in the drawer
        Drawer.tsx            # stage overlay for driver radio (Escape closes)
        SensorChart.tsx       # one uPlot per card, one or more sensors: shared window and cursor
```

Tests sit next to the code as `*.test.ts` and are not part of the build output.

## Data flow

```text
Core Watch ─────► core/watch.ts ───────► store ────────────┐
analysis POST ──► analysis/routes.ts ──► analysis store ───┼─(drain every tick)──► fanout hub ──► browser WS
browser POST ───► driver/routes.ts ────► driver history ───┘
                        └─(background)─► Kafka `event`, private listener
```

- `watch.ts`, the analysis routes, and the driver routes only write to their stores; the driver routes also produce
  to Kafka. The hub only reads the stores.
- Analysis state is not filtered by the sensor subscription: every browser gets every prediction and alert.
- Exactly one Core `Watch` for the process lifetime (`client_id` `telemetry`). A `hello` whose `race_id` differs
  from `RACE_ID` is never accepted: the call is closed and retried with backoff. `INVALID_ARGUMENT` from Core exits
  the process, because a retry would send the same request.
- A new socket gets a snapshot of all sensors on the next tick, then deltas. A `subscribe` that changes the selection
  is answered the same way; one that repeats the current selection changes nothing, so the browser's initial
  `subscribe` does not cost a second snapshot. The store folds every point into its bucket before the hub sees it,
  so the snapshot already contains that tick's changes. A bucket sent again replaces the browser's copy.
- Core connection state (`connected`, `hello`) is pushed as a `status` message whenever it changes.

## Browser surface

WebSocket messages and their fields: [`src/shared/protocol.ts`](src/shared/protocol.ts). JSON on `/ws`, u64 values
as decimal strings. Do not expose gRPC-web.

| Route                                         | Access                  | Use                                                                 |
|-----------------------------------------------|-------------------------|---------------------------------------------------------------------|
| `/healthz`                                    | public                  | Cloud Run probe                                                     |
| `/auth/login`, `/auth/callback`, `/auth/logout` | public                | fixed by the [container contract](../docs/component-telemetry.md#container-contract) |
| `/api/me`                                     | public                  | current session, 401 without one                                    |
| `/api/sensors`                                | session                 | tracked sensor ids and ring stats                                   |
| `/api/analysis`                               | session                 | current predictions and recent alerts (same shape as in the snapshot) |
| `/api/driver/messages` (`POST`)               | session, same `Origin`  | send a driver message; `202` with it as `sending`, `503` without Kafka; the outcome arrives over `/ws` |
| `/analysis/v1/*` (`POST`)                     | bearer token            | analysis ingest; contract in [Analysis ingest](../docs/component-telemetry.md#analysis-ingest) |
| `/ws`                                         | session, same `Origin`  | live preview                                                        |
| everything else                               | session                 | Vite build; `index.html` for client routes                          |

## UI

A dark, single-viewport dashboard: `100dvh`, no page scroll down to ~1000 px wide; below 820 px cards stack and the
page scrolls. One top bar, then the stage with three tabs; every card's main visual takes the height that is left, so
the stage has no empty bands. Driver radio opens as an overlay drawer with its own scroll; Escape closes it.

Sensor names, units, ranges, and descriptions come from
[`infra/contract/sensors.json`](../infra/contract/sensors.json), imported at build time. Which zone shows which
sensor is [`src/client/sensors/layout.ts`](src/client/sensors/layout.ts); a test fails when a contract sensor is in no
zone or in two. Its `hidden` zone lists sensors that are received but not shown, because the operator does not act
on them: `gps_sats`, `gps_pdop`, `gps_alt`, `baro`, and `gps_speed` (the speed dial shows `speed`).

UI copy is English (CAPS labels where they fit); driver presets are the message text and stay in the driver's
language.

| Region       | Contents                                                                                                   |
|--------------|------------------------------------------------------------------------------------------------------------|
| Top bar      | brand, tabs `LIVE` / `CHARTS` / `ANALYSIS` (ANALYSIS counts unseen alerts), connection + race, gaps chip (only after a gap), driver strip, logout |
| LIVE         | speed and rpm dials with mode, throttle, load · pedals · steering · g-force · wheel speeds │ track map │ energy (HV SOC, fuel, 12 V) · temperatures |
| CHARTS       | 60 s chart per card filling the stage; unknown sensors after them                                          |
| ANALYSIS     | prediction cards │ alert list                                                                              |
| Alert toast  | corner popup only when a **new** alert arrives                                                             |

- Track map: GUGiK orthophoto (zoom ≤ 19 native) with the car as a heading arrow and the trail driven since the page
  opened, kept outside the component so a tab switch does not lose it. The map follows the car at zoom 17; zoom keeps
  it centred, a drag stops following until *FOLLOW CAR*. A chip says *GPS 2D* or *NO GPS FIX · LAST POSITION* (fix
  below 2D or position stale); a 3D fix shows nothing. Before the first position the map shows Poland and *WAITING FOR
  GPS FIX*. Tiles load from `mapy.geoportal.gov.pl` in the browser; outside Poland the map has no imagery.
- LIVE instruments show the newest value only. Charts live on the CHARTS tab; `lambda`/`lambda_cmd`
  and `stft`/`ltft` share a chart when compared. All charts share that window and hover cursor.
- Orientation follows the contract: `steer_angle` and `accel_lat` left = plus, so the wheel turns and the g-dot moves
  to the screen's left for plus; `accel_long` plus (accelerating) is up.
- Color carries state: cyan for speed, GPS, and EV; green for good values; orange/red for warnings, brake pressure,
  and heat. The rpm dial is neutral until its warning band. Thresholds and nominal bands are [`src/client/sensors/tones.ts`](src/client/sensors/tones.ts) — UI
  judgement for this car, not the contract. A wheel more than 3 km/h and 8 % off the four-wheel mean is highlighted.
- The y axis is the contract `min`–`max`, widened when data leaves it; without a range it follows the data. A value
  outside the range is shown in red, never clipped. NaN and infinities are shown as the value and leave a one-bucket
  hole in the chart.
- A sensor without points shows `—`. A sensor with no point for 5 s (arrival time in the browser) is dimmed. Hovering
  any value shows its sensor id, contract description, and the age of its last point.
- Live sensors missing from the contract appear on the CHARTS tab.
- ANALYSIS: a prediction card is *LIVE* (green), *WAITING* (orange), or *UNAVAILABLE* (red) with analysis' `info`.
  Instruction/description type is emphasized; numeric results use a simpler mono motif. Alerts show time, message,
  reason, and sensor values with contract units. Analysis text is shown as sent. New alerts also fire a corner toast
  (history is not replayed as toasts).
- Sensors in `STATUS_LABELS` (today `gps_fix`) show a state label instead of a number.
- Driver strip → drawer: a preset fills the composer; only Enter or *SEND* sends. Presets are UI copy in
  [`src/client/lib/driver.ts`](src/client/lib/driver.ts). Status is *SENDING…*, *SENT* (Kafka ack, not a read receipt),
  or *FAILED*. Without Kafka configuration the composer is disabled.
- The browser subscribes to all sensors. Buckets and newest samples go to a per-sensor store, not React state, so an instrument
  re-renders only for its own sensors; each chart is created once and updated in place. Numeric readouts show the
  exact sample.
- Motion follows the push interval, which the snapshot carries as `tickMs`. Live instrument values glide with ease-out
  over ~1.6 ticks via Motion (`useSmoothedMotion` and the `useMotion*` DOM binders; the heading takes the shorter way
  round 0°/360°). Binders write `transform` / SVG attributes on the MotionValue's animation frames — they do **not**
  re-render React each frame (that was the source of stepped motion). Consecutive pushes overlap so the needle does
  not fully stop between samples, without overshooting. Charts slide their window every animation frame, one tick
  behind the newest point at wall-clock speed (`SeriesStore.viewEnd`), instead of jumping once per push; sub-pixel
  moves below ~0.15 px are not redrawn. Both put the display roughly one tick behind the data.
- Bars animate with `transform: scale`; SVG needles/wheels use native `transform="rotate(a cx cy)"` (not CSS rotate on
  `<g>`, which breaks the origin). `prefers-reduced-motion` jumps to the sample instead of gliding.

## Gaps and retention

| Event                                                        | Effect                                    | Shown as                                               |
|--------------------------------------------------------------|-------------------------------------------|--------------------------------------------------------|
| Retention: age since arrival, per-sensor cap, total cap      | oldest buckets evicted                    | nothing; counted in `evictedBuckets`                   |
| Core `gap` event                                             | Core dropped a sequence range             | `core` gap                                             |
| `Watch` call ends                                            | outage; Core reports no loss for it       | `reconnect` gap, `status` with `connected: false`      |
| New `stream_id`                                              | Core restarted                            | `stream_restart` gap; buckets before and after are kept |
| Browser socket over `TELEMETRY_WS_MAX_BUFFERED_BYTES`         | updates skipped for that browser only     | `overloaded` message, then a resync snapshot           |

Retention trims the oldest end of a series, so it never makes a hole in the timeline. Age and both caps use the
arrival of a bucket's first sample at Telemetry (not the source timestamp); the total cap always drops the globally
oldest arrival across sensors. The per-sensor cap is `TELEMETRY_RETENTION_MS / TELEMETRY_BUCKET_MS`, which a sensor
with a sane clock never exceeds; one with a skewed clock is kept like any other.

## Hard requirements (Event Loop, RAM, h2c)

Preview stays best-effort: drop preview work under load, never block or slow Core ingest
([Telemetry](../docs/component-telemetry.md#accepted-risks) accepted risks).

### Single process / Event Loop

One Node process is enough for tens of operators. Do not add worker threads or a second process until a load test
proves the Event Loop is hot on encode/fan-out.

- **One encode, many sends.** Each distinct frame (status, gaps, buckets per subscription, snapshot per subscription)
  is encoded once per tick and sent to every matching socket. Nothing is encoded while no browser is connected.
- **Timed batching.** Push every `TELEMETRY_FANOUT_TICK_MS`, inside the 2–5 s latency budget, not once per Core
  `PointBatch`.
- **Per-browser sensor filter** from the shared store; still one Core `Watch`.
- **Per-socket backpressure.** See the last row of the gap table; the Core sink and the ring keep running.
- **Static UI at runtime.** Vite output is served from disk; no Vite work in production.

Cloud Run stays one warm instance; horizontal scale is out of scope without an architecture change.

### Bucket store

The store keeps no raw points. Each point is folded into its sensor's bucket: `TELEMETRY_BUCKET_MS` of source time on
a grid shared by all sensors (bucket `index` = timestamp / bucket width). A bucket keeps its minimum and maximum and
the order they occurred in; a non-finite sample makes it a hole. Each sensor also keeps its newest sample by source
time for instruments. The wire form is [`Buckets`](src/shared/protocol.ts). A late or duplicate point changes its
bucket, which is sent again; min and max are unaffected by duplicates.

Why min/max and not every n-th point: a chart draws about one bucket per pixel (1 200 buckets per 60 s window at the
default), and min/max per pixel column draws the same line as all points (M4 aggregation), so short peaks survive.
Sampling every n-th point drops peaks shorter than the stride and aliases fast oscillation. The browser's work then
depends on the window and sensor count, not on the input rate.

Measured locally with 38 sensors at 900 000 points/s over gRPC: Telemetry at 38 % of one core (protobuf decode
about 300 ns per point, bucketing about 60 ns), about 70 KB/s of WebSocket per browser, every 2 ms spike kept, and
the browser at 60 fps with the service drawer open. A snapshot of a full 60 s window is about 3 MB.

`TELEMETRY_SENSORS=*` may ingest every sensor Core sends, so the store has hard caps on buckets (variables below).
About 244 bytes of heap per bucket (measured), so the default total cap of 100 000 buckets is about 24 MB.
`bucketsInStore`, `approxBytes`, and the eviction counters are logged every 20 s as `telemetry_stats`, so memory
pressure is visible before Cloud Run OOM-kills the container.

Core already drops on its own queue limits; Telemetry's store is a second RAM copy and caps independently.

### Private Core hop (h2c)

The Core link is plaintext HTTP/2 by contract (`CORE_TELEMETRY_URL` must be `http:`). Network authorization is the
Telemetry subnet firewall, not TLS.

- `ChannelCredentials.createInsecure()` on the Watch channel, not SSL.
- `grpc-address.ts` rejects non-`http:` URLs and passes `dns:///host:port` to grpc-js.
- Channel keepalive follows [ADR - Core telemetry stream](../docs/adr-telemetry-stream.md) (15 s).

Do not "fix" this hop by adding TLS; that would fight the platform contract.

### Private Kafka hop (driver messages)

Driver messages go to Kafka's PRIVATE listener (plaintext, `KAFKA_PRIVATE_BOOTSTRAP`), authorized by the private
Kafka firewall rule like Core and Relay; no TLS or SASL on this hop either. One long-lived producer, connected on the
first send. Same guarantees as Relay's operator events: idempotent, `acks` from all in-sync replicas, 3 retries 250 ms
apart, 5 s connect and request timeouts, so a dead broker fails a message within seconds instead of leaving it
`sending`. History keeps the last 100 messages; a request body is at most 4 KiB.

## Tuning and local variables

Terraform-injected variables are owned by the [container contract](../docs/component-telemetry.md#container-contract).
These are optional, have defaults, and are validated at startup; an invalid value stops the process with the
variable's name.

| Variable                          | Default               | Use                                                              |
|-----------------------------------|-----------------------|------------------------------------------------------------------|
| `PORT`                            | `8080`                | HTTP listen port                                                 |
| `TELEMETRY_BUCKET_MS`             | `50`                  | bucket width ([Bucket store](#bucket-store)); about a pixel of the 60 s chart |
| `TELEMETRY_RETENTION_MS`          | `60000`               | maximum age of a bucket since its first arrival; covers the 60 s chart window |
| `TELEMETRY_MAX_BUCKETS`           | `100000`              | total cap; drops the globally oldest arrival across sensors      |
| `TELEMETRY_FANOUT_TICK_MS`        | `100`                 | WebSocket push interval; the UI animates over one tick ([UI](#ui)) |
| `TELEMETRY_WS_MAX_BUFFERED_BYTES` | `1048576`             | per-socket send buffer before updates are skipped                |
| `TELEMETRY_CLIENT_DIR`            | `dist/client`         | UI build directory                                               |
| `SESSION_SECRET`                  | `OAUTH_CLIENT_SECRET` | HS256 key of the 12 h session cookie, at least 32 characters; set it to rotate sessions independently |
| `TELEMETRY_AUTH_DISABLED`         | `false`               | local runs only; otherwise startup fails without the OAuth variables |

## Build and container

- `pnpm run generate` — `buf generate` → `src/server/gen`
- `pnpm run build` — generate, `tsc` for the server, client typecheck + `vite build` into `dist/client`
- `pnpm start` — `node dist/server/index.js`, serving `dist/client` and `/ws`
- `pnpm test` — Vitest
- `pnpm load:charts [points/s]` — build, then [`scripts/mock-core.mjs`](scripts/mock-core.mjs) at that rate
  (default 900 000) into the server with auth off on `PORT` (default 8080); charts are on the CHARTS tab. Ctrl+C
  stops both. The mock alone takes `MOCK_CORE_RATE` and `MOCK_CORE_PORT`. Its GPS laps Tor Poznań at 110 km/h with a
  3D fix, so the map shows a real track; the other sensors are smooth waves.
- Dockerfile is multi-stage: generate + build, then a production `node` image with `TELEMETRY_SENSORS=*`

Dev loop: `pnpm dev:client` proxies `/auth`, `/api`, `/healthz`, and `/ws` to the Node server. With OAuth on, set
`TELEMETRY_ROOT_URL` to the Vite origin so the callback and the WebSocket `Origin` check match.

On SIGTERM the process stops the Watch, closes browser sockets, the Kafka producer, and the HTTP server, and exits on
its own.

Local driver messages: set `KAFKA_PRIVATE_BOOTSTRAP=localhost:9092` and `KAFKA_EVENT_TOPIC=event` against a local
broker (e.g. [`core/dev-only/dev-docker/kafka`](../core/dev-only/dev-docker/kafka/docker-compose.yaml)) and create the
`event` topic first; the producer does not create topics.

Local analysis: `node scripts/mock-analysis.mjs` posts announcements, values, instructions, an unavailable phase, and
alerts to `TELEMETRY_URL` (default `http://127.0.0.1:8080`), with `ANALYSIS_INGEST_TOKEN` when set.

## Remaining work

- Provision `ANALYSIS_INGEST_TOKEN`: Secret Manager secret, Telemetry and analysis access, Terraform `env`.
- Define the unit of `refresh_rate` with analysis; then a card can go stale after a missed refresh.
- CI `buf lint` / `buf breaking` against `core/proto` (generate already uses Buf).
- GCP load test of the Event Loop and bucket memory; workers only if proven needed.

## Out of scope here

- Grafana / `dashboard/` (fleet health only)
- Influx or Kafka reads
- Multiple Core connections or one Watch per browser
- Durable history / replay across Telemetry restarts
