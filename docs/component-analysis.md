# Analysis

Analysis consumes aggregated telemetry windows, evaluates alert rules, detects laps, and produces live predictions.
It is consume-only on the vehicle path: it reads Core `Watch` and posts alerts and predictions to Telemetry for the
engineer's panel. It does not write to Core, Kafka, or the car.

## Runtime and inputs

The stream process in [`analysis/data_stream/pathway_stream_pub.py`](../analysis/data_stream/pathway_stream_pub.py) publishes separate local ZeroMQ topics for rules and predictions. The [rules engine](../analysis/rules/rules.py) consumes `sensor_agg`; the [predictions engine](../analysis/predictions/predictions.py) consumes `sensor_pred`.

Core still emits car-specific physical `sensor_id`s. The gRPC connector remaps them once at ingest using `gs://chronoracer-artifacts/analysis/sensor_config.json` (`BUSINESS_KEY → physical_id`, inverted at runtime). A missing, empty, or invalid `sensor_config.json` is logged and retried in-process with the same backoff as rule download; Watch does not start until a usable mapping loads. Unmapped physical ids are dropped. For each business key the connector also drops a point whose `timestamp_nano` is earlier than the last timestamp already accepted for that key; arrival time does not matter. From Pathway onward, rules and predictions see only business keys (`COOLANT_TEMP_C`, `CAR_SPEED_KMH`, …). Tumbling windows for rules are one second of event time and for predictions half a second; they emit when event time reaches the window end.

The required business-key list is owned by `gs://chronoracer-artifacts/analysis/sensor_needed.json` (`RULES_REQUIRED_SENSOR_IDS`). Operators build the mapping with [`analysis/rules/build_config.py`](../analysis/rules/build_config.py) (`python -m analysis.rules.build_config`), which writes `analysis/sensor_config.json`.

Rules are loaded from `gs://chronoracer-artifacts/analysis/rules/*.json`. The rule JSON is the runtime source for signal selection, expressions, thresholds, persistence, and decision-table messages. Those values can change independently of this document, so they are not duplicated here. Artifact layout and ownership are documented in [GCS artifacts](component-gcs_buckets.md#artifacts-bucket). A GCS or empty-prefix failure at process start is logged and retried in-process with exponential backoff (1s, doubling to 30s). The process stays up and does not subscribe to windows until at least one rule JSON loads.

## Alert rules

A rule runs when its required signals are present in an aggregated window. Its JavaScript function evaluates current values and recent history; the decision table maps the returned state to an `analysis_message`. The function also supplies an `analysis_reason` and the contributing readings attached to an alert.

States have these meanings:

- `GREEN`: the rule did not detect a sustained condition at its configured level. This does not prove every individual reading was normal.
- `YELLOW`: warning; monitor the condition.
- `RED`: serious condition requiring attention.
- `BLACK`: critical condition requiring immediate attention.
- `SENSOR_ERROR`: a required reading is outside its configured physical range, or the rule configuration is invalid.
- `NO_DATA`: required readings or paired history are missing or invalid. The decision table's blank/default row supplies this message.

The engine sends alerts to Telemetry for `YELLOW`, `RED`, `BLACK`, and `SENSOR_ERROR`. `GREEN` and `NO_DATA` are rule results but are filtered before the alert request. Repeated alerts for one rule are rate-limited to one send per three seconds. A failed alert request is logged as a warning and does not stop evaluation.

| Rule object | What it detects | States in the decision table |
|---|---|---|
| `ENGINE_COOLANT_TEMPERATURE` | Sustained high engine coolant temperature. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `ENGINE_OIL_TEMPERATURE` | Sustained high engine oil temperature. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `12V_BATTERY_VOLTAGE` | Sustained voltage deviation on either the low or high side. | RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `HYBRID_BATTERY_CHARGE_LEVEL` | Low hybrid battery state of charge. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `BRAKE_PRESSURE_SENSOR_DIFF` | Sustained disagreement between two brake-pressure channels. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `POSSIBLE_FUEL_STARVATION` | Possible fuel starvation while cornering under engine load. | RED, GREEN, SENSOR_ERROR, NO_DATA |
| `OIL_TO_COOLANT_TEMPERATURE_DIFF` | Sustained separation between oil and coolant temperatures; a possible heat-exchanger or cooling issue. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `INTAKE_TO_AMBIENT_TEMEPRATURE_DIFF` | Intake heat soak relative to ambient temperature. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `LEAN_MIXTURE_UNDER_LOAD` | A lean measured mixture relative to the commanded mixture under high engine load. | RED, GREEN, SENSOR_ERROR, NO_DATA |
| `COLD_OIL_AT_HIGH_RPM` | Sustained high engine speed while the oil is cold. | YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `POSSIBLE_WHEEL_LOCKUP` | Possible wheel lock-up while braking, based on wheel-speed slip. | YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `VEHICLE-TO-GPS-SPEED-DIFF` | Sustained disagreement between vehicle and GPS speed. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `LOW_FUEL_LEVEL` | Low or critically low fuel level. | BLACK, RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |
| `INTAKE_AIR_TEMPERATURE` | Sustained high intake-air temperature. | RED, YELLOW, GREEN, SENSOR_ERROR, NO_DATA |

Decision-table messages should describe the condition and its severity, not repeat configured numeric thresholds. Thresholds and persistence remain owned by the rule JSON. The object name `INTAKE_TO_AMBIENT_TEMEPRATURE_DIFF` currently contains a spelling error; use the exact object name when locating it in the bucket.

### Rule review notes

The current bucket snapshot contains two incorrect table messages: the `SENSOR_ERROR` row in `ENGINE_COOLANT_TEMPERATURE` duplicates the GREEN message, and the `SENSOR_ERROR` row in `COLD_OIL_AT_HIGH_RPM` also duplicates its GREEN message. These rows should describe an out-of-range reading. The other reviewed tables have state-specific messages without embedded numeric thresholds.

The optional BLACK thresholds are not configured for `12V_BATTERY_VOLTAGE` or `INTAKE_AIR_TEMPERATURE`, and neither table has a BLACK row. Their functions therefore do not currently produce a BLACK result. In `INTAKE_AIR_TEMPERATURE`, the configured persistence used by the function is its dedicated persistence expression; a separate generic persistence expression is not used by that function.

## Lap detection

The prediction process detects laps from GPS position. A window without GPS lat/lon skips only the lap-detector update; it does not stop predictions. Predictions marked as lap-based (`needs_lap` in the registry) run only after the lap start is set; the others (oil and coolant forecasts) run on every window whose declared inputs are present. A missing input such as `CAR_SPEED_KMH` skips only the predictions that declare it. It records the first valid GPS timestamp, then sets the start point using a later GPS position once 1.5 seconds have elapsed. Elapsed time comes from GPS timestamps, not from a presumed sample rate.

A lap is armed after the car moves 20 m from the start. It completes when the segment between consecutive GPS points comes within 8 m of the start, which prevents a sparse GPS stream from skipping the start between samples.

The implementation in [`prediction_models.py`](../analysis/predictions/prediction_models.py) is authoritative.

## Predictions

The [prediction registry and output formatting](../analysis/predictions/predictions_files.py) define the prediction names and their required inputs as business keys (same contract as rules after ingest remap). The engine marks a prediction available when its declared signal set is present. Availability does not guarantee that a result is ready: some predictions also require GPS/lap context or enough observations to train their online model. Results carry a numeric value, an instruction, and the contributing readings.

| Prediction ID | Meaning and current behavior |
|---|---|
| `fuel_prediction` | Projects fuel remaining at the end of the current lap and, once enough lap observations exist, estimates laps to empty and fuel use per lap. It learns from completed laps and their average speed. |
| `lap_time_prediction` | Projects the full lap time near the quarter-, halfway-, and three-quarter-distance track splits. The detector records the track until `track_ready_lap` (default 2). The model publishes nothing at a split until `min_split_laps` (default 2) finished laps have been recorded there. |
| `tyre_wear_prediction` | Projects tyre grip retention at the end of the lap relative to a learned fresh-tyre baseline. It builds that baseline from early-lap cornering data. |
| `brake_fade_prediction` | Projects brake performance at the end of the lap relative to a learned fresh-brake baseline, using selected straight-line braking samples. |
| `car_fluids_prediction` | Forecasts engine oil temperature over the configured forward horizon (display name: oil temperature forecast). The prediction ID keeps the historical `car_fluids` label. |
| `coolant_temp_prediction` | Forecasts coolant temperature 60 seconds ahead from recent coolant/oil temperatures and current driving conditions. |
| `lap_compare_prediction` | After each finished lap (from the second lap onward), compares that lap with the previous and best lap and publishes a composite lap score plus a comparison summary (lap time deltas, average speed, hard braking, HV SoC use). |

The table lists the predictions currently registered in the engine. `SpeedOnTurnsPrediction` is implemented in code but its registration is commented out, so `speed_on_turns_prediction` is not published. The active prediction classes use in-process models; the engine's ONNX download helper is not called by these registered predictions.

The running/unavailable status reports whether each prediction's declared input set is present. On the first sensor window the engine announces every registered prediction (unavailable when inputs are missing, running when the set is complete); afterward it posts only on availability transitions. `refresh_rate` is a static English display string set on each prediction class: finish-cadence models use `every lap`; split models use labels such as `every 25/50/75% distance`; time-horizon forecasts (coolant/oil) use `every {fit_time_s} s` from the model fit interval; turn models use `at each turn`. When `refresh_rate` is null, Telemetry shows "nieregularne odświeżanie". A prediction can still skip output while it waits for a lap, GPS track context, or enough training data. Some models learn a baseline online during the race, so early results may be unavailable or provisional.
