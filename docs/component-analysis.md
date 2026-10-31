# Analysis

Analysis consumes aggregated telemetry windows, evaluates alert rules, detects laps, and produces live predictions.

## Runtime and inputs

The stream process in [`analysis/data_stream/pathway_stream_pub.py`](../analysis/data_stream/pathway_stream_pub.py) publishes separate local ZeroMQ topics for rules and predictions. The [rules engine](../analysis/rules/rules.py) consumes `sensor_agg`; the [predictions engine](../analysis/predictions/predictions.py) consumes `sensor_pred`.

Rules and the signal catalog are loaded from `gs://chronoracer-artifacts/analysis/rules/*.json` and `analysis/sensors.json`. Artifact layout and ownership are documented in [GCS artifacts](component-gcs_buckets.md#artifacts-bucket). The rule JSON is the runtime source for signal selection, expressions, thresholds, persistence, and decision-table messages. Those values can change independently of this document, so they are not duplicated here.

## Alert rules

A rule runs when its required signals are present in an aggregated window. Its JavaScript function evaluates current values and recent history; the decision table maps the returned state to an `analysis_message`. The function also supplies an `analysis_reason` and the contributing readings attached to an alert.

States have these meanings:

- `GREEN`: the rule did not detect a sustained condition at its configured level. This does not prove every individual reading was normal.
- `YELLOW`: warning; monitor the condition.
- `RED`: serious condition requiring attention.
- `BLACK`: critical condition requiring immediate attention.
- `SENSOR_ERROR`: a required reading is outside its configured physical range, or the rule configuration is invalid.
- `NO_DATA`: required readings or paired history are missing or invalid. The decision table's blank/default row supplies this message.

The engine sends alerts to Telemetry only for `YELLOW`, `RED`, and `BLACK`. `GREEN`, `SENSOR_ERROR`, and `NO_DATA` are rule results but are filtered before the alert request. Repeated alerts for one rule are rate-limited to one send per three seconds.

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

The prediction process detects laps from GPS position, with vehicle speed required by the outer prediction handler. It records the first valid GPS timestamp, then sets the start point using a later GPS position once 1.5 seconds have elapsed. Elapsed time comes from GPS timestamps, not from a presumed sample rate.

A lap is armed after the car moves 20 m from the start. It completes when the segment between consecutive GPS points comes within 8 m of the start, which prevents a sparse GPS stream from skipping the start between samples.

The implementation in [`prediction_models.py`](../analysis/predictions/prediction_models.py) is authoritative.

## Predictions

The [prediction registry and output formatting](../analysis/predictions/predictions_files.py) define the prediction names and their required inputs. The engine marks a prediction available when its declared signal set is present. Availability does not guarantee that a result is ready: some predictions also require GPS/lap context or enough observations to train their online model. Results carry a numeric value, an instruction, and the contributing readings.

| Prediction ID | Meaning and current behavior |
|---|---|
| `fuel_prediction` | Projects fuel remaining at the end of the current lap and, once enough lap observations exist, estimates laps to empty and fuel use per lap. It learns from completed laps and their average speed. |
| `lap_time_prediction` | Projects the full lap time near the quarter-, halfway-, and three-quarter-distance track splits. It learns from completed laps at each split. |
| `tyre_wear_prediction` | Projects tyre grip retention at the end of the lap relative to a learned fresh-tyre baseline. It builds that baseline from early-lap cornering data. |
| `brake_fade_prediction` | Projects brake performance at the end of the lap relative to a learned fresh-brake baseline, using selected straight-line braking samples. |
| `car_fluids_prediction` | Predicts engine oil temperature over the configured forward horizon. Despite its ID and display name, the current implementation predicts oil temperature only. |

These five predictions are registered and active in the current engine. `SpeedOnTurnsPrediction` is implemented in code but its registration is commented out, so `speed_on_turns_prediction` is not currently published. The active prediction classes use in-process models; the engine's ONNX download helper is not called by these registered predictions.

The running/unavailable status reports whether each prediction's declared input set is present. A prediction can still skip output while it waits for a lap, GPS track context, or enough training data. Some models learn a baseline online during the race, so early results may be unavailable or provisional.
