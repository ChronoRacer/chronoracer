# Load test

Publishes Avro sensor batches to Kafka for the real sensor names in the
[channel file](../../analysis/corolla_channels-2.json). Values come from a drive model (a looping lap of a hybrid
Corolla), clamped to each channel's range, so related channels stay consistent.

Each message is one random sensor with a random number of readings (`min_points_per_message` to
`max_points_per_message`) spread over the `batch_window_ms` that ended now. Messages are paced so the total stays near
`points_per_second`. Kafka throttling can push the achieved rate below the target; the `[Sensors]` line reports the
actual rate.

Optionally a change-driver and a shutdown message are sent per [config file](config.json). For a remote cluster with
OAuth, fill in the `oauth` block and set `enabled`.

## Run

```bash
pip install -r requirements.txt
python3 load_test.py --rate 2000 --duration 60
```

| Flag | Overrides |
| --- | --- |
| `--rate` | `load.points_per_second` |
| `--duration` | `load.duration_s` |
| `--seed` | `load.seed` (random when unset) |
| `--config` | config file path |
| `--no-control` | skips change-driver and shutdown messages |
| `--report-interval` | seconds between `[Report]` lines (default 10) |

## Report

Every `--report-interval` the generator prints a `[Report]` line for that window, and a `[Summary]` line for the whole
run after the final flush. Only sensor messages are counted.

| Field | Meaning |
| --- | --- |
| `sent_pts_s`, `sent_msg_s` | Rate handed to the producer |
| `behind_max_s` | Worst lateness against the pacing schedule |
| `queued` | Messages in the producer not yet acknowledged by Kafka |
| `delivered`, `failed` | Delivery reports; the first failure per window is printed |
| `delivery_ms` | Produce to broker acknowledgement, same histogram format as Core's [pipeline report](../../docs/component-core.md#pipeline-report) |

A `behind_max_s` that keeps growing means this process cannot generate its rate: add instances rather than read it as a
Kafka or Core limit. A growing `queued` with a rising `delivery_ms` means Kafka is throttling the producer.

For load tests, run the generator on an NTP-synced GCP VM: Core's latencies are measured from the record timestamp this
process sets, so clock skew shows up directly in them.

## Parallel instances

Run several processes for more load. Each draws its own random seed, so lap phase and sensor order differ. Pass
`--no-control` to all but one so control messages are sent once:

```bash
python3 load_test.py --rate 5000 &
python3 load_test.py --rate 5000 --no-control &
wait
```

Total load is the sum of the instance rates.
