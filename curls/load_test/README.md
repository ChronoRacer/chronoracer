# Load test

Publishes Avro sensor batches to Kafka for the real sensor names in the
[channel file](../../analysis/corolla_channels-2.json). Values come from a drive model (a looping lap of a hybrid
Corolla), clamped to each channel's range, so related channels stay consistent.

Each message is one random sensor with a random number of readings (`min_points_per_message` to
`max_points_per_message`) spread over the `batch_window_ms` that ended now. Messages are paced so the total stays near
`points_per_second`. Kafka throttling can push the achieved rate below the target; the final line reports the actual
rate.

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

## Parallel instances

Run several processes for more load. Each draws its own random seed, so lap phase and sensor order differ. Pass
`--no-control` to all but one so control messages are sent once:

```bash
python3 load_test.py --rate 5000 &
python3 load_test.py --rate 5000 --no-control &
wait
```

Total load is the sum of the instance rates.
