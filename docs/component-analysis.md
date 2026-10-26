# Analysis

## Lap detection

The prediction process detects laps from matching `gps_lat` and `gps_lon` windows only.
It starts collecting at the first valid GPS timestamp and records the current GPS point as
the start after 2.5 seconds. It must not infer elapsed time from a sample count because
sensor and aggregation rates vary.

A lap is armed after the car has moved 20 m from that point. It completes when the line
segment joining two consecutive GPS positions passes within 10 m of the start. Segment
intersection prevents a sparse GPS stream from skipping the start between samples.
