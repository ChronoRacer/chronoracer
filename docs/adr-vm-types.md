# ADR - VM Types

Status: accepted

Global prerequisites:

- available in `europe-west3`
- no commitment obligations or discounts
- the race stack uses one Kafka VM and one Core VM; their responsibilities are described in [Kafka](component-kafka.md)
  and [Core](component-core.md)

## Decision

Use these machine types as the Terraform defaults:

| VM              | Machine type    | vCPU | Memory | Boot disk                   |
|-----------------|-----------------|-----:|-------:|-----------------------------|
| Kafka           | `n4-standard-2` |    2 |   8 GB | `hyperdisk balanced`, 20 GB |
| Core + InfluxDB | `c4d-highcpu-4` |    4 |   7 GB | `hyperdisk balanced`, 20 GB | 

Terraform variables in [`infra/race/variables.tf`](../infra/race/variables.tf) are the implementation of this decision.
These types must not be changed while a race is running; the race lifecycle and instance pinning rules are owned
by [Terraform](component-terraform.md).

## Core + InfluxDB VM

### Requirements

- Core can be compiled for either x86_64 or AArch64, but the current payload pipeline publishes a Linux x86_64 binary.
- InfluxDB 3 Core supports x86_64.
- Core needs predictable CPU capacity and at least two physical cores to avoid making message processing and database
  work compete on one physical core.
- The VM must provide enough memory for both processes, InfluxDB cache, the operating system, and short bursts beyond
  the measured run.

### Evidence

The local load test sent 500,000 sensor-data batches in about 260 seconds
(see [load test](/core/dev-only/dev-load-test/README.md). Core and InfluxDB were placed in a slice configured by [
`install-core-vm-service.sh`](../core/dev-only/install-core-vm-service.sh) with a 400% CPU quota, a 16 GB maximum, and a
15 GB high watermark. The local Kafka container was limited to 2 CPUs and 8 GB.

The measurements were:

| Measurement                                         |                              Result |
|-----------------------------------------------------|------------------------------------:|
| Whole Core slice peak CPU                           |   around 25% CPU in `systemd-cgtop` |
| Whole Core slice peak memory                        |                              700 MB |
| `app-core.service` CPU time / service lifetime      | 2 min 9.881 sec / 11 min 12.413 sec |
| `app-core.service` peak memory                      |                            123.6 MB |
| `influxdb-core.service` CPU time / service lifetime |       5.852 sec / 14 min 33.875 sec |
| `influxdb-core.service` peak memory                 |                            454.4 MB |

The service lifetimes exceeded the 260-second sending period, so their total CPU times are not a load-window average.
Kafka stayed below 100% of its 2-CPU container limit. The test ran on a faster local Intel Core i7-14700K with DDR5 RAM
through WSL, and Kafka traffic used localhost, so these results establish a useful capacity baseline but do not prove
GCP latency or throughput.

Core thread config:

- `rayon_threads`: 4
- `tokio_workers`: 2
- `tokio_blocking`: 1

### Rationale

We need more CPU than memory for the current setup. 1:4 ratio is not needed, 1:2 is enough.

N2 is the older generation, E2 does not have physical cores we need. The best choice is either N4 or C4 (more
expensive).

For longer workloads, the cost difference would matter, but for around an estimated 30 hours per month, the difference
is tiny and C4 has better CPU architecture with higher frequency. Core pinning to one thread is not required unless
runtime proves otherwise. Since InfluxDB writes locally and we have buffers in Core, the RAM usage should not be too
large, so we can save by going for `highcpu` variant instead of the standard one.

This would be `5.64$/30h` in `europe-west3` with `20GB` boot disk for `c4d-highcpu-4`, which is quite a good deal. Sadly
we cannot go for spot VM here - otherwise it would be three times cheaper.

[In pricing calculator](https://cloud.google.com/products/calculator?dl=CjhDaVF3WVRSbU9XSTVNeTAyTTJRNUxUUXdOR1F0WWprellpMHdZbUV4TkRRM1kySmlNekFRQVE9PRAIGiRDQUQ5MTA4MC0zOUVDLTQ2NTYtQjg3OC05QTUxQjY0MzA2Rjg&_gl=1*hmvx65*_up*MQ..&gclid=CjwKCAjwqc_UBhBKEiwAWbl25q5jMtOk9eAmBdh9cuBnOYDnJZJLyO9afG3l6jbu6ZQ56lfLfq_q6BoCYFAQAvD_BwE&gclsrc=aw.ds)

## Kafka VM

Kafka used less than one of the two CPUs available in the local test. `n4-standard-2` therefore should be enough.
That would be around `3.30$/30h` in `europe-west3` with `20GB` balanced hyperdisk boot disk.

If needed, we can go for a highmem variant, which would bump to `4.30$/30h`, so not that much difference.

For some reason, the N4 version is cheaper than the E2 version in the pricing calculator for Frankfurt.

## [!IMPORTANT] These values might change after load testing on real GCP infrastructure
