# ADR - VM Types

Status: accepted

Global prerequisites:

- available in `europe-west3`
- no commitment obligations or discounts
- the race stack uses one Kafka VM and one Core VM; their responsibilities are described in [Kafka](component-kafka.md)
  and [Core](component-core.md)

## Decision

Use these machine types as the Terraform defaults:

| VM              | Machine type    | vCPU | Memory | Boot disk     | Reason                                                                                                                              |
|-----------------|-----------------|-----:|-------:|---------------|-------------------------------------------------------------------------------------------------------------------------------------|
| Kafka           | `e2-standard-2` |    2 |   8 GB | `pd-balanced` | Matches the configured Kafka test envelope; CPU observations did not require more.                                                  |
| Core + InfluxDB | `n2-standard-4` |    4 |  16 GB | `pd-ssd`      | Provides two physical cores on the N2 SMT layout and leaves room for InfluxDB cache, the operating system, and slower GCP hardware. |

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
through WSL, and Kafka traffic used localhost, so these results establish a useful capacity baseline but do not prove GCP
latency or throughput.

Core thread config:
- `rayon_threads`: 4
- `tokio_workers`: 2
- `tokio_blocking`: 1

### Rationale

The measurements do not justify 16 GB and four vCPUs for raw utilization alone. The selected `n2-standard-4` keeps the
physical-core requirement, retains substantial memory headroom for InfluxDB and filesystem cache, and gives room for the
local-to-GCP performance difference without making the database share one physical core with Core. The current x86_64
payload also avoids an ARM image and build-pipeline change.

## Kafka VM

Kafka used less than one of the two CPUs available in the local test. `e2-standard-2` therefore supplies the tested
2-vCPU and 8-GB envelope without carrying over the unused `e2-standard-4` capacity. Kafka has no requirement for the
Core VM's physical-core layout in the measured workload.

If a GCP run shows CPU contention or latency on Kafka despite staying within this capacity, the next size is
`n2-standard-2` with the same vCPU and memory counts but more predictable general-purpose performance. That is a
measured follow-up option, not the default.

## [!IMPORTANT] These values might change after load testing on real GCP infrastructure 