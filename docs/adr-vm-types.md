# ADR - VM Types

Status: accepted

Global prerequisites:

- available in `europe-west1`
- no commitment obligations or discounts

## Decision

| VM              | Machine type    |     vCPU | Memory | Boot disk                   |
|-----------------|-----------------|---------:|-------:|-----------------------------|
| Kafka           | `n4-standard-2` |        2 |   8 GB | `hyperdisk-balanced`, 20 GB |
| Core + InfluxDB | `c4d-highcpu-4` |        4 |   7 GB | `hyperdisk-balanced`, 20 GB |
| Analysis        | `e2-micro`      | 2 shared |   1 GB | `pd-balanced`, 20 GB        |

Terraform variables in [`infra/race/variables.tf`](../infra/race/variables.tf) are the implementation of this decision.

## Core + InfluxDB VM

### Requirements

- Core can be compiled for either x86_64 or AArch64, but the current payload pipeline publishes a Linux x86_64 binary.
- InfluxDB 3 Core supports x86_64.
- Core needs predictable CPU capacity and at least two physical cores to avoid making message processing and database
  work compete on one physical core.
- The VM must provide enough memory for both processes, InfluxDB cache, the operating system, and short bursts beyond
  the measured run.

### Rationale

We need more CPU than memory for the current setup. 1:4 ratio is not needed, 1:2 is enough.

N2 is the older generation, E2 does not have physical cores we need. The best choice is either N4 or C4 (more
expensive).

For longer workloads, the cost difference would matter, but for around an estimated 30 hours per month, the difference
is tiny and C4 has better CPU architecture with higher frequency. Core pinning to one thread is not required unless
runtime proves otherwise. Since InfluxDB writes locally and we have buffers in Core, the RAM usage should not be too
large, so we can save by going for `highcpu` variant instead of the standard one.

This would be a bit less than `5.50$/30h` with `20GB` boot disk for `c4d-highcpu-4`, which is quite a good deal. Sadly
we cannot go for spot VM here - otherwise it would be three times cheaper.

The only drawback of C4 is that for CPU native Core compilation, a dedicated builder VM is needed.

Technically, from the latest load testing, picking a 2-core variant with slightly more memory would probably be more cost efficient.

[In pricing calculator](https://cloud.google.com/products/calculator?dl=CjhDaVF3WVRSbU9XSTVNeTAyTTJRNUxUUXdOR1F0WWprellpMHdZbUV4TkRRM1kySmlNekFRQVE9PRAIGiRDQUQ5MTA4MC0zOUVDLTQ2NTYtQjg3OC05QTUxQjY0MzA2Rjg&_gl=1*hmvx65*_up*MQ..&gclid=CjwKCAjwqc_UBhBKEiwAWbl25q5jMtOk9eAmBdh9cuBnOYDnJZJLyO9afG3l6jbu6ZQ56lfLfq_q6BoCYFAQAvD_BwE&gclsrc=aw.ds)

## Kafka VM

Load test evidence ([results](load-test-results.md#resource-usage)) shows Kafka CPU well within two vCPUs, so
`n4-standard-2` is enough.
That would be around `3.30$/30h` with `20GB` balanced hyperdisk boot disk.

If needed, we can go for a highmem variant, which would bump to `4.30$/30h`, so not that much difference. N2 series has
better
JVM performance than E2 series and for the predicted instance lifespan, cost difference is negligible.

## Analysis VM

`e2-micro` is an infrastructure placeholder pending module sizing. It uses `pd-balanced` because E2 does not support Hyperdisk.

[IMPORTANT!] TODO - workload sizing and final machine type.

## Evidence from load testing

See [Load test results](load-test-results.md).
