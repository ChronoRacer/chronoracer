# Analysis

For documentation, see the [main document](../docs/component-analysis.md).

## Related documentation

- VM image, payload publication, and boot process: [Analysis startup](startup/README.md) and [Compute images](../docs/component-images.md).
- Artifact bucket and sensor/rule objects: [GCS artifacts](../docs/component-gcs_buckets.md#artifacts-bucket).
- Race VM metadata, IAM, and secrets: [Terraform race stack](../docs/component-terraform.md#race-stack).
- Incoming Core `Watch` stream: [Core telemetry stream](../docs/component-core.md#telemetry-stream) and its [protocol decision](../docs/adr-telemetry-stream.md).
- VM sizing: [Analysis VM decision](../docs/adr-vm-types.md#analysis-vm).

## Tests

From the repository root, install the development dependencies and run the unit tests:

```sh
python -m pip install -r analysis/requirements-dev.txt
python -m pytest analysis/tests
```

## Rule configuration validation

From a source checkout with access to the artifacts bucket, validate the stored rules against the production
evaluation path and check their decision-table coverage:

```sh
python -m analysis.rules.test.rule_test
```

For local rule and sensor files, see the options in [`rule_test.py`](rules/test/rule_test.py).

## Local end-to-end run

See the [local run guide](dev/README.md) for starting Core, Analysis, Telemetry, and delayed synthetic track data.
