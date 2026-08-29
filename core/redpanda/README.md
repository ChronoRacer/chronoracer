# Redpanda configuration files

Files inside this directory are configuration files or templates for Redpanda CLI.

## `export.gcs.yaml`, `export.local.yaml`

- Type: template
- Caller: Core
- When: Core shutdown sequence
- Purpose: export chosen Kafka topics into parquet files
- Why: handling the export, parsing and compression flow manually is error-prone and tedious  

`export.gcs.yaml`: GCS version will upload to the GCS bucket specified in the template.

`export.local.yaml`: Local version will copy to a directory on the machine that invoked the export flow.
