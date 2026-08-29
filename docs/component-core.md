# Deep dive - Core

Code at: [Core root](../core)

Where: one paid VM. Core and InfluxDB 3 Core (self-managed, single node) share that VM.

Shared VM pain points: Core and database share similar load spikes which means that VM usage can spike greatly.

Lifecycle: race-scoped. Created at the start. After export succeeds, VM and disk are destroyed.

Access: private

Core responsibility:

- ingest data from Kafka
- process, validate data
- batch and write to the database
- serve as a buffer for the database
- handle shutdown sequence export flow (for both database and Kafka)

## Startup

Boot scripts: [Core startup](../core/startup/README.md). Influx on this VM: [InfluxDB](component-influxdb.md). Image and
payload: [Compute images](component-images.md).

## Layers

Core can be divided into 3 main layers.

#### Layer 1 - messaging layer

Name: Messaging layer Purpose: consume and publish messages from Kafka Main components:

- Consumer for sensor data
- Consumer for control events
- DLQ producer
- Heartbeat producer

#### Layer 2 - processing layer

Name: Processing layer Purpose: Process the data from messages Main components:

- receiving pipeline for incoming messages
- process the message, deserialize and transform if needed
- filter component for the control message that decides how to act on the message

#### Layer 3 - persistence layer

Name: Persistence layer Purpose: handle connection with the database, flush data Main components:

- batcher collecting data into batches depending on time, batch size and sensor type
- flusher writing data into the database

#### Layer 4 - Shutdown layer

Name: Shutdown layer Purpose: Special layer used during shutdown sequence. Includes exporting data Main components:

- Kafka exporter
- database exporter
- GCS writer
- Google Pub/Sub publisher

## Data ingestion flow

```mermaid
flowchart TB
    subgraph kafka["Messaging layer"]
        dataSink["Sensor data sink"]
        dlqPublisher["DLQ Publisher"]

    end

    subgraph process["Processing layer"]
        extract["Deserialize from Avro"]
        unwind["Unwind message"]
    end

    subgraph persistence["Persistence layer"]
        splitter["Splitter"]
        batcherFast["Fast batcher"]
        batcherNormal["Normal batcher"]
        flusher[("Flusher")]
    end

    dataSink --> extract
    extract --> unwind
    extract --> dlqPublisher
    unwind --> dlqPublisher
    unwind --> splitter
    splitter --> batcherFast
    splitter --> batcherNormal
    batcherFast --> flusher
    batcherNormal --> flusher

```

### Core heartbeat

```mermaid
flowchart TB
    subgraph kafka["Messaging layer"]
        heartbeatProducer["Heartbeat producer"]
    end
```

### Control event handling

```mermaid
flowchart TB
    subgraph kafka["Messaging layer"]
        kafkaControl["Control event listener"]
    end

    subgraph process["Processing layer"]
        filterControl["Control event filter"]
        configureProcess["Adjust configuration"]
    end

    subgraph persistence["Persistence layer"]
        configurePersistence["Adjust configuration"]
    end

    kafkaControl --> filterControl
    filterControl --> configureProcess
    filterControl --> configurePersistence

```

### Shutdown event handling

```mermaid
flowchart TB
    subgraph kafka["Messaging layer"]
        kafkaControl["Control event listener"]
    end

    subgraph process["Processing layer"]
        filterControl["Control event filter"]
    end

    subgraph shutdown["Shutdown layer"]
        dataStop["Stop data ingestion"]
        pipelineDrain["Wait until data pipeline drained"]
        exportKafka[("Export Kafka (via Redpanda)")]
        exportDb["Export database"]
        gcsWriter[("Upload exports to GCS")]
        notify["Broadcast export done"]
    end

    kafkaControl --> filterControl
    filterControl --> dataStop
    dataStop --> pipelineDrain
    pipelineDrain --> exportKafka
    pipelineDrain --> exportDb
    exportDb --> gcsWriter
    exportKafka --> notify
    gcsWriter --> notify
```

Successful shutdown sequence means sending the export-done event to Google Pub/Sub. Failure means not sending the event
and thus forcing either operator to invoke the force-destroy flow after optional manual intervention.

Race stop sequence around this: [Race destruction](flow-race_destruction.md). Export
prefix: [GCS](component-gcs_buckets.md).

#### Kafka export

Kafka data export and GCS upload are handled via Redpanda templates rendered during
runtime: [Redpanda templates](../core/redpanda/README.md). Topics to be exported are defined in the Core configuration.
Usually just DLQ, but any number of topics can be included. Behaviour when trying to export a non-existing topic is not
tested and thus undefined.

Whether Kafka export failure marks the shutdown sequence as failed is defined in the Core configuration.

#### InfluxDB export

InfluxDB export handled via InfluxDB plugin: [plugin directory](../core/influxdb-plugins/README.md). After exporting
files are uploaded to GCS.

Whether InfluxDB export failure marks the shutdown sequence as failed is defined in the Core configuration.
