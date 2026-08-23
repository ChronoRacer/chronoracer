
# Deep dive - Core

## Layers

Core can be divided into 3 main layers.

#### Layer 1 - messaging layer

Name: Messaging layer
Purpose: consume and publish messages from Kafka
Main components:
- Consumer for sensor data
- Consumer for control events
- DLQ producer
- Heartbeat producer


#### Layer 2 - processing layer

Name: Processing layer
Purpose: Process the data from messages
Main components:
- receiving pipeline for incoming messages
- process the message, deserialize and transform if needed
- filter component for the control message that decides how to act on the message

#### Layer 3 - persistence layer

Name: Persistence layer
Purpose: handle connection with the database, flush data
Main components:
- batcher collecting data into batches depending on time, batch size and sensor type 
- flusher writing data into the database

#### Layer 4 - Shutdown layer

Name: Shutdown layer
Purpose: Special layer used during shutdown sequence to handle exporting data
Main components:
- kafka exporter
- database exporter
- GCS writer
- Google Pub/Sub publisher


## Data ingestion flow

```mermaid
flowchart TB
    %% Groups
    subgraph kafka["Messaging layer"]
        dataSink["Sensor data sink"]
        dlqPublisher["DLQ Publisher"]
        
    end

    subgraph process["Processing layer"]
        extract["Deserialze from Avro"]
        unwind["Unwind message"]
    end

    subgraph persistance["Persistance layer"]
        splitter["Splitter"]
        batcherFast["Fast batcher"]
        batcherNormal["Normal batcher"]
        flusher[("Flusher")]
    end

    %% Connections
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
    %% Groups
    subgraph kafka["Messaging layer"]
        heartbeatProducer["Heartbeat producer"]
    end
```

### Control event handling

```mermaid
flowchart TB
    %% Groups
    subgraph kafka["Messaging layer"]
        kafkaControl["Control event listener"]
    end

    subgraph process["Processing layer"]
        filterControl["Contol event filter"]
        configureProcess["Adjust configuration"]
    end

       subgraph persistance["Persistance layer"]
        configurePersistance["Adjust configuration"]
    end

    %% Connections
    kafkaControl --> filterControl
    filterControl --> configureProcess
    filterControl --> configurePersistance

```

### Shutdown event handling

```mermaid
flowchart TB
    %% Groups
    subgraph kafka["Messaging layer"]
        kafkaControl["Control event listener"]
    end

    subgraph process["Processing layer"]
        filterControl["Contol event filter"]
    end

    subgraph shutdown["Shutdown layer"]
        dataStop["Stop data ingestion"]
        pipelineDrain["Wait until data pipeline drained"]
        exportKafka["Export Kafka (via Redpanda)"]
        exportDb["Export database"]
        gcsWriter[("Upload exports to GCS")]
        notify["Broadcast shutdown complete"]
    end

    %% Connections
    kafkaControl --> filterControl
    filterControl --> dataStop
    dataStop --> pipelineDrain
    pipelineDrain --> exportKafka
    pipelineDrain --> exportDb
    exportDb --> gcsWriter
    exportKafka --> notify
    gcsWriter --> notify

```