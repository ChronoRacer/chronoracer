# Load testing

Load testing script. Will generate Kafka messages with data and control events according to the configuration provided.

- `batch_count` - the amount of sensor data batches to send (each batch has from 1 to 15 readings)
- `testing_time` - time in seconds for sending the batches

Essentially, it will try to send `batch_count` batches over `testing_time` seconds. Kafka might throttle with high
throughput, so the effective testing time might be longer than the provided value.

500000 batches on average (points in batch random) take up approximately 250MB of Kafka storage.

Optionally, shutdown and change driver messages can also be sent.

Sample run output:

```terminaloutput
Starting load test configuration:
- Total Time: 30.0s
- Batch Count: 500000
- CHDRV enabled: True
- SHUTDOWN enabled: True
- OAuth enabled: False
[Control] Sleeping for initial delay of 1.0 seconds...
[Control] Starting to send 2 messages to 'control'...
[Control] Finished sending control messages.
Generation complete
[Control] Sent shutdown message.
Load test complete in 243.82 seconds.
```

Change configuration in [config file](config.json)

For a remote Kafka cluster using OAuth, set additional OAuth properties and enable the toggle.

Run with `python3 load_test.py`
