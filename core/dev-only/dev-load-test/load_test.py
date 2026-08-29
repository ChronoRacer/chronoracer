import io
import json
import random
import threading
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

schema_path = Path(__file__).resolve().parents[2] / 'avro' / 'sensor_data.avsc'
PARSED_SCHEMA = parse_schema(json.loads(schema_path.read_text()))
CONFIG = json.loads((Path(__file__).resolve().parent / 'config.json').read_text())
SENSOR_IDS = CONFIG["sensor_ids"]

def generate_sensor_batch(base_timestamp_ns):
    num_readings = random.randint(1, 10)
    readings = []

    current_delta = 0
    for _ in range(num_readings):
        max_step = (2_000_000_000 - current_delta) // (num_readings - len(readings) + 1)
        if max_step <= 0:
            break

        step = random.randint(1, max_step)
        current_delta += step

        readings.append({
            "delta": current_delta,
            "value": round(random.uniform(10.0, 100.0), 4)
        })

    return {
        "sensor_id": random.choice(SENSOR_IDS),
        "timestamp": base_timestamp_ns,
        "readings": readings
    }

def serialize_record(record):
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, PARSED_SCHEMA, record)
    return bytes_writer.getvalue()

def delivery_report(err, msg):
    """Callback triggered by Kafka on successful or failed delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")

def produce_sensor_messages(producer):
    """Thread function for producing Avro sensor messages to random topics."""
    topics = CONFIG["sensor_topics"]
    count = CONFIG["batch_count"]
    total_time = CONFIG["testing_time"]

    delay_between_messages = total_time / count if count > 0 else 0
    current_timestamp_ns = time.time_ns()

    for i in range(count):
        loop_start = time.monotonic()

        current_timestamp_ns += random.randint(1_000_000, 10_000_000)

        record = generate_sensor_batch(current_timestamp_ns)
        avro_bytes = serialize_record(record)
        target_topic = random.choice(topics)

        producer.produce(
            topic=target_topic,
            value=avro_bytes,
            on_delivery=delivery_report,
            key=record["sensor_id"]
        )

        if i % 5000 == 0:
            producer.poll(0)

        elapsed_in_loop = time.monotonic() - loop_start
        sleep_time = delay_between_messages - elapsed_in_loop
        if sleep_time > 0:
            time.sleep(sleep_time)

def produce_change_driver_messages(producer):
    """Thread function for producing change_driver messages."""
    change_driver = CONFIG.get("change_driver", {})
    if not change_driver.get("enabled", False):
        return

    count = change_driver.get("count", 0)
    if count <= 0:
        return

    init_delay = change_driver.get("initial_delay", 0.0)
    topic = change_driver.get("topic", "control")
    total_time = CONFIG["testing_time"]

    print(f"[Control] Sleeping for initial delay of {init_delay} seconds...")
    time.sleep(init_delay)

    active_time = total_time - init_delay
    if active_time <= 0:
        print("[Control] Warning: Testing time is shorter than initial delay. Sending all at once.")
        delay_between_messages = 0
    else:
        delay_between_messages = active_time / count

    print(f"[Control] Starting to send {count} messages to '{topic}'...")

    for _ in range(count):
        loop_start = time.monotonic()

        payload = str(uuid.uuid4().hex).encode('utf-8')

        producer.produce(
            topic=topic,
            value=payload,
            on_delivery=delivery_report,
            key="CHDRV"
        )
        producer.poll(0)

        elapsed_in_loop = time.monotonic() - loop_start
        sleep_time = delay_between_messages - elapsed_in_loop
        if sleep_time > 0:
            time.sleep(sleep_time)

    print("[Control] Finished sending control messages.")

def produce_shutdown_message(producer):
    shutdown = CONFIG.get("shutdown", {})
    if not shutdown.get("enabled", False):
        return


    topic = shutdown.get("topic", "control")
    payload = ""

    producer.produce(
        topic=topic,
        value=payload,
        on_delivery=delivery_report,
        key="SHUTDOWN"
    )
    producer.poll(0)

    print("[Control] Sent shutdown message.")

def main():
    global SENSOR_IDS
    
    producer_conf = {
        'bootstrap.servers': CONFIG["broker"],
        'linger.ms': 5,
        'batch.num.messages': 300,
        'queue.buffering.max.messages': 10000,
        'compression.type': 'lz4'
    }

    producer = Producer(producer_conf)
    chdrv_enabled = CONFIG.get("change_driver", {}).get("enabled", False)
    shutdown_enabled = CONFIG.get("shutdown", {}).get("enabled", False)

    print(f"Starting load test configuration:")
    print(f"- Total Time: {CONFIG['testing_time']}s")
    print(f"- Batch Count: {CONFIG['batch_count']}")
    print(f"- CHDRV enabled: {chdrv_enabled}")
    print(f"- SHUTDOWN enabled: {shutdown_enabled}")
    

    start_time = time.monotonic()

    sensor_thread = threading.Thread(target=produce_sensor_messages, args=(producer,))
    sensor_thread.start()
    if chdrv_enabled:
        control_thread = threading.Thread(target=produce_change_driver_messages, args=(producer,))
        control_thread.start()

    sensor_thread.join()
    if chdrv_enabled:
        control_thread.join()

    print("Generation complete")
    produce_shutdown_message(producer)
    producer.flush()

    actual_time = time.monotonic() - start_time
    print(f"Load test complete in {actual_time:.2f} seconds.")

if __name__ == "__main__":
    main()