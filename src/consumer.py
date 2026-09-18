import json
import random
import sys
import time
from io import BytesIO

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

from kafka import KafkaConsumer, KafkaProducer
from fastavro import parse_schema, schemaless_reader


# -----------------------------
# Load Avro schema
# -----------------------------
with open("schemas/order.avsc", "r") as file:
    schema = json.load(file)

parsed_schema = parse_schema(schema)


# -----------------------------
# Avro deserialization
# -----------------------------
def deserialize_order(data):
    bytes_reader = BytesIO(data)
    return schemaless_reader(bytes_reader, parsed_schema)


# -----------------------------
# Kafka Consumer
# -----------------------------
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="order-consumer-group"
)


# -----------------------------
# Kafka Producer for DLQ
# -----------------------------
dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)


# -----------------------------
# Running average variables
# -----------------------------
total_price = 0
order_count = 0


# -----------------------------
# Retry settings
# -----------------------------
MAX_RETRIES = 3
RETRY_DELAY = 1

# Simulated chance that a single processing attempt fails,
# mimicking a flaky downstream dependency.
FAILURE_PROBABILITY = 0.25


# -----------------------------
# Send failed message to DLQ
# -----------------------------
def send_to_dlq(order):
    dlq_producer.send(
        "orders.DLQ",
        value=json.dumps(order).encode("utf-8")
    )

    dlq_producer.flush()

    print("Order sent to DLQ:", order)


# -----------------------------
# Process order
# -----------------------------
def process_order(order):

    global total_price
    global order_count

    # Simulate a flaky downstream dependency on each attempt.
    # Orders that keep failing past MAX_RETRIES are effectively
    # "permanent" failures and get routed to the DLQ.
    if random.random() < FAILURE_PROBABILITY:
        raise Exception("Simulated transient downstream failure")

    # -----------------------------
    # Normal processing
    # -----------------------------
    price = order["price"]

    total_price += price
    order_count += 1

    running_average = total_price / order_count

    print(
        f"Order processed: "
        f"{order.get('orderId')} | "
        f"Product: {order.get('product')} | "
        f"Price: {price} | "
        f"Running Average: {running_average:.2f}"
    )

# -----------------------------
# Main consumer loop
# -----------------------------
print("Consumer started...")
print("Waiting for orders...\n")


try:

    for message in consumer:

        # Deserialize Avro message
        order = deserialize_order(message.value)

        print("\nReceived order:", order)

        success = False

        # -----------------------------
        # Retry processing
        # -----------------------------
        for attempt in range(1, MAX_RETRIES + 1):

            try:

                print(
                    f"Processing attempt "
                    f"{attempt}/{MAX_RETRIES}"
                )

                process_order(order)

                success = True
                break

            except Exception as error:

                print(
                    f"Attempt {attempt} failed: "
                    f"{error}"
                )

                if attempt < MAX_RETRIES:

                    print(
                        f"Retrying in "
                        f"{RETRY_DELAY} second..."
                    )

                    time.sleep(RETRY_DELAY)

        # -----------------------------
        # Send to DLQ if all retries fail
        # -----------------------------
        if not success:
            send_to_dlq(order)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
    dlq_producer.close()