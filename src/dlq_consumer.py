import json
import sys

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

from kafka import KafkaConsumer


# -----------------------------
# Kafka DLQ Consumer
# -----------------------------
consumer = KafkaConsumer(
    "orders.DLQ",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="dlq-consumer-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


# -----------------------------
# Start DLQ consumer
# -----------------------------
print("DLQ Consumer started...")
print("Waiting for failed orders...\n")


try:

    for message in consumer:

        order = message.value

        print("Failed order received from DLQ")
        print(f"Order ID : {order['orderId']}")
        print(f"Product  : {order['product']}")
        print(f"Price    : {order['price']}")
        print("-" * 40)

except KeyboardInterrupt:
    print("\nDLQ consumer stopped.")

finally:
    consumer.close()