import json
import random
import time
from io import BytesIO

from kafka import KafkaProducer
from fastavro import parse_schema, schemaless_writer


# -----------------------------
# Load Avro schema
# -----------------------------
with open("schemas/order.avsc", "r") as file:
    schema = json.load(file)

parsed_schema = parse_schema(schema)


# -----------------------------
# Avro serialization function
# -----------------------------
def serialize_order(order):
    bytes_writer = BytesIO()

    schemaless_writer(
        bytes_writer,
        parsed_schema,
        order
    )

    return bytes_writer.getvalue()


# -----------------------------
# Create Kafka producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)


# -----------------------------
# Generate and send orders
# -----------------------------
products = [
    "Laptop",
    "Phone",
    "Keyboard",
    "Mouse",
    "Monitor"
]


try:
    order_number = 1001

    # -----------------------------
    # Continuously send random orders
    # -----------------------------
    while True:

        order = {
            "orderId": str(order_number),
            "product": random.choice(products),
            "price": round(random.uniform(100.0, 2000.0), 2)
        }

        serialized_order = serialize_order(order)

        producer.send(
            "orders",
            value=serialized_order
        )

        producer.flush()

        print(
            f"Sent order: "
            f"{order['orderId']} | "
            f"{order['product']} | "
            f"${order['price']:.2f}"
        )

        order_number += 1

        time.sleep(1)


except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.close()