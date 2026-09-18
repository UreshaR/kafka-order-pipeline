# Kafka Avro Orders

A Kafka-based order processing pipeline with Avro serialization, real-time
price aggregation, retry logic for transient failures, and a Dead Letter
Queue (DLQ) for messages that never succeed.

## Architecture

```
producer.py -> [ orders topic ] -> consumer.py -> [ orders.DLQ topic ] -> dlq_consumer.py
```

- **`producer.py`** continuously generates random orders (`orderId`,
  `product`, `price`), serializes them with Avro, and publishes them to the
  `orders` topic.
- **`consumer.py`** reads from `orders`, deserializes each message, and
  processes it while maintaining a running average of `price` across all
  successfully processed orders. Each processing attempt has a simulated
  25% chance of failing (mimicking a flaky downstream dependency). Failed
  orders are retried up to 3 times with a short delay between attempts. An
  order that still fails after all retries is published to `orders.DLQ`.
- **`dlq_consumer.py`** reads and prints whatever lands in `orders.DLQ`, so
  permanently failed orders are visible instead of silently dropped.

Retry vs. permanent failure isn't decided in advance — every order is
subject to the same simulated failure chance, and whether it counts as
"temporary" or "permanent" only becomes clear from how many attempts it
takes: most failing orders succeed within 3 attempts (temporary), while a
small fraction (statistically, `FAILURE_PROBABILITY^3` of all orders) never
recover and end up in the DLQ (permanent).

## Order schema ([schemas/order.avsc](schemas/order.avsc))

| Field     | Type   | Description                              |
|-----------|--------|-------------------------------------------|
| orderId   | string | Unique identifier for the order           |
| product   | string | Name of the purchased item                |
| price     | float  | Price of the product                      |

## Prerequisites

- Docker (for running Kafka)
- Python 3.10+

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

## Running

1. Start Kafka (KRaft mode, no Zookeeper needed):

   ```bash
   docker compose up -d
   ```

2. In separate terminals, start the DLQ consumer, the main consumer, and
   the producer (order matters only in that the consumers should be up
   before you care about seeing their output, since Kafka retains messages
   either way):

   ```bash
   python src/dlq_consumer.py
   python src/consumer.py
   python src/producer.py
   ```

3. Watch `consumer.py`'s output for the running average, retry attempts,
   and DLQ hand-offs. Stop any of the three with `Ctrl+C`.

4. Optional: AKHQ (a Kafka web UI) is also started by `docker compose up`
   and is available at [http://localhost:8080](http://localhost:8080) for
   browsing the `orders` and `orders.DLQ` topics visually.

## Project structure

```
schemas/order.avsc      Avro schema shared by producer and consumer
src/producer.py         Generates and publishes random orders
src/consumer.py         Aggregates prices, retries failures, forwards to DLQ
src/dlq_consumer.py     Reads and prints permanently failed orders
docker-compose.yml      Kafka (KRaft mode) + AKHQ
requirements.txt        Python dependencies (kafka-python, fastavro)
```
