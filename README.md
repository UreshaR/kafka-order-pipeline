# Kafka Avro Orders

A Kafka-based order processing pipeline with Avro serialization, real-time
price aggregation, retry logic for transient failures, and a Dead Letter
Queue (DLQ) for messages that never succeed.

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Order schema](#order-schema-schemasordeavsc)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running](#running)
- [Sample output](#sample-output)
- [Project structure](#project-structure)

## Features

- [x] Avro serialization/deserialization, shared schema file between producer and consumer
- [x] Real-time aggregation — running average of `price` across processed orders
- [x] Retry logic for transient failures — up to 3 attempts with a delay between each
- [x] Dead Letter Queue — orders that exhaust all retries are routed to `orders.DLQ`

## Architecture

```
producer.py -> [ orders topic ] -> consumer.py -> [ orders.DLQ topic ] -> dlq_consumer.py
```

| Component | Role |
|---|---|
| `producer.py` | Continuously generates random orders (`orderId`, `product`, `price`), serializes them with Avro, and publishes them to the `orders` topic. |
| `consumer.py` | Reads from `orders`, deserializes each message, and processes it while maintaining a running average of `price`. Each attempt has a simulated 25% chance of failing (a stand-in for a flaky downstream dependency). Failures are retried up to 3 times; an order that still fails after all retries is published to `orders.DLQ`. |
| `dlq_consumer.py` | Reads and prints whatever lands in `orders.DLQ`, so permanently failed orders stay visible instead of silently dropping. |

Retry vs. permanent failure isn't decided in advance — every order is
subject to the same simulated failure chance, and whether it counts as
"temporary" or "permanent" only becomes clear from how many attempts it
takes: most failing orders succeed within 3 attempts (temporary), while a
small fraction (statistically, `FAILURE_PROBABILITY^3` of all orders) never
recover and end up in the DLQ (permanent).

## Order schema ([schemas/order.avsc](schemas/order.avsc))

| Field | Type | Description |
|---|---|---|
| `orderId` | string | Unique identifier for the order |
| `product` | string | Name of the purchased item |
| `price` | float | Price of the product |

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

**1. Start Kafka** (KRaft mode, no Zookeeper needed):

```bash
docker compose up -d
```

**2. Start the DLQ consumer, the main consumer, and the producer**, each in
its own terminal:

```bash
python src/dlq_consumer.py
python src/consumer.py
python src/producer.py
```

**3. Watch.** `consumer.py`'s output shows the running average, retry
attempts, and DLQ hand-offs live. Stop any of the three with `Ctrl+C`.

**4. Optional — AKHQ.** A Kafka web UI is also started by `docker compose
up` and available at [http://localhost:8080](http://localhost:8080), for
browsing the `orders` and `orders.DLQ` topics visually.

## Sample output

```
Received order: {'orderId': '1002', 'product': 'Monitor', 'price': 1226.68}
Processing attempt 1/3
Attempt 1 failed: Simulated transient downstream failure
Retrying in 1 second...
Processing attempt 2/3
Order processed: 1002 | Product: Monitor | Price: 1226.68 | Running Average: 1528.15

Received order: {'orderId': '1089', 'product': 'Phone', 'price': 897.33}
Processing attempt 1/3
Attempt 1 failed: Simulated transient downstream failure
Retrying in 1 second...
Processing attempt 2/3
Attempt 2 failed: Simulated transient downstream failure
Retrying in 1 second...
Processing attempt 3/3
Attempt 3 failed: Simulated transient downstream failure
Order sent to DLQ: {'orderId': '1089', 'product': 'Phone', 'price': 897.33}
```

## Project structure

```
schemas/order.avsc      Avro schema shared by producer and consumer
src/producer.py         Generates and publishes random orders
src/consumer.py         Aggregates prices, retries failures, forwards to DLQ
src/dlq_consumer.py     Reads and prints permanently failed orders
docker-compose.yml      Kafka (KRaft mode) + AKHQ
requirements.txt        Python dependencies (kafka-python, fastavro)
```
