# 🛒 Super Cash Stream – Exercise
## Objective
Build a real-time data pipeline for a supermarket cash flow system using Apache Kafka. The goal is to ingest events from multiple cash registers, stream them through Kafka, and process them to produce hourly statistics.

## Requirements

1. Event Source
Each cash register sends events in real time with the following fields:

- timestamp (ISO format)
- shop_id
- order_id
- product_id
- quantity
- unit price

Example event:
```json
{  "timestamp": "2025-10-30T22:55:53Z",  "shop_id": "S123",  "order_id": "O98765",  "product_id": "P456",  "quantity": 2,  "price": 4.99}
```

2. Ingestion System

Accept events from registers.
Send them to a Kafka topic (e.g., super-cash-stream).

3. Streaming System

Use Kafka Streams to process events.
Compute real-time stats on a 1-hour tumbling window, such as:

- Total revenue per shop.
- Total quantity sold per product.

```mermaid
flowchart LR
    A[Cash Registers] -->|JSON Events| B[Kafka Topic: super-cash-stream]
    B --> C[Kafka Streams App]
    C --> D[Hourly Stats Store]
    C --> E[ksqlDB for Queries]
```

Bonus

Add a ksqlDB query to perform the same aggregation using SQL-like syntax.
