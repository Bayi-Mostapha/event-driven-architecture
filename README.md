# Real-Time Event-Driven Streaming Architecture: Reference Guide

This document outlines an end-to-end real-time processing pipeline built using Docker, Apache Kafka, Kafka Streams, Spring Boot components (controllers, handlers, suppliers), and downstream analytics/visualization sinks.

---

## 1. Infrastructure & Core Setup

### Cluster Initialization
Spin up the local containerized environment containing Zookeeper/KRaft brokers and associated streaming infrastructure:
```bash
docker compose up -d
```

### Kafka Message Consumption (CLI)
To read raw events flowing through a specific topic (e.g., topic `R2`) directly from the broker container:
```bash
docker exec --interactive --tty broker kafka-console-consumer \
  --bootstrap-server broker:9092 \
  --topic R2
```

### Kafka Message Production (CLI)
To inject messages manually into topic `R2` via the interactive console producer:
```bash
docker exec --interactive --tty broker kafka-console-producer \
  --bootstrap-server broker:9092 \
  --topic R2
```

To consume and replay all historical messages from the very beginning of the topic log:
```bash
docker exec --interactive --tty broker kafka-console-consumer \
  --bootstrap-server broker:9092 \
  --topic R2 \
  --from-beginning
```

---

## 2. Microservice Application Layers

The application code is structured across standard architectural layers to ingest, handle, and publish streaming payloads.

### Controllers & Endpoints
* **Role:** Exposes RESTful or reactive web interfaces to ingest client requests, trigger upstream events, or query current state stores.
* **Implementation Focus:** Validates incoming payloads and delegates execution down to event suppliers and handlers.

### Handlers & Business Logic
* **Role:** Encapsulates core processing logic and message parsing rules.
* **Implementation Focus:** Translates raw DTOs into event models, coordinates error handling, and manages state transitions before publishing to Kafka.

### Suppliers & Event Producers
* **Role:** Continuously generate or poll data sources to feed the streaming pipeline.
* **Implementation Focus:** Acts as the entry point for raw telemetry or transactional feeds directed into initial ingestion topics.

---

## 3. Kafka Streams Processing Topology (`KStream`)

The Kafka Streams application processes continuous event streams, transforming intermediate topics (`R3`, `R4`) through stateful operators.

### Core Processing Flow
1. **Source & Transformation (`R3` & `R4`):** Consumes raw events from upstream topics, filtering and mapping them into strongly-typed stream records.
2. **Key Grouping (`groupByKey`):** Re-partitions the stream based on record keys to ensure all events for a given entity aggregate correctly onto the same stream partition.
3. **Time-Windowing (`windowedBy`):** Applies tumbling, sliding, or session windows to bound calculations over specific time intervals (e.g., aggregations per 5-minute window).
4. **Analytics Aggregation:** Computes metrics (such as counts, sums, moving averages, or anomaly flags) across windowed key-value pairs.

---

## 4. Downstream Sinks & Visualization

Once stream aggregations are completed, the resulting metrics are pushed to persistence layers or analytics endpoints for operational monitoring.

* **Console Sinks:** Real-time logging of processed stream outputs via Kafka console consumers for debugging and audit purposes.
* **Analytics & Metrics Store:** Storage layer where aggregated window results are indexed for fast querying.
* **Visualization Layer:** Dashboards and user interfaces rendering real-time graphs, charts, and system telemetry based on stream outputs.