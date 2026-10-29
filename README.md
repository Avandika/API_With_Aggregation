# Submission API with Real-Time Aggregation Engine

A concurrent backend service written in Go designed to handle high-throughput form submissions with strict validation, idempotency guarantees, and an architectural blueprint for stream analytics using Apache Flink.

## System Performance Architecture

### Part 1 & 2: API Engine & Load Test Results
The backend engine leverages native Go `goroutines` and thread-safe memory allocations to maximize throughput. Data integrity and duplicate write protection are strictly enforced via a localized `sync.RWMutex` tracking registry.

During a benchmarking load of **100,000 asynchronous payloads** containing a 20% duplicate ratio, the system registered the following metrics:

*   **Total Requests Processed:** 100,000
*   **Unique Submissions Stored:** 80,000
*   **Duplicate Submissions Identified/Rejected:** 20,000
*   **Network/Validation Errors:** 0
*   **Local Processing Throughput Rate:** ~9,785 requests/second

---

### Part 3: Real-Time Stream Aggregation with Apache Flink

To process persistent, running counts of form submissions broken down per hour continuously without impeding HTTP API thread performance, the following high-throughput asynchronous pipeline is planned:
```
[ Go Web API ]
│  (Produces validated, unique submission events)
▼
[ Apache Kafka / RabbitMQ Event Broker ]
│  (Partitioned by form_id)
▼
[ Apache Flink Cluster Application ]
│  ├── DataStream.keyBy(record -> record.form_id)
│  └── .window(TumblingEventTimeWindows.of(Time.hours(1)))
│  └── .aggregate(new CountAggregator())
▼
[ Distributed Cache View Layer (e.g., Redis / PostgreSQL) ]
```
#### Fault Tolerance & Crash Recovery Mechanics
If the Flink aggregation node crashes mid-stream, data consistency is strictly protected against omissions or duplication using the following design pillars:

1.  **Exactly-Once Processing Semantics:** Flink leverages the **Chandy-Lamport distributed snapshot algorithm** to create incremental, asynchronous state checkpoints stored to reliable storage (e.g., AWS S3 or HDFS).
2.  **Deterministic Offset Replays:** Upon system boot recovery, the Flink job restores its internal memory to the exact state captured in the last verified checkpoint. It then requests the message broker (e.g., Apache Kafka) to replay the event stream starting precisely at the corresponding uncommitted log offset.
3.  **Idempotent Sink Writes:** The calculated metric increments are committed to the downstream persistence layer via an upsert pattern or two-phase commit protocol, ensuring that restarted stream evaluations never duplicate or skew historical hourly graphs.
