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

