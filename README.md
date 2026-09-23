## End-to-End-coinbase-streaming-pipeline

A real-time big data pipeline that ingests live Coinbase edit events, moves them through a streaming architecture, persists the raw stream to a data lake, and produces real-time analytics on a live dashboard.

The pipeline ingests Coinbase's public event stream, transports it through **Apache Flume** and **Apache Kafka**, archives the raw events in **HDFS**, processes them in real time with **Apache Spark Structured Streaming**, stores the resulting metrics in **Hbase**

## Architecture

```
coinbase recentchange API
        │
        ▼
  Flume Agent #1  (exec coinbase.py → memory channel → Kafka sink)
        │
        ▼
     Kafka topic: topic
        │
        ├──────────────────────────────┐
        ▼                              ▼
  Flume Agent #2                  Spark Structured
  (Kafka source → HDFS sink)      Streaming (real-time
        │                          processing)
        ▼                              │
       HDFS                            ▼
   (raw data lake)                 HBase

```

