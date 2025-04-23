# Table Formats Comparison

| Feature | Delta Lake | Apache Iceberg | Apache Hudi | Apache Hive |
|---------|------------|----------------|-------------|-------------|
| **Version (as of Apr 2025)** | 3.0.0 | 1.5.0 | 0.15.0 | 3.1.3 |
| **ACID Transactions** | Yes - Delta Log + JSON | Yes - Snapshot isolation | Yes - Optimistic concurrency | Limited - Directory-based locking |
| **Schema Evolution** | Yes - Add/drop/rename/reorder columns | Yes - Add/drop/rename/reorder/type promotion | Yes - Add/drop/rename columns | Limited - Append-only schema changes |
| **Time Travel** | Yes - History of changes (time/version) | Yes - Data versioning (time/snapshot ID) | Yes - Incremental queries with commit time | No |
| **Partition Evolution** | Limited - Requires rewriting data | Yes - Dynamic partition evolution | Yes - Flexible partition strategies | No - Static partitioning |
| **Incremental Processing** | Batch and Incremental (Change Data Feed) | Batch and Incremental (row-level deltas) | Batch and Incremental (built-in CDC) | Batch only |
| **Query Performance** | Good - Optimized for Databricks | Excellent - Statistics and metadata filtering | Good - Pre-computed indexes | Moderate - Limited optimization |
| **Metadata Handling** | Centralized - Single JSON file | Distributed - Metafiles in hierarchical structure | Distributed - Timeline and commit metadata | Centralized - External metastore |
| **Cloud Storage Integration** | Excellent - Optimized for object stores | Excellent - Native object store support | Good - Works with S3/ADLS/GCS | Limited - Requires connectors |
| **Processing Engine Compatibility** | Spark, Limited Flink/Presto | Spark, Flink, Presto, Trino, Hive | Spark, Presto, Hive, Flink | Hive, Spark, Presto |
| **Data Governance Features** | Good - Lineage via Databricks | Excellent - Built-in data lineage | Good - Record-level metadata | Basic - External tools required |
| **Primary Focus** | Large scale analytics, Data lakehouse | Large scale analytics, Partition management | Real-time data ingestion, Incremental processing | Batch processing, Data warehousing |
| **Base File Format** | Parquet | Parquet, ORC, Avro | Parquet, ORC | Parquet, ORC, AVRO, Text, etc. |
| **Major Platform Providers** | Databricks | Snowflake, AWS (Athena), GCP (BigLake) | OneHouse, Uber | Amazon EMR, Google Dataproc |
| **Community Adoption** | High - Databricks ecosystem | Very High - Multi-vendor support | High - Growing adoption | Very High - Legacy installations |
| **When Not to Use** | Non-Spark environments, Multi-engine setups | Real-time data ingestion, Low latency requirements | High concurrency writes, Simple read-only workloads | Any ACID requirements, Time travel needs |
| **Openness** | Databricks preferred (open source with ecosystem lock-in) | Open - Vendor neutral Apache project | Open - Vendor neutral Apache project | Open - Vendor neutral Apache project |
| **Typical Use Case** | Data lakehouse, Unified analytics | Data lakes with diverse query engines | Near real-time analytics, Upsert-heavy workloads | Traditional data warehousing |
| **Concurrency Control** | Optimistic - Multiple readers, single writer | Optimistic - Multiple readers, multiple writers | Optimistic - Multiple readers, multiple writers | Pessimistic - Directory locking |
| **Small File Handling** | Auto compaction | Table maintenance API | Auto compaction with cleaning | Manual compaction required |
