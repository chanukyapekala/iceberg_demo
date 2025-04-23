# Apache Iceberg Demo Project

This project demonstrates key Apache Iceberg features using PySpark and Hadoop catalog, including:

## Features
- **Schema Evolution**: Add/drop/rename/reorder columns and type promotion
- **Partition Evolution**: Dynamic partition management
- **SCD Type 2**: Slowly Changing Dimension implementation
- **Time Travel**: Data versioning with snapshot IDs
- **Machine Learning**: Linear regression with Iceberg tables
- **Streaming**: Batch and incremental processing demos

## Quick Start
```bash
# Load sample data
make run-load-sample-data

# Run feature demos
make run-schema-evolution
make run-partitioning
make run-scd-type-2
make run-time-travel-feature

# Run ML pipeline
make run-linear-regression

# Run streaming demos
make run-producer
make run-consumer
make run-structured-streaming
```

### Airflow integration
```bash 
# Run DAGs
make run-batch-writer-airflow-dag
make run-stream-writer-airflow-dag
make run-ml-writer-airflow-dag

# and stop airflow
make stop-airflow
```