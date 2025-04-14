from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

table = "local.db.basic_table"
warehouse_dir = "/Users/chanukya/GIT/iceberg_warehouse"
log_path = "logs/metadata_changes.log"

df = spark.read.table(table)
snapshots = spark.sql("SELECT * FROM local.db.basic_table.snapshots").show(truncate=False)

with open(log_path, "w") as log:
    log.write(f"Snapshots for table: {table}\n")
    while snapshots.hasNext():
        snapshot = snapshots.next()
        log.write(f"- Snapshot ID: {snapshot.snapshotId()}\n")
        log.write(f"  Timestamp  : {snapshot.timestampMillis()}\n")
        log.write(f"  ManifestList: {snapshot.manifestListLocation()}\n\n")

print(f"Metadata info written to {log_path}")
