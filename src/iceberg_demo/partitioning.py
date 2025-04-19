# src/project/partitioning.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Alice", "2023-01-01"),
    (2, "Bob", "2023-02-01")
], ["id", "name", "event_date"])

df.writeTo("local.db.partitioned_table") \
    .using("iceberg") \
    .partitionedBy("event_date") \
    .createOrReplace()

check_df = spark.read.table("local.db.partitioned_table")
check_df.show(10, False)