# src/iceberg_demo/partitioning.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, months

spark = SparkSession.builder.getOrCreate()

# Evolve the schema
spark.sql("ALTER TABLE local.db.dim_users ADD COLUMN start_date date")

# Create DataFrame with proper date conversion
df = spark.createDataFrame([
    (6, "Alice", 25, "2025-01-01"),
    (7, "Bob", 30, "2025-01-01"),
    (8, "Charlie", 35, "2025-03-01")
], ["id", "name", "age", "start_date"])

df = df.withColumn("start_date", to_date(col("start_date")))

# Append data with month partitioning
df.writeTo("local.db.dim_users") \
    .using("iceberg") \
    .partitionedBy("start_date") \
    .append()