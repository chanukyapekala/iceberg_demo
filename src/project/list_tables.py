from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

namespace = "local.db"
log_path = "logs/list_tables.log"

tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
tables = tables_df.collect()

with open(log_path, "w") as log:
    log.write(f"Tables in namespace `{namespace}`:\n\n")
    for row in tables:
        log.write(f"- {row.tableName}\n")

print(f"Tables listed in {log_path}")
