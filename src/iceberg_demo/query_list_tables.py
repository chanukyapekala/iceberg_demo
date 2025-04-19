# src/project/query_list_tables.py

import sys
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Optional table name from args
table_name = sys.argv[1] if len(sys.argv) > 1 else None

if table_name:
    print(f"\n Querying table: local.db.{table_name}")
    df = spark.read.table(f"local.db.{table_name}")
    df.show()
else:
    print("\n Listing tables in catalog `local.db`:")
    spark.sql("SHOW TABLES IN local.db").show()

