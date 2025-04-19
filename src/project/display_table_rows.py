# src/project/display_table_rows.py

import sys
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Check if table name is provided as an argument
if len(sys.argv) < 2:
    print("Error: Please provide a table name as an argument.")
    sys.exit(1)

# Get the table name from the arguments
table_name = sys.argv[1]

try:
    # Read the table into a DataFrame
    print(f"\n Fetching 10 rows from table: local.db.{table_name}")
    df = spark.read.table(f"local.db.{table_name}")

    # Show the first 10 rows
    df.show(10, truncate=False)
except Exception as e:
    print(f"Error: Unable to fetch rows from table '{table_name}'.\n{e}")
    print("Please ensure the table exists and is accessible.")