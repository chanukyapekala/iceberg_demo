import os
import pandas as pd
from pyspark.sql import SparkSession

# Define the streaming source directory and file
streaming_source = os.path.join(os.getcwd(), "src/project/streaming/data")
file_name = "streaming_records.parquet"
file_path = os.path.join(streaming_source, file_name)

# Define the Iceberg table name
iceberg_table = "local.db.streaming_users"

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder.getOrCreate()

# Create the Iceberg table if it doesn't exist
def create_iceberg_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {iceberg_table} (
            id BIGINT,
            name STRING,
            age INT,
            city STRING,
            country STRING,
            occupation STRING
        )
        USING iceberg
    """)

# Function to read streaming records and write to Iceberg table
def consume_streaming_records():
    if os.path.exists(file_path):
        # Read the Parquet file into a Pandas DataFrame
        df = pd.read_parquet(file_path)

        # Convert the Pandas DataFrame to a Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Write the Spark DataFrame to the Iceberg table
        spark_df.writeTo(iceberg_table).append()
        print(f"Appended {len(df)} records to the Iceberg table: {iceberg_table}")

def display_table():
    # Read the Iceberg table into a Spark DataFrame
    iceberg_df = spark.read.table(iceberg_table)

    # Show the contents of the Iceberg table
    iceberg_df.show()

def drop_table():
    # Drop the Iceberg table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")

# Run the consumer
if __name__ == "__main__":
    drop_table()
    create_iceberg_table()
    consume_streaming_records()
    #display_table()