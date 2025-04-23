from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os
import signal
import sys
import shutil

def handle_shutdown(signum, frame):
    print("\nReceived shutdown signal. Stopping streaming...")
    try:
        if 'query' in globals() and query is not None and query.isActive:
            query.stop()
            print("Streaming stopped gracefully.")
    except Exception as e:
        print(f"Error during shutdown: {e}")
    finally:
        sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Initialize Spark
spark = SparkSession.builder \
    .appName("StreamingToIceberg") \
    .getOrCreate()

# Define paths
base_path = os.path.join(os.getcwd(), "src/iceberg_demo/streaming/data")
streaming_source = os.path.join(base_path, "")
checkpoint_path = os.path.join(base_path, "checkpoints")

if os.path.exists(checkpoint_path):
    shutil.rmtree(checkpoint_path)
os.makedirs(checkpoint_path)

# Load static schema from one file
sample_files = [f for f in os.listdir(streaming_source) if f.endswith(".parquet")]
if not sample_files:
    print(f"No parquet files found in {streaming_source}. Exiting...")
    sys.exit(1)

sample_file_path = os.path.join(streaming_source, sample_files[0])
source_df = spark.read.parquet(sample_file_path)
print("Source Schema:")
source_df.printSchema()

# Structured Streaming DataFrame
streaming_df = (
    spark.readStream
    .format("parquet")
    .schema(source_df.schema)
    .option("path", streaming_source)
    .option("maxFilesPerTrigger", 1)  # Simulate streaming
    .load()
    .withColumn("processing_time", current_timestamp())
)

print("Streaming Schema:")
streaming_df.printSchema()

# Drop existing table if needed
spark.sql("DROP TABLE IF EXISTS local.db.structured_streaming_users")

# Start the streaming query
query = (
    streaming_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .trigger(once=True)  # For testing, use trigger once
    .option("checkpointLocation", checkpoint_path)
    .toTable("local.db.structured_streaming_users")
)

try:
    print("Streaming query finished.")
    query.awaitTermination()
    spark.sql("SELECT * FROM local.db.structured_streaming_users").show(truncate=False)
except Exception as e:
    print(f"Unhandled streaming error: {e}")
    handle_shutdown(None, None)
