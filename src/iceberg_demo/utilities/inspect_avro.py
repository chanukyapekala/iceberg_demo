from pyspark.sql import SparkSession
import os
import fastavro
import json
import base64
import pyarrow.parquet as pq

# Create SparkSession
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# Paths
warehouse_path = "/Users/chanukya/GIT/iceberg_warehouse/db/dim_users"
metadata_path = os.path.join(warehouse_path, "metadata")
data_path = os.path.join(warehouse_path, "data")

def clean_for_json(obj):
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('utf-8')
    elif isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj

def list_avro_files():
    if os.path.exists(metadata_path):
        for root, _, files in os.walk(metadata_path):
            for f in files:
                if f.endswith('.avro'):
                    avro_path = os.path.join(root, f)
                    print(f"\nReading Avro file: {f}")
                    try:
                        with open(avro_path, 'rb') as avro_file:
                            avro_reader = fastavro.reader(avro_file)
                            schema = avro_reader.writer_schema
                            records = list(avro_reader)

                            print("\nSchema:")
                            print(json.dumps(clean_for_json(schema), indent=2))

                            print("\nRecords:")
                            for record in records:
                                print(json.dumps(clean_for_json(record), indent=2))

                    except Exception as e:
                        print(f"Error reading {f}: {str(e)}")

def show_parquet_contents():
    if os.path.exists(data_path):
        for root, _, files in os.walk(data_path):
            for f in files:
                if f.endswith('.parquet'):
                    parquet_path = os.path.join(root, f)
                    print(f"\nReading Parquet file: {f}")
                    try:
                        # Read parquet metadata
                        parquet_file = pq.ParquetFile(parquet_path)

                        print("\nMetadata:")
                        print(f"Number of row groups: {parquet_file.num_row_groups}")
                        print(f"Schema: {parquet_file.schema}")

                        # Read and show data
                        print("\nData:")
                        table = parquet_file.read()
                        print(table.to_pandas())

                        # Show detailed row group info
                        print("\nRow Group Details:")
                        for i in range(parquet_file.num_row_groups):
                            row_group = parquet_file.metadata.row_group(i)
                            print(f"\nRow Group {i}:")
                            print(f"Number of rows: {row_group.num_rows}")
                            print(f"Total byte size: {row_group.total_byte_size}")

                    except Exception as e:
                        print(f"Error reading {f}: {str(e)}")
                    break  # Show only first parquet file

# Read both file types
list_avro_files()
print("\n" + "="*50 + "\n")
show_parquet_contents()