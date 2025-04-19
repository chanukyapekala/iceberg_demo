from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType


# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the target Iceberg table
iceberg_table = "local.db.basic_table_2"

# Define the schema explicitly
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), True)
])

# Create initial data
initial_data = spark.createDataFrame([
    (1, "Alice", None, None, True),
    (2, "Bob", None, None, True)
], schema=schema)

# Write initial data to the Iceberg table
initial_data = initial_data.withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp"))
initial_data.writeTo(iceberg_table).createOrReplace()

# Simulate incoming data
incoming_data = spark.createDataFrame([
    (1, "Alice Updated"),
    (3, "Charlie")
], ["id", "name"])

# Load the existing Iceberg table
existing_data = spark.read.format("iceberg").load(iceberg_table)

# Rename the 'name' column in incoming_data to avoid ambiguity
incoming_data_renamed = incoming_data.withColumnRenamed("name", "incoming_name")

# Mark existing records as expired if they are updated
expired_records = existing_data.join(incoming_data_renamed, "id", "inner") \
    .withColumn("end_date", current_timestamp()) \
    .withColumn("is_current", lit(False))

# Add new records with current flag
new_records = incoming_data_renamed.withColumn("start_date", current_timestamp()) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumnRenamed("incoming_name", "name")

# Align schemas of expired_records and new_records
aligned_expired_records = expired_records.select(
    col("id").cast("int"),
    col("name").cast("string"),
    col("start_date").cast("timestamp"),
    col("end_date").cast("timestamp"),
    col("is_current").cast("boolean")
)

aligned_new_records = new_records.select(
    col("id").cast("int"),
    col("name").cast("string"),
    col("start_date").cast("timestamp"),
    col("end_date").cast("timestamp"),
    col("is_current").cast("boolean")
)

# Combine expired and new records
scd2_data = aligned_expired_records.unionByName(aligned_new_records)

# Write back to the Iceberg table
scd2_data.writeTo(iceberg_table).overwritePartitions()

# Verify the updated table
check_df = spark.read.format("iceberg").load(iceberg_table)
check_df.show(truncate=False)