from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with Iceberg config (if not done via CLI)
spark = SparkSession.builder \
    .appName("IcebergSample") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "file:///Users/chanukya/GIT/iceberg_warehouse") \
    .getOrCreate()

# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# Write to Iceberg table
df.writeTo("local.db.people").createOrReplace()

# Read from Iceberg table
df_read = spark.read.table("local.db.people")
df_read.show()