# src/project/load_sample_data.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

# drop existing table
spark.sql("DROP TABLE IF EXISTS local.db.dim_users")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

data = [
    (1, "Antti"),
    (2, "Eron"),
    (3, "Matti"),
    (4, "Teppo")
]

df = spark.createDataFrame(data, schema)

df.writeTo("local.db.dim_users").create()

print("\nTable dim_users created with sample data:")
df.show(truncate=False)