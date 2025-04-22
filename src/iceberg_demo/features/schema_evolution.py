# src/project/schema_evolution.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df2 = spark.createDataFrame([
    (5, "Jason", 45)
], ["id", "name", "age"])

# Evolve the schema
spark.sql("ALTER TABLE local.db.dim_users ADD COLUMN age INT")

# Append data with schema evolution
df2.writeTo("local.db.dim_users") \
    .using("iceberg") \
    .append()

spark.sql("SELECT * FROM local.db.dim_users").show(truncate=False)
