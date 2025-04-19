# src/project/schema_evolution.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Alice", 25),
    (2, "Bob", 30)
], ["id", "name", "age"])

df.writeTo("local.db.basic_table").using("iceberg").createOrReplace()

schema_evolution_df = spark.table("local.db.basic_table")
schema_evolution_df.printSchema()
schema_evolution_df.show()
