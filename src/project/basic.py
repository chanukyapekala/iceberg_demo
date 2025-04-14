# src/project/basic.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob")
], ["id", "name"])

df.writeTo("local.db.basic_table").createOrReplace()

check_df = spark.read.table("local.db.basic_table")
check_df.show()
