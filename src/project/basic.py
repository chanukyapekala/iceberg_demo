# src/project/basic.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    (1, "Antti"),
    (2, "Elina"),
    (3, "Jani"),
    (4, "Kati"),
    (5, "Mika")
], ["id", "name"])

df.writeTo("local.db.users").createOrReplace()

check_df = spark.read.table("local.db.basic_table")
check_df.show()


