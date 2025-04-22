from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Source and target table names
source_table = "local.db.structured_streaming_users"
agg_table = "local.db.user_demographics"

spark.sql(f"DROP TABLE IF EXISTS {agg_table}")

def create_aggregation_table():
    # Read from source table
    df = spark.read.table(source_table)

    # Create aggregations
    agg_df = df.groupBy("country", "occupation").agg(
        count("*").alias("total_users"),
        avg("age").alias("avg_age"),
        min("age").alias("min_age"),
        max("age").alias("max_age"),
        count("city").alias("cities_count")
    )

    # Write to new table
    agg_df.writeTo(agg_table) \
        .using("iceberg") \
        .createOrReplace()

    print(f"Created aggregation table: {agg_table}")
    agg_df.show()

if __name__ == "__main__":
    create_aggregation_table()