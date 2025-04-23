from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    datediff, current_date, col, expr,
    when, round
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load and enrich data (same as before)
df = spark.table("local.db.dim_users")
enriched_df = df.withColumn("age", (col("id") * 7 + 25))
enriched_df = enriched_df.withColumn(
    "signup_date",
    expr("date_sub(current_date(), cast(id * 30 as int))")
)
enriched_df = enriched_df.withColumn(
    "account_tenure_days",
    datediff(current_date(), col("signup_date"))
)
enriched_df = enriched_df.withColumn(
    "total_purchases",
    col("age") * 2 - col("account_tenure_days") / 30
)

# Prepare and train model (same as before)
assembler = VectorAssembler(
    inputCols=["age", "account_tenure_days"],
    outputCol="features"
)
df_assembled = assembler.transform(enriched_df)
lr = LinearRegression(featuresCol="features", labelCol="total_purchases")
model = lr.fit(df_assembled)
predictions = model.transform(df_assembled)

# Add customer segment based on predicted value
final_predictions = predictions.select(
    "name",
    "age",
    "signup_date",
    "account_tenure_days",
    "total_purchases",
    round("prediction", 2).alias("predicted_purchases")
).withColumn(
    "customer_segment",
    when(col("predicted_purchases") >= 80, "Premium")
    .when(col("predicted_purchases") >= 60, "High Value")
    .when(col("predicted_purchases") >= 40, "Medium Value")
    .when(col("predicted_purchases") >= 20, "Low Value")
    .otherwise("Basic")
).drop("predicted_purchases")

# Create Iceberg table
spark.sql("DROP TABLE IF EXISTS local.db.user_segments")
final_predictions.writeTo("local.db.user_segments").using("iceberg").create()

# Show final predictions
print("\nFinal Predictions with Customer Segments:")
final_predictions.show(truncate=False)