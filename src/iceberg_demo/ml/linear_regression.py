from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, current_date, col, expr
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load base data and add enriched columns
df = spark.table("local.db.dim_users")
enriched_df = df.withColumn("age", (col("id") * 7 + 25))  # Synthetic age
enriched_df = enriched_df.withColumn(
    "signup_date",
    expr("date_sub(current_date(), cast(id * 30 as int))")  # Synthetic signup dates
)
enriched_df = enriched_df.withColumn(
    "account_tenure_days",
    datediff(current_date(), col("signup_date"))
)
enriched_df = enriched_df.withColumn(
    "total_purchases",
    col("age") * 2 - col("account_tenure_days") / 30  # Synthetic target
)

# Show enriched DataFrame
enriched_df.select("name", "age", "signup_date", "account_tenure_days", "total_purchases").show()
# Prepare features
assembler = VectorAssembler(
    inputCols=["age", "account_tenure_days"],
    outputCol="features"
)
df_assembled = assembler.transform(enriched_df)

# Train regression model
lr = LinearRegression(featuresCol="features", labelCol="total_purchases")
model = lr.fit(df_assembled)

# Make predictions
predictions = model.transform(df_assembled)
predictions.select("name", "age", "account_tenure_days", "total_purchases", "prediction").show()

# Filter predictions to include only scalar columns
final_predictions = predictions.select(
       "name",
       "age",
       "signup_date",
       "account_tenure_days",
       "total_purchases",
       "prediction"
)

spark.sql("DROP TABLE IF EXISTS local.db.user_predictions")

# write predictions to iceberg table
final_predictions.writeTo("local.db.user_predictions").using("iceberg").create()