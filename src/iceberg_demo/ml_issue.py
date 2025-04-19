from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load data from the basic_table
df = spark.read.table("local.db.basic_table")

# Add a dummy feature column for demonstration purposes
# (In real scenarios, you would use meaningful numerical features)
df = df.withColumn("feature1", df["id"] * 1.0) \
       .withColumn("feature2", df["id"] * 2.0)

# Assemble features into a single vector column
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
feature_df = assembler.transform(df)

# Apply K-Means clustering
kmeans = KMeans(k=2, seed=1)  # k=2 clusters
model = kmeans.fit(feature_df)

# Make predictions
predictions = model.transform(feature_df)

# Show the results
predictions.select("id", "name", "features", "prediction").show(truncate=False)