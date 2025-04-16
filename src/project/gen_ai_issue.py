from pyspark.sql import SparkSession
from transformers import pipeline

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load data from the basic_table
df = spark.read.table("local.db.basic_table")

# Collect the `name` column as a list
names = df.select("name").rdd.flatMap(lambda x: x).collect()

# Initialize a text generation pipeline using a pre-trained model
generator = pipeline("text-generation", model="gpt2")

# Generate text for each name
for name in names:
    print(f"Input: {name}")
    result = generator(f"My name is {name} and", max_length=30, num_return_sequences=1)
    print(f"Generated Text: {result[0]['generated_text']}\n")