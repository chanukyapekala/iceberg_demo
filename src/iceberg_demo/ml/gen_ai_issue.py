from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

def create_persona(age, total_purchases):
    if age < 30:
        generation = "young professional"
    elif age < 50:
        generation = "mid-career professional"
    else:
        generation = "experienced consumer"

    if total_purchases < 3:
        offer = "welcome 15% discount"
        segment = "new customer"
    elif total_purchases < 8:
        offer = "special 20% discount"
        segment = "regular customer"
    else:
        offer = "exclusive 25% discount"
        segment = "loyal customer"

    return generation, segment, offer

@udf(returnType=StringType())
def generate_message(name, age, total_purchases):
    generation, segment, offer = create_persona(age, int(total_purchases))

    templates = [
        f"Hi {name}! As our {segment}, enjoy your {offer} today. Perfect for your {generation} lifestyle!",
        f"Special for {name}: Your {segment} status unlocks a {offer}. Don't miss out!",
        f"Dear {name}, thanks for {int(total_purchases)} purchases! Here's your {offer} to celebrate."
    ]

    # Simple rotation based on purchase count
    template_idx = int(total_purchases) % len(templates)
    return templates[template_idx]

# Load and process data
df = spark.table("local.db.user_predictions")
df_with_messages = df.withColumn(
    "marketing_message",
    generate_message("name", "age", "total_purchases")
)

spark.sql("DROP TABLE IF EXISTS local.db.user_messages")

# Save results
df_with_messages.select(
    "name",
    "age",
    "total_purchases",
    "marketing_message"
).writeTo("local.db.user_messages").using("iceberg").create()