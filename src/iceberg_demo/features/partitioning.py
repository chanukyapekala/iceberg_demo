from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Add required columns
print("Adding new columns...")
spark.sql("ALTER TABLE local.db.dim_users ADD COLUMN year INT")
spark.sql("ALTER TABLE local.db.dim_users ADD COLUMN created_month INT")

# Update data with month values first
print("\nUpdating month values...")
spark.sql("""
    UPDATE local.db.dim_users
    SET created_month = MONTH(CURRENT_DATE())
""")

# Update data with year values
print("\nUpdating year values...")
spark.sql("""
    UPDATE local.db.dim_users
    SET year = YEAR(CURRENT_DATE())
""")

# Update partition spe
print("\nUpdating partition spec...")
spark.sql("ALTER TABLE local.db.dim_users ADD PARTITION FIELD year")
spark.sql("ALTER TABLE local.db.dim_users ADD PARTITION FIELD created_month")
# Show updated partition spec
print("\nUpdated Partition Spec:")
spark.sql("DESCRIBE TABLE EXTENDED local.db.dim_users").show(truncate=False)

# Show data distribution
print("\nData Distribution by Year and Month:")
spark.sql("""
    SELECT year, created_month, count(*) as records
    FROM local.db.dim_users
    GROUP BY year, created_month
    ORDER BY year, created_month
""").show(truncate=False)

# Show updated table
print("\nUpdated Table:")
spark.sql("SELECT * FROM local.db.dim_users").show(truncate=False)