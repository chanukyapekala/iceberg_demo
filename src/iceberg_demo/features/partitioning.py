from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder.getOrCreate()

print("Dropping existing table...")
spark.sql("DROP TABLE IF EXISTS local.db.dim_users")

# Create dim_sales table with year partition
print("\nCreating dim_sales table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.dim_sales (
        sale_id INT,
        product STRING,
        amount DECIMAL(10,2),
        year INT,
        month INT
    ) USING iceberg
    PARTITIONED BY (year)
""")
sleep(1)

# Insert initial data (year partition only)
print("\nInserting initial data with year partition...")
spark.sql("""
    INSERT INTO local.db.dim_sales VALUES
        (1, 'Laptop', 1200.00, 2023, 1),
        (2, 'Phone', 800.00, 2023, 2),
        (3, 'Tablet', 500.00, 2023, 3),
        (4, 'Monitor', 300.00, 2023, 4)
""")
sleep(1)

# Show current partition spec
print("\nCurrent Partition Spec:")
spark.sql("DESCRIBE TABLE EXTENDED local.db.dim_sales").show(truncate=False)


# Show data distribution
print("\nData Distribution:")
spark.sql("""
    SELECT year, month, count(*) as records
    FROM local.db.dim_sales
    GROUP BY year, month
    ORDER BY year, month
""").show(truncate=False)