# This code doesn't work, because the branch creation is not supported in PySpark API.
# It is only supported in Java/Scala API.

from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder.getOrCreate()

print("Creating main table...")
spark.sql("DROP TABLE IF EXISTS local.db.product_catalog")
spark.sql("""
    CREATE TABLE local.db.product_catalog (
        product_id INT,
        name STRING,
        price DECIMAL(10,2),
        category STRING
    ) USING iceberg
""")

print("\nInserting initial data into main...")
spark.sql("""
    INSERT INTO local.db.product_catalog VALUES
        (1, 'Laptop Pro', 1299.99, 'Electronics'),
        (2, 'Wireless Mouse', 49.99, 'Accessories'),
        (3, 'Monitor 4K', 399.99, 'Electronics')
""")
sleep(1)

print("\nCreating development branch...")
spark.sql("""
    ALTER TABLE local.db.product_catalog 
    CREATE BRANCH development
""") # This creates a new branch for development, but its not working, based on the documentation, it works only via Java/Scala API.
sleep(1)

print("\nInserting data into main...")
spark.sql("""
    INSERT INTO local.db.product_catalog VALUES
        (4, 'Keyboard RGB', 89.99, 'Accessories'),
        (5, 'Webcam HD', 79.99, 'Electronics')
""")
sleep(1)

print("\nInserting data into development branch...")
spark.sql("""
    INSERT INTO local.db.product_catalog.branch_development VALUES
        (4, 'Gaming Keyboard', 129.99, 'Gaming'),
        (5, 'Gaming Mouse', 89.99, 'Gaming')
""")

print("\nMain branch data:")
spark.sql("SELECT * FROM local.db.product_catalog").show()

print("\nDevelopment branch data:")
spark.sql("SELECT * FROM local.db.product_catalog.branch_development").show()

print("\nBranch information:")
spark.sql("SELECT * FROM local.db.product_catalog.refs").show(truncate=False)