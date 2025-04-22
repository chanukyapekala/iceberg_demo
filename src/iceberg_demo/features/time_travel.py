from pyspark.sql import SparkSession
from time import sleep

# Initialize Spark
spark = SparkSession.builder \
    .getOrCreate()

# drop table
print("\nDropping table if exists...")
spark.sql("""
    DROP TABLE IF EXISTS local.db.dim_countries
""")
# Create dim_countries table
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.dim_countries (
        country_id INT,
        country_name STRING,
        continent STRING
    ) USING iceberg
""")

print("\nTable History after creating the table:")
spark.sql("SELECT * FROM local.db.dim_countries.history ORDER BY made_current_at").show(truncate=False)

# Initial insert
print("\nInserting initial data...")
spark.sql("""
    INSERT INTO local.db.dim_countries VALUES
        (1, 'Finland', 'Europe'),
        (2, 'Canada', 'North America')
""")
sleep(1)

print("\nTable History after inserting initial sample data:")
spark.sql("SELECT * FROM local.db.dim_countries.history ORDER BY made_current_at").show(truncate=False)

# Update Finland's name
print("\nUpdating Finland's name...")
spark.sql("""
    UPDATE local.db.dim_countries 
    SET country_name = 'Republic of Finland'
    WHERE country_id = 1
""")
sleep(1)

print("\nTable History after deleting the data:")
spark.sql("SELECT * FROM local.db.dim_countries.history ORDER BY made_current_at").show(truncate=False)

# Delete Canada
print("\nDeleting Canada...")
spark.sql("""
    DELETE FROM local.db.dim_countries 
    WHERE country_id = 2
""")

# Show table history
print("\nTable History:")
spark.sql("SELECT * FROM local.db.dim_countries.history ORDER BY made_current_at").show(truncate=False)

# Show current data
print("\nCurrent Data:")
spark.sql("SELECT * FROM local.db.dim_countries").show()