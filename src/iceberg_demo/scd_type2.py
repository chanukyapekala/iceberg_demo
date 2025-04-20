from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when

# Initialize Spark
spark = SparkSession.builder.getOrCreate()


# Read current table
spark.sql("""
    CREATE OR REPLACE TABLE local.db.dim_users_scd2 (
        id INT,
        name STRING,
        age INT,
        start_date DATE,
        end_date DATE,
        is_current BOOLEAN
    ) USING iceberg
    PARTITIONED BY (month(start_date))
""")

# Insert initial data from dim_users
spark.sql("""
    INSERT INTO local.db.dim_users_scd2
    SELECT
        id,
        name,
        age,
        COALESCE(start_date, CURRENT_DATE()) as start_date,
        NULL as end_date,
        TRUE as is_current
    FROM local.db.dim_users
""")

# Create changes DataFrame and register as temp view
changes = spark.createDataFrame([
    (1, "Antti Updated", 26, "2024-03-20"),
    (5, "Jason Updated", 46, "2024-03-20")
], ["id", "name", "age", "start_date"])
changes.createOrReplaceTempView("changes")

# Update using MERGE
spark.sql("""
    MERGE INTO local.db.dim_users_scd2 t
    USING changes s
    ON t.id = s.id AND t.is_current = true
    WHEN MATCHED THEN
        UPDATE SET end_date = CURRENT_DATE(), is_current = false
    WHEN NOT MATCHED THEN
        INSERT (id, name, age, start_date, end_date, is_current)
        VALUES (s.id, s.name, s.age, CAST(s.start_date AS DATE), NULL, true)
""")

# Insert new versions
spark.sql("""
    INSERT INTO local.db.dim_users_scd2
    SELECT
        c.id,
        c.name,
        c.age,
        CAST(c.start_date AS DATE),
        NULL,
        true
    FROM changes c
    JOIN local.db.dim_users_scd2 t
    ON c.id = t.id
    WHERE t.is_current = false
""")