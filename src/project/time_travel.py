from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

table = "local.db.basic_table"
log_path = "logs/time_travel_demo.log"

# Get snapshot history using metadata table
snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at")
snapshots = snapshots_df.collect()

if not snapshots:
    print("No snapshots found.")
else:
    first_snapshot_id = snapshots[0]["snapshot_id"]

    # Log snapshot info
    os.makedirs("logs", exist_ok=True)
    with open(log_path, "w") as log:
        log.write("📸 Available Snapshots:\n")
        for snap in snapshots:
            log.write(f"- ID: {snap['snapshot_id']}, Timestamp: {snap['committed_at']}\n")

    # Time travel to the first snapshot
    historic_df = spark.read.option("snapshot-id", first_snapshot_id).table(table)
    historic_df.show()

    with open(log_path, "a") as log:
        log.write("\n🕰️ Data from first snapshot:\n")
        for row in historic_df.collect():
            log.write(str(row) + "\n")

    print(f"✅ Time travel demo written to `{log_path}`")
