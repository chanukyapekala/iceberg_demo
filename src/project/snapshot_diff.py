from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

table = "local.db.basic_table"
log_path = "logs/snapshot_diff.log"

# Get snapshot metadata
snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at")
snapshots = snapshots_df.collect()

if len(snapshots) < 2:
    print("❗ Need at least two snapshots to show diff.")
else:
    old_snapshot = snapshots[0]["snapshot_id"]
    new_snapshot = snapshots[-1]["snapshot_id"]

    # Read snapshots
    old_df = spark.read.option("snapshot-id", old_snapshot).table(table)
    new_df = spark.read.option("snapshot-id", new_snapshot).table(table)

    # Compute diffs
    added_rows = new_df.subtract(old_df)
    removed_rows = old_df.subtract(new_df)

    # Write log
    os.makedirs("logs", exist_ok=True)
    with open(log_path, "w") as log:
        log.write(f"📸 Comparing snapshots:\n- Old: {old_snapshot}\n- New: {new_snapshot}\n\n")

        log.write("➕ Added Rows:\n")
        for row in added_rows.collect():
            log.write(str(row) + "\n")

        log.write("\n➖ Removed Rows:\n")
        for row in removed_rows.collect():
            log.write(str(row) + "\n")

    print(f"✅ Snapshot diff written to `{log_path}`")
