import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession

// This code snippet demonstrates how to evolve the partition spec of an Iceberg table using Spark.
// This is WIP for full length scala and spark implementation.

// Create Spark session
val spark = SparkSession.builder()
  .appName("IcebergPartitionEvolution")
  .master("local[*]")
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "hadoop")
  .config("spark.sql.catalog.local.warehouse", "file:///Users/chanukya/GIT/iceberg_warehouse") // change to your Iceberg warehouse path
  .getOrCreate()

// Get the Iceberg catalog
val catalog = spark
  .sparkContext
  .env
  .lookup("local")
  .asInstanceOf[SparkCatalog]

// Load the table
val table = catalog.loadTable(TableIdentifier.of("db", "dim_sales"))

// Evolve the partition spec
val newSpec = PartitionSpec
  .builderFor(table.schema())
  .identity("year")
  .identity("month")  // adding new partition field
  .build()

// Commit the new partition spec
table.updateSpec().replaceSpec(newSpec).commit()

println("Partition spec evolved successfully!")

//TODO:  Not sure it will work, but let's try to read the table..
