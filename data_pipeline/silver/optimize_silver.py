import json
import pathlib
from pyspark.sql import SparkSession
from delta import *

# Delta table location. This can be further changed as per final location but for scope of this project, it is in same repo.
p = pathlib.Path(__file__)
file_dir = p.resolve().parent
delta_loc = file_dir.parent.parent.joinpath("delta")


# Create spark session by setting spark warehouse to delta_loc
builder = (
    SparkSession.builder.appName("optimize_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", delta_loc.as_posix())
    .enableHiveSupport()
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read required metadata.
metadata_file_path = file_dir.parent.joinpath("metadata.json")
with open(metadata_file_path) as f:
    metadata = json.load(f)

# Dimension tables are optimized by date
deltaTable = DeltaTable.forName(spark, f"silver.dim_crypto_usd")
deltaTable.optimize().executeZOrderBy(
    metadata["properties"]["dimension_load"]["zorder_by"]
)
# Vacuum is not needed as by default vacuum removes file unused files which are older than 7 days
deltaTable.vacuum()
print(f"Optimization complete for dimension table")

# Fact tables are optimized by symbol and date
for market in metadata["fact_load"]:
    deltaTable = DeltaTable.forName(spark, f"silver.fact_{market}")
    deltaTable.optimize().executeZOrderBy(
        metadata["properties"]["fact_load"]["zorder_by"]
    )
    deltaTable.vacuum()
    print(f"Optimization complete for fact {market} table")
