import json
import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, from_unixtime
from delta import *

# Delta table location. This can be further changed as per final location but for scope of this project, it is in same repo.
p = pathlib.Path(__file__)
file_dir = p.resolve().parent
delta_loc = file_dir.parent.parent.joinpath("delta")


# Create spark session by setting spark warehouse to delta_loc
builder = (
    SparkSession.builder.appName("bronze_layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", delta_loc.as_posix())
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS SILVER;")

# Read required metadata.
metadata_file_path = file_dir.parent.joinpath("metadata.json")
with open(metadata_file_path) as f:
    metadata = json.load(f)


# Dimension tables read
first_read = True

for l_type in metadata["dimension_load"]:
    if first_read:
        delta_tbl = spark.read.table(f"bronze.{l_type}")
        delta_tbl = delta_tbl.withColumn("symbol", lit(l_type))
        first_read = False
    else:
        delta_tbl_temp = spark.read.table(f"bronze.{l_type}")
        delta_tbl_temp = delta_tbl_temp.withColumn("symbol", lit(l_type))
        delta_tbl = delta_tbl.union(delta_tbl_temp)

# Ideally, I would store it in separate tables as all dimension tables should. But I wanted to play with it a little in optimize and vaccum.
delta_tbl.write.format("delta").mode("overwrite").saveAsTable("silver.dim_crypto_usd")
print(f"Write successful for DIMENSION {l_type}")

# Fact tables read
first_read = True
for l_type in metadata["fact_load"]:
    if first_read:
        delta_tbl = spark.read.table(f"bronze.{l_type}")
        delta_tbl = (
            delta_tbl.withColumn("trade_on", lit(l_type))
            .withColumn("date", from_unixtime("last_updated").cast("date"))
            .drop("_c0", "last_updated")
        )
        first_read = False
    else:
        delta_tbl_temp = spark.read.table(f"bronze.{l_type}")
        delta_tbl_temp = (
            delta_tbl_temp.withColumn("trade_on", lit(l_type))
            .withColumn("date", from_unixtime("last_updated").cast("date"))
            .drop("_c0", "last_updated")
        )
        delta_tbl = delta_tbl.union(delta_tbl_temp)

delta_tbl.write.format("delta").mode("overwrite").saveAsTable(f"silver.fact_{l_type}")
print(f"Write successful for FACT {l_type}")
