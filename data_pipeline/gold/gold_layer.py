import pathlib
from pyspark.sql import SparkSession
from delta import *

# Delta table location. This can be further changed as per final location but for scope of this project, it is in same repo.
p = pathlib.Path(__file__)
file_dir = p.resolve().parent
delta_loc = file_dir.parent.parent.joinpath("delta")


# Create spark session by setting spark warehouse to delta_loc
builder = (
    SparkSession.builder.appName("gold_layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", delta_loc.as_posix())
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sql("CREATE SCHEMA IF NOT EXISTS GOLD;")

# Using predicate pushdown
dim_df = spark.read.table("silver.dim_crypto_usd").select(
    "date", "open", "high", "low", "close", "adj_close", "volume", "symbol"
)
fact_df = spark.read.table("silver.fact_coinmarketcap").select(
    "date",
    "24h_volume_usd",
    "available_supply",
    "id",
    "market_cap_usd",
    "max_supply",
    "name",
    "percent_change_7d",
    "price_usd",
    "rank",
    "symbol",
    "total_supply",
)

join_cond = ["date", "symbol"]
joined_df = fact_df.join(dim_df, join_cond)
print(joined_df.printSchema())
joined_df.write.format("delta").mode("overwrite").saveAsTable(f"gold.crypto_market")

# Tables where no trade data is present
no_trade_data = dim_df.join(fact_df, "symbol", "leftanti")
no_trade_data.groupBy("symbol").agg({"date": "min"})
no_trade_data.write.format("delta").mode("overwrite").saveAsTable(
    f"gold.coins_with_no_trade_data"
)
