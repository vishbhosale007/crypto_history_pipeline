import json
import pathlib
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.sql import SparkSession
from delta import *

# Delta table location. This can be further changed as per final location but for scope of this project, it is in same repo.
p = pathlib.Path(__file__)
file_dir = p.resolve().parent
delta_loc = file_dir.parent.parent.joinpath("delta")

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
spark.sql("CREATE SCHEMA IF NOT EXISTS BRONZE;")

# Read required metadata.
metadata_file_path = file_dir.parent.joinpath("metadata.json")
with open(metadata_file_path) as f:
    metadata = json.load(f)


# Get history data location
for i in file_dir.parents:
    for dir_name in [j for j in i.iterdir() if j.is_dir()]:
        print(f"i is {i} and {dir_name} is dir_name")
        if "history" == dir_name.name:
            history_data_path = dir_name
            break
    else:
        continue
    break


def fix_column_names(column_list) -> list:
    fix_column_name_expression: list = []
    for i in column_list:
        fix_column_name_expression.append(
            f"`{str(i)}` AS {str(i).replace(' ','_').lower()}"
        )
    return fix_column_name_expression


# Create a path for delta tables
delta_location = history_data_path.parent.joinpath("delta", "bronze")

load_types = [i for i in metadata.keys() if "load" in i]

for l_type in load_types:
    for load in metadata[l_type]:
        filename = history_data_path.joinpath(load).as_posix()
        df = spark.read.option("header", "true").csv(filename + "*")
        fixed_expr = fix_column_names(df.columns)
        df = df.selectExpr(fixed_expr)
        df = df.withColumn("file_name", input_file_name()).withColumn(
            "inserted_on", current_timestamp()
        )

        # As this pipeline, is only indicative of history load, I've used overwrite mode.
        df.write.format("delta").mode("overwrite").saveAsTable(
            f'bronze.{load.split(".")[0]}'
        )

        print(f"Write successful for {load}")
