import os
import pathlib
from pyspark.sql import SparkSession
from delta import *
builder = SparkSession.builder.appName("bronze_layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

p = pathlib.Path('.')
work_dir = p.resolve()

# Get history data location
for i in work_dir.parents:
    for dir_name in [j for j in i.iterdir() if j.is_dir()]:
        print(f"i is {i} and {dir_name} is dir_name")
        if 'history' == dir_name.name:
            history_data_path = dir_name
            break
    else:
        continue
    break

def fix_column_names(column_list) -> list:
    fix_column_name_expression:list = []
    for i in column_list:
        fix_column_name_expression.append(f"`{str(i)}` AS {str(i).replace(' ','_').lower()}")
    return fix_column_name_expression

# Create a path for delta tables
delta_location = history_data_path.parent.joinpath('delta','bronze')

for i in os.listdir(history_data_path):
    filename = history_data_path.joinpath(i).as_posix()
    df = spark.read.option('header','true').csv(filename)
    fixed_expr = fix_column_names(df.columns)
    df = df.selectExpr(fixed_expr)
    df.write.format("delta").save(delta_location.joinpath(i.split('.')[0]).as_posix())
    print("Write successful")