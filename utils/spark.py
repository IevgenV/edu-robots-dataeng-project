import logging
import pathlib

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession


class SparkDefaults:
    DEFAULT_SPARK_MASTER = "local"
    DEFAULT_SPARK_JDBC_DRIVER_PATH = "/opt/spark/drivers/postgresql-42.2.20.jar"
    SUPPORTED_DST_FORMATS = ["parquet"]
    SUPPORTED_SRC_FORMATS = ["json", "csv", "parquet"]

def open_file_as_df(spark:SparkSession, fpath:pathlib.Path) -> DataFrame:
    # NOTE(i.vagin): # Format is based on file extension:
    src_data_format = fpath.suffix.lstrip('.') 
    if src_data_format not in SparkDefaults.SUPPORTED_SRC_FORMATS:
        raise TypeError("Source file need to have one "
                        f"of the supported extensions: {SparkDefaults.SUPPORTED_SRC_FORMATS}.")
    df = spark.read.option("header", "true").format(src_data_format).load(fpath.as_posix())
    return df
