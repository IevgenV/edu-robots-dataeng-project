import abc
import logging
import pathlib
from datetime import date
from typing import Union

from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from utils.creds import Credentials
from utils.hdfs import HDFSDefaults
from utils.spark import SparkDefaults


class SparkOperator(BaseOperator):

    @apply_defaults
    def __init__(self
               , src_path:Union[pathlib.Path, str] = HDFSDefaults.DEFAULT_BRONZE_PATH
               , dst_path:Union[pathlib.Path, str] = HDFSDefaults.DEFAULT_SILVER_PATH
               , spark_master:str = SparkDefaults.DEFAULT_SPARK_MASTER
               , spark_app_name:str = Credentials.CONN_SPARK_ID
               , write_mode:str = "append"
               , *args, **kwargs):
        assert src_path is not None, "`src_path` argument can't be specified as None"
        assert dst_path is not None, "`dst_path` argument can't be specified as None"
        assert write_mode is not None, "`write_mode` argument can't be specified as None"
        spark_master = spark_master if spark_master is not None \
                       else SparkDefaults.DEFAULT_SPARK_MASTER
        spark_app_name = spark_app_name if spark_app_name is not None \
                         else Credentials.CONN_SPARK_ID

        src_path = src_path if isinstance(src_path, pathlib.Path) \
                   else pathlib.Path(src_path) 
        dst_path = dst_path if isinstance(dst_path, pathlib.Path) \
                   else pathlib.Path(dst_path)

        self.src_path = src_path
        self.dst_path = dst_path
        self.write_mode = write_mode
        self.spark = SparkSession.builder \
                                 .master(spark_master) \
                                 .appName(spark_app_name) \
                                 .getOrCreate()

        dst_data_format = dst_path.suffix.lstrip('.')
        if dst_data_format in SparkDefaults.SUPPORTED_DST_FORMATS:
            raise TypeError("Destination file (Silver data) need to have one "
                           f"of the supported extensions: {SparkDefaults.SUPPORTED_DST_FORMATS}.")    

        super().__init__(*args, **kwargs)