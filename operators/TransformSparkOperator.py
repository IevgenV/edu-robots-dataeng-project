import abc
import logging
import pathlib
from datetime import date
from typing import Union

from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hdfs.client import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from utils.creds import Credentials
from utils.hdfs import HDFSDefaults, create_hdfs_path_if_not_exists

from ..utils.spark import SparkDefaults, open_file_as_df


class TransformSparkOperator(BaseOperator):

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

        dst_data_format = dst_path.suffix.lstrip('.')
        if dst_data_format in SparkDefaults.SUPPORTED_DST_FORMATS:
            raise TypeError("Destination file (Silver data) need to have one "
                           f"of the supported extensions: {SparkDefaults.SUPPORTED_DST_FORMATS}.")

        self.spark = SparkSession.builder \
                                 .master(spark_master) \
                                 .appName(spark_app_name) \
                                 .getOrCreate()

        self.src_path = src_path
        self.dst_path = dst_path
        self.write_mode = write_mode

        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def validate(self, df:DataFrame) -> DataFrame:
        pass

    @abc.abstractmethod
    def clean(self, df:DataFrame) -> DataFrame:
        pass

    @abc.abstractmethod
    def save(self, df:DataFrame) -> bool:
        pass

    def execute(self, context):
        logging.info(f"Read source (bronze) data from `{self.src_path}`...")
        df = open_file_as_df(self.src_path)
        records_cnt = df.count()
        logging.info(f"`{self.src_path}` has been opened."
                     f" DataFrame contains {records_cnt} records.")

        logging.info("Start data validation process...")
        df = self.validate(df)
        records_cnt = df.count()
        logging.info("Data validation done. DataFrame contains "
                    f"{records_cnt} after data validation.")

        logging.info("Start data cleaning process...")
        df = self.clean(df)
        records_cnt = df.count()
        logging.info("Data cleaning done. DataFrame contains "
                    f"{records_cnt} after data cleaning.")

        logging.info(f"Saving transformed data into `{self.dst_path}`."
                     f"{records_cnt} rows are going to be written.")
        if self.save(df):
            logging.info("Transformed data has been wtited to "
                        f"`{self.dst_path}` in `{self.write_mode}` mode.")
        else:
            logging.info("Transformed data hasn't been wtited to "
                        f"`{self.dst_path}` in `{self.write_mode}` mode."
                         "Please, check logs above to figure out the reson.")


class TransformSparkHDFSOperator(TransformSparkOperator):
    
    def __init__(self
               , hdfs_conn_id:str = Credentials.DEFAULT_HDFS_CONN_ID
               , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hdfs_conn_id = hdfs_conn_id

    def save(self, df:DataFrame) -> bool:
        hdfs_creds = Credentials.DEFAULT_HDFS_CREDS if self._hdfs_conn_id is None \
                else Credentials.get_hdfs_creds(self._hdfs_conn_id)
        logging.info(f"Create client and connect to HDFS at {hdfs_creds['url']}...")
        hdfs_client = InsecureClient(**hdfs_creds)
        create_hdfs_path_if_not_exists(hdfs_client, self.dst_path.parents[0])
        logging.info(f"Client has been created.")
        # NOTE(i.vagin): # Format is based on file extension:
        dst_data_format = self.dst_path.suffix.lstrip('.')
        df.write.format(dst_data_format) \
                .mode(self.write_mode) \
                .save(self.dst_path.as_posix())
        return True


class TransformSparkHDFSDailyOperator(TransformSparkHDFSOperator):
    
    def execute(self, context):
        execution_date = context.get("execution_date")
        execution_date = date.today() if execution_date is None \
                         else execution_date.date()
        self.src_path = self.src_path / pathlib.Path(execution_date.isoformat())
        self.dst_path = self.dst_path / pathlib.Path(execution_date.isoformat())
        super().execute(context)