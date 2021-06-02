import abc
import logging
from operators.SparkOperator import SparkOperator
import pathlib
from datetime import date

from hdfs.client import InsecureClient
from pyspark.sql.dataframe import DataFrame
from utils.creds import Credentials
from utils.hdfs import create_hdfs_path_if_not_exists
from utils.spark import open_file_as_df


class TransformSparkOperator(SparkOperator):

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
        df = open_file_as_df(self.spark, self.src_path)
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

    def __init__(self
               , src_file_ext:str = "json"
               , dst_file_ext:str = "parquet"
               , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src_file_ext = src_file_ext
        self.dst_file_ext = dst_file_ext

    def execute(self, context):
        execution_date = context.get("execution_date")
        execution_date = date.today() if execution_date is None \
                         else execution_date.date()
        date_dir = pathlib.Path(execution_date.isoformat())
        src_date_file = pathlib.Path(".".join([execution_date.isoformat(), self.src_file_ext]))
        dst_date_file = pathlib.Path(".".join([execution_date.isoformat(), self.dst_file_ext]))
        self.src_path = self.src_path / date_dir / src_date_file
        self.dst_path = self.dst_path / date_dir / dst_date_file
        super().execute(context)
