import abc
import logging
import os
import pathlib
from datetime import date

import psycopg2
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hdfs import InsecureClient

from ..utils.creds import Credentials
from ..utils.hdfs import HDFSDefaults, create_hdfs_path_if_not_exists


class ExtractPGTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self
               , table_name:str
               , data_directory:pathlib.Path = None
               , db_conn_id:str = Credentials.DEFAULT_DB_CONN_ID
               , *args, **kwargs):
        assert table_name, "`table_name` argument can't be specified as None"
        self._table_name = table_name
        self._data_directory = data_directory if data_directory is not None \
                               else HDFSDefaults.DEFAULT_BRONZE_PATH
        self._conn_id = db_conn_id
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def extract_table(self, pg_conn, extraction_date:date):
        pass

    def execute(self, context):
        logging.info("Parameters for table loading parsing started...")
        db_creds = Credentials.get_pg_creds(self._conn_id)
        extraction_date = context.get("execution_date")

        db_creds = db_creds if db_creds is not None \
                   else Credentials.DEFAULT_PGDB_CREDS
        extraction_date = date.today() if extraction_date is None \
                          else extraction_date.date()

        logging.info("Parameters for table loading were parsed:")
        logging.info(f"Target data root path: {self._data_directory}")
        logging.info(f"Target data path: {extraction_date.isoformat()}")

        logging.info("Connecting to the database. "
                    f"host: {db_creds['host']}, "
                    f"port: {db_creds['port']}, "
                    f"db: {db_creds['database']} ...")
        with psycopg2.connect(**db_creds) as pg_connection:
            logging.info("DB connection has been established.")
            self.extract_table(pg_connection, extraction_date)


class ExtractPGTable2HDFSOperator(ExtractPGTableOperator):

    def __init__(self
               , hdfs_conn_id:str = Credentials.DEFAULT_HDFS_CONN_ID
               , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hdfs_conn_id = hdfs_conn_id

    def extract_table(self, pg_conn, extraction_date:date):
        hdfs_creds = Credentials.DEFAULT_HDFS_CREDS if self._hdfs_conn_id is None \
                     else Credentials.get_hdfs_creds(self._conn_id)

        logging.info(f"Create client and connect to HDFS at {hdfs_creds['url']}...")
        hdfs_client = InsecureClient(**hdfs_creds)
        logging.info(f"Client has been created.")

        data_path = self._data_directory / pathlib.Path(extraction_date.isoformat())
        logging.info(f"Check if target directory `{data_path}` exists at HDFS and create it if doesn't.")
        create_hdfs_path_if_not_exists(hdfs_client, data_path)

        logging.info(f"Extracting table `{self._table_name}` to HDFS: {data_path}...")
        hdfs_fname = os.path.join(data_path, f"{self._table_name}.csv")
        logging.info("Connection to the database has been established")
        cursor = pg_conn.cursor()
        logging.info("HDFS file opening for writing...")
        with hdfs_client.write(hdfs_fname, overwrite=True) as csv_file:
            logging.info("HDFS file has been opened.")
            logging.info(f"Writing data from `{self._table_name}` DB table to {hdfs_fname}...")
            cursor.copy_expert(f"COPY public.{self._table_name} TO STDOUT WITH HEADER CSV", csv_file)
            logging.info(f"Table `{self._table_name}` has been dumped top the {hdfs_fname}.")
        logging.info(f"Table {self._table_name} has been succesfully extracted to {data_path}.")
