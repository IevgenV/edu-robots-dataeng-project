import abc
import logging
import json
import pathlib
from datetime import date

from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from hdfs import InsecureClient

from .oos.Cache import Cache
from .oos.FileDailyCache import FileDailyCache
from .oos.HDFSDailyCache import HDFSDailyCache
from .oos.Server import DailyDataSource, ProdServer

DEFAULT_HDFS_CONN_ID = "hdfs_server"
DEFAULT_SERVER_NAME = "prod_server"
DEFAULT_DATA_PATH = "~"
DEFAULT_OOS_CONFIG = {
    "prod_server": {
        "address": "https://robot-dreams-de-api.herokuapp.com",
        "login": "rd_dreams",
        "password": "djT6LasE",
        "apis": {
            "authorize": {
               "endpoint": "/auth",
               "method": "POST"
            },
            "out_of_stock": {
                "endpoint": "/out_of_stock",
                "method": "GET"
            }
        }
    }
}


def __get_oos_creds(conn_id:str) -> dict:
    conn_oos = BaseHook.get_connection(conn_id)
    return {
          conn_id : {
              'address': conn_oos.host,
              'login': conn_oos.login,
              'password': conn_oos.password,
              'apis': json.loads(conn_oos.extra)
          }
    }

def __get_hdfs_creds(conn_id:str) -> dict:
    conn_hdfs = BaseHook.get_connection(conn_id)
    return {
          'url': ":".join([conn_hdfs.host, str(conn_hdfs.port)]),
          'user': conn_hdfs.login
    }


class ExtractOOSOperator(BaseOperator):

    @apply_defaults
    def __init__(self
               , oos_config_path:pathlib.Path = None
               , _data_directory:pathlib.Path = None
               , oos_conn_id:str = DEFAULT_SERVER_NAME
               , *args, **kwargs):
        self.__data_directory = _data_directory if _data_directory is not None else DEFAULT_DATA_PATH
        self._server_name = oos_conn_id
        self._oos_config = oos_config_path
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _get_cache_strategy(self
                          , data_source:DailyDataSource
                          , cache_date:date=None) -> Cache:
        pass

    def execute(self, context):
        cache_date = context.get("execution_date")
        cache_date = date.today() if cache_date is None else cache_date.date()
        config = self._oos_config if self._oos_config is not None else __get_oos_creds(self._server_name)

        logging.info("Parameters for OOS products loading are:")
        logging.info(f"Target data root path: {self._data_directory}")
        logging.info(f"Target data path: {cache_date.isoformat()}")

        logging.info("Creating data source (connection with remote API):")
        server = ProdServer(config, self._server_name)
        logging.info("Data source (i.e., connection with remote API) has been created.")

        logging.info("Create Cache object to store retrieved product data into directory:")
        cache = self._get_cache_strategy(server, cache_date)
        logging.info("Cache object has been created.")

        logging.info("Attempt to cache data from data source...")
        if cache.update():  # <- Gets data and cache it at disk if not cached yet
            logging.info("Data has been cached.")
            logging.info("Data has been saved (cached) to the {}".format(cache.get_cache_filepath()))
        else:
            logging.error("Data has NOT been cached.")
            msg = "Data hasn't been retrieved for {} date. " \
                "Seems that it isn't available at data source".format(cache_date.isoformat())
            raise ResourceWarning(msg)


class ExtractOOSOperatorLocalFS(ExtractOOSOperator):

    def _get_cache_strategy(self
                          , data_source:DailyDataSource
                          , cache_date:date=None) -> Cache:
        logging.info(f"Creating local filesystem cache to store OOS data...")
        cache = FileDailyCache(self._data_directory, data_source, cache_date)
        logging.info(f"Local filesystem cache object for storing OOS data has been created.")
        return cache


class ExtractOOSOperatorHDFS(ExtractOOSOperator):

    def __init__(self
               , hdfs_conn_id:str = DEFAULT_HDFS_CONN_ID
               , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hdfs_conn_id = hdfs_conn_id

    def _get_cache_strategy(self
                          , data_source:DailyDataSource
                          , cache_date:date=None) -> Cache:
        creds = __get_hdfs_creds(self._hdfs_conn_id)
        logging.info(f"Create client and connect to HDFS at {creds['url']}...")
        hdfs_client = InsecureClient(**creds)
        logging.info(f"Client has been created.")
        logging.info(f"Creating HDFS cache to store OOS data...")
        cache = HDFSDailyCache(hdfs_client, self._data_directory, data_source, cache_date)
        logging.info(f"HDFS cache object for storing OOS data has been created.")
        return cache
