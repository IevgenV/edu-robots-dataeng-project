import json

from airflow.hooks.base_hook import BaseHook


class Credentials:

    DEFAULT_DB_CONN_ID = "dshop_db_server"
    DEFAULT_HDFS_CONN_ID = "hdfs_server"
    DEFAULT_SERVER_NAME = "prod_server"
    DEFAULT_CONN_SPARK_ID = "spark_server"
    DEFAULT_CONN_GOLD_DB_ID = "gold_db_server"

    DEFAULT_HDFS_CREDS = {
            'url': "http://127.0.0.1:50070"
          , 'user': "user"
    }

    DEFAULT_PGDB_CREDS = {
          'host': 'localhost'
        , 'port': '5432'
        , 'database': 'dshop'
        , 'user': 'pguser'
        , 'password': ''
    }

    DEFAULT_GOLD_DB_CREDS = {
          'host': 'localhost'
        , 'port': '5432'
        , 'database': 'gold'
        , 'user': 'pguser'
        , 'password': ''
    }

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

    @staticmethod
    def get_oos_creds(conn_id:str) -> dict:
        conn_oos = BaseHook.get_connection(conn_id)
        return {
            conn_id : {
                'address': conn_oos.host,
                'login': conn_oos.login,
                'password': conn_oos.password,
                'apis': json.loads(conn_oos.extra)
            }
        }

    @staticmethod
    def get_hdfs_creds(conn_id:str) -> dict:
        conn_hdfs = BaseHook.get_connection(conn_id)
        return {
            'url': ":".join([conn_hdfs.host, str(conn_hdfs.port)]),
            'user': conn_hdfs.login
        }

    @staticmethod
    def get_pg_creds(conn_id:str) -> dict:
        conn_dshop = BaseHook.get_connection(conn_id)
        return {
            'host': conn_dshop.host
            , 'port': conn_dshop.port
            , 'database': "dshop"
            , 'user': conn_dshop.login
            , 'password': conn_dshop.password
        }

    @staticmethod
    def get_gold_creds(conn_id:str) -> dict:
        conn_dshop = BaseHook.get_connection(conn_id)
        return {
            'host': conn_dshop.host
            , 'port': conn_dshop.port
            , 'database': "gold"
            , 'user': conn_dshop.login
            , 'password': conn_dshop.password
        }

    @staticmethod
    def get_spark_creds() -> dict:
        conn_spark = BaseHook.get_connection(CONN_SPARK_ID)
        return {
            'master': conn_spark.host
        }