import json

from airflow.hooks.base_hook import BaseHook


class Credentials:

    DEFAULT_DB_CONN_ID = "dshop_db_server"
    DEFAULT_HDFS_CONN_ID = "hdfs_server"
    DEFAULT_SERVER_NAME = "prod_server"
    CONN_SPARK_ID = "spark_server"

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
    def get_spark_creds() -> dict:
        conn_spark = BaseHook.get_connection(CONN_SPARK_ID)
        return {
            'master': conn_spark.host
        }