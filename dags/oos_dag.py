from utils.spark import SparkDefaults
from operators.LoadOperator import LoadOOSOperator
import pathlib
from datetime import datetime

from airflow import DAG
from operators.ExtractOOSOperator import ExtractOOS2HDFSOperator
from operators.TransformOOSOperator import TransformOOSOperator
from utils.creds import Credentials


# Path to the directory where the out-of-stock data has to be stored/cached in json
DATA_PATH_OOS_BRONZE = pathlib.Path("/bronze/oos")

# Path to the directory where the out-of-stock data has to be stored/cached in parquet
DATA_PATH_OOS_SILVER = pathlib.Path("/silver/oos")

DEFAULT_ARGS = {
      'owner': 'airflow'
    , 'email': ['airflow@airflow.com']
    , 'email_on_failure': False
    , 'retries': 2
}

dag = DAG(
      dag_id='oos_dag_0_1_0'
    , description='Load OOS Data from remote API to HDFS Bronze, clear and verify, then put into the Silver.'
    , schedule_interval='@daily'
    , start_date=datetime(2021, 1, 1, 23)  # <- load data each evening at 11 p.m.
    , default_args=DEFAULT_ARGS
)

with dag:
    oos_extract_task = ExtractOOS2HDFSOperator(
          task_id='extract_oos_bronze'
        , provide_context=True
        , hdfs_conn_id=Credentials.DEFAULT_HDFS_CONN_ID
        , data_directory=DATA_PATH_OOS_BRONZE
        , oos_conn_id=Credentials.DEFAULT_SERVER_NAME
    )

    oos_transform_task = TransformOOSOperator(
          task_id='transform_oos_silver'
        , provide_context=True
        , src_file_ext="json"
        , dst_file_ext="parquet"
        , hdfs_conn_id=Credentials.DEFAULT_HDFS_CONN_ID
        , src_path=DATA_PATH_OOS_BRONZE
        , dst_path=DATA_PATH_OOS_SILVER
        , spark_master="local"
        , spark_app_name="transform_oos_app"
    )

    oos_load_task = LoadOOSOperator(
              task_id="load_oos_gold"
            , provide_context=True
            , dst_db_conn_id = Credentials.DEFAULT_CONN_GOLD_DB_ID
            , jdbc_driver_path = SparkDefaults.DEFAULT_SPARK_JDBC_DRIVER_PATH
            , src_path=DATA_PATH_OOS_SILVER
            , spark_master="local"
            , spark_app_name="load_oos_app"
    )

oos_extract_task >> oos_transform_task >> oos_load_task
