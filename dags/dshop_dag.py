from utils.spark import SparkDefaults
from operators.LoadOperator import LoadDShopOperator
import pathlib
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.ExtractTableOperator import ExtractPGTable2HDFSOperator
from operators.TransformTableOperator import TransformTableOperator
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
      dag_id='dshop_dag_0_0_4'
    , description='Load data from `dshop` database to Bronze, clear and verify then put into the Silver. After all, load data to Gold Greenplum database.'
    , schedule_interval='@daily'
    , start_date=datetime(2021, 6, 2, 5)  # <- load data each morning at 5 a.m.
    , default_args=DEFAULT_ARGS
)

extract_tasks = []
transform_tasks = []
table_names = [
      "orders",  "products", "departments", "aisles"
    , "clients", "stores",   "store_types", "location_areas"
]
with dag:
    for table_name in table_names:
        extract_task = ExtractPGTable2HDFSOperator(
              task_id=f"extract_dshop_bronze_{table_name}"
            , provide_context=True
            , hdfs_conn_id=Credentials.DEFAULT_HDFS_CONN_ID
            , table_name=table_name
            , data_directory=DATA_PATH_OOS_BRONZE
            , db_conn_id=Credentials.DEFAULT_DB_CONN_ID
        )
        extract_tasks.append(extract_task)

        transform_task = TransformTableOperator(
              task_id=f"transform_dshop_silver_{table_name}"
            , provide_context=True
            , src_file_ext="csv"
            , dst_file_ext="parquet"
            , src_file_name=table_name
            , dst_file_name=table_name
            , hdfs_conn_id=Credentials.DEFAULT_HDFS_CONN_ID
            , src_path=DATA_PATH_OOS_BRONZE
            , dst_path=DATA_PATH_OOS_SILVER
            , spark_master="local"
            , spark_app_name="dshop_app"
        )
        transform_tasks.append(transform_task)

    wait_extract_task = DummyOperator(
          task_id="wait_dshop_extraction_step"
    )

    wait_transform_task = DummyOperator(
          task_id="wait_dshop_transform_step"
    )

    load_task = LoadDShopOperator(
              task_id="load_dshop_gold"
            , provide_context=True
            , dst_db_conn_id = Credentials.DEFAULT_CONN_GOLD_DB_ID
            , jdbc_driver_path = SparkDefaults.DEFAULT_SPARK_JDBC_DRIVER_PATH
            , src_path=DATA_PATH_OOS_BRONZE
            , spark_master="local"
            , spark_app_name="dshop_app"
    )

for extract_task, transform_task in zip(extract_tasks, transform_tasks):
    extract_task >> wait_extract_task >> transform_task >> wait_transform_task >> load_task
