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
      dag_id='dshop_dag_0_0_2'
    , description='Load data from `dshop` database to Bronze, clear and verify then put into the Silver. After all, load data to Gold Greenplum database.'
    , schedule_interval='@daily'
    , start_date=datetime(2021, 1, 1, 5)  # <- load data each morning at 5 a.m.
    , default_args=DEFAULT_ARGS
)

table_names = [
      "ordersproducts"
    , "departments"
    , "aisles"
    , "clients"
    , "stores"
    , "store_types"
    , "location_areas"
]
extract_tasks = []
transform_tasks = []
load_tasks = []

with dag:
    wait_extract_task = DummyOperator(
          task_id="wait_dshop_extraction_step"
        , dag=dag
    )

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
              task_id=f"transform_dshop_bronze_{table_name}"
            , provide_context=True
            , src_file_ext="csv"
            , dst_file_ext="parquet"
            , hdfs_conn_id=Credentials.DEFAULT_HDFS_CONN_ID
            , src_path=DATA_PATH_OOS_BRONZE
            , dst_path=DATA_PATH_OOS_SILVER
            , spark_master="local"
            , spark_app_name="transform_dshop_app"
        )
        transform_tasks.append(transform_task)

for extract_task, transform_task in zip(extract_tasks, transform_tasks):
    extract_task >> wait_extract_task >> transform_task
