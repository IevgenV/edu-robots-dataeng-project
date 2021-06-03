import abc
import logging
import pathlib
from datetime import date

from pyspark.sql.types import IntegerType
from utils.spark import SparkDefaults, open_file_as_df

import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession

from utils.creds import Credentials

from operators.SparkOperator import SparkOperator


class LoadOperator(SparkOperator):

    def __init__(self
               , dst_db_conn_id:str = Credentials.DEFAULT_CONN_GOLD_DB_ID
               , jdbc_driver_path:str = SparkDefaults.DEFAULT_SPARK_JDBC_DRIVER_PATH
               , *args, **kwargs):
        self._dst_db_conn_id = dst_db_conn_id
        self.jdbc_driver_path = jdbc_driver_path
        self.load_date = date.today()
        super().__init__(*args, **kwargs)

    def spark_create(self, spark_master, spark_app_name):
        return SparkSession.builder \
                           .config("spark.driver.extraClassPath", self.jdbc_driver_path) \
                           .master(spark_master) \
                           .appName(spark_app_name) \
                           .getOrCreate()

    @abc.abstractmethod
    def load_data(self, src_path:pathlib.Path, conn_url:str, conn_creds:dict) -> bool:
        pass

    def execute(self, context):
        db_creds = Credentials.get_pg_creds(self._dst_db_conn_id)
        load_date = context.get("execution_date")

        db_creds = db_creds if db_creds is not None \
                   else Credentials.DEFAULT_GOLD_DB_CREDS
        self.load_date = self.load_date if load_date is None \
                         else load_date.date()

        date_dir = pathlib.Path(self.load_date.isoformat())
        src_path = self.src_path / date_dir

        logging.info("Parameters for data load stafge were parsed:")
        logging.info(f"Source data directory: {src_path}")

        conn_url = f"jdbc:postgresql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
        conn_creds = {"user": db_creds["user"], "password": db_creds["password"]}

        self.load_data(src_path, conn_url, conn_creds)


class LoadDShopOperator(LoadOperator):

    def load_data(self, src_path:pathlib.Path, conn_url:str, conn_creds:dict) -> bool:
        logging.info("Load silver data to DataFrames...")
        orders_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("orders.parquet"))
        products_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("products.parquet"))
        departments_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("departments.parquet"))
        aisles_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("aisles.parquet"))
        clients_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("clients.parquet"))
        stores_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("stores.parquet"))
        store_types_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("store_types.parquet"))
        location_areas_tbl_df = open_file_as_df(self.spark, src_path / pathlib.Path("location_areas.parquet"))

        logging.info("Load data to `products` table at Gold layer...")
        gold_products_df = products_tbl_df \
            .join(aisles_tbl_df, on="aisle_id") \
            .join(departments_tbl_df, on="department_id") \
            .select(products_tbl_df.product_id
                  , products_tbl_df.product_name
                  , departments_tbl_df.department_name
                  , aisles_tbl_df.aisle)

        gold_products_df.write.jdbc(conn_url, "products", mode="overwrite", properties=conn_creds)

        logging.info("Load data to `location_areas` table at Gold layer...")
        location_areas_tbl_df.write.jdbc(conn_url, "location_areas", mode="overwrite", properties=conn_creds)

        logging.info("Load data to `clients` table at Gold layer...")
        gold_clients_df = clients_tbl_df \
            .select(clients_tbl_df.id.alias("client_id")
                  , clients_tbl_df.fullname
                  , clients_tbl_df.location_area_id)

        gold_clients_df.write.jdbc(conn_url, "clients", mode="overwrite", properties=conn_creds)

        logging.info("Load data to `stores` table at Gold layer...")
        gold_stores_df = stores_tbl_df \
            .join(store_types_tbl_df, on="store_type_id") \
            .select(stores_tbl_df.store_id
                  , stores_tbl_df.location_area_id
                  , store_types_tbl_df.type.alias("store_type"))

        gold_stores_df.write.jdbc(conn_url, "stores", mode="overwrite", properties=conn_creds)

        logging.info("Load data to `dates` table at Gold layer...")
        gold_dates_df = orders_tbl_df \
            .sort(orders_tbl_df.order_date) \
            .select(orders_tbl_df.order_date
                  , F.weekofyear(orders_tbl_df.order_date).alias('date_week')
                  , F.month(orders_tbl_df.order_date).alias('date_month')
                  , F.year(orders_tbl_df.order_date).alias('date_year')
                  , F.dayofweek(orders_tbl_df.order_date).alias('date_weekday'))

        gold_dates_df.write.jdbc(conn_url, "dates", mode="ignore", properties=conn_creds)

        logging.info("Load data to `orders` table at Gold layer...")
        gold_orders_df = orders_tbl_df \
            .select(orders_tbl_df.product_id
                  , orders_tbl_df.client_id
                  , orders_tbl_df.order_date
                  , orders_tbl_df.store_id
                  , orders_tbl_df.quantity)

        gold_orders_df.write.jdbc(conn_url, "orders", mode="overwrite", properties=conn_creds)


class LoadOOSOperator(LoadOperator):

    def load_data(self, src_path:pathlib.Path, conn_url:str, conn_creds:dict) -> bool:
        logging.info("Load silver data to DataFrames...")
        src_file_name = ".".join(self.load_date.isoformat(), "json")
        oos_src_df = open_file_as_df(self.spark, src_path / pathlib.Path(src_file_name))

        logging.info("Load data to `dates` table at Gold layer...")
        gold_dates_df = oos_src_df \
            .sort(oos_src_df.date) \
            .select(oos_src_df.date
                  , F.weekofyear(oos_src_df.date).alias('date_week')
                  , F.month(oos_src_df.date).alias('date_month')
                  , F.year(oos_src_df.date).alias('date_year')
                  , F.dayofweek(oos_src_df.date).alias('date_weekday'))
        gold_dates_df = gold_dates_df.dropDuplicates()
        gold_dates_df.write.jdbc(conn_url, "dates", mode="ignore", properties=conn_creds)

        logging.info("Load data to `out_of_stock` table at Gold layer...")
        gold_oos_df = oos_src_df \
            .select(oos_src_df.product_id, oos_src_df.date) \
            .withColumn('store_id', F.lit(None).cast(IntegerType()))
        
        gold_oos_df.write.jdbc(conn_url, "out_of_stock", mode="append", properties=conn_creds)