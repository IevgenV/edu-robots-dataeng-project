import abc
import logging
import pathlib
from datetime import date
from pyspark.conf import SparkConf
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.types import DateType, IntegerType
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
        self.conn_url = None
        self.conn_creds = None

    @abc.abstractmethod
    def load_data(self, src_path:pathlib.Path) -> bool:
        pass

    def execute(self, context):
        db_creds = Credentials.get_gold_creds(self._dst_db_conn_id)
        load_date = context.get("execution_date")

        db_creds = db_creds if db_creds is not None \
                   else Credentials.DEFAULT_GOLD_DB_CREDS
        self.load_date = self.load_date if load_date is None \
                         else load_date.date()

        date_dir = pathlib.Path(self.load_date.isoformat())
        src_path = self.src_path / date_dir
        self.conn_url = f"jdbc:postgresql://{db_creds['host']}:{db_creds['port']}/{db_creds['database']}"
        self.conn_creds = {"user": db_creds["user"], "password": db_creds["password"]}

        logging.info("Parameters for data load stage were parsed:")
        logging.info(f"Source data directory: {src_path}")
        logging.info(f"Connection URL: {self.conn_url}")

        self.load_data(src_path)

    def _merge2db(self, df:DataFrame, dst_table_name:str, join_cols:list) -> DataFrame:
        old_df = self.spark.read.jdbc(self.conn_url
                                    , dst_table_name
                                    , properties=self.conn_creds)
        logging.info("Prevousy {} records existed in {}".format(old_df.count(), dst_table_name))
        new_df = df.join(old_df, on=join_cols, how="left_anti")
        logging.info("New {} records to be added into {}.".format(new_df.count(), dst_table_name))
        new_df.write.jdbc(self.conn_url, dst_table_name, mode="append", properties=self.conn_creds)
        return new_df


class LoadDShopOperator(LoadOperator):

    def load_data(self, src_path:pathlib.Path) -> bool:
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
        self._merge2db(gold_products_df, "public.products", "aisle_id")
        logging.info("Load data to `products` table at Gold layer done.")

        logging.info("Load data to `location_areas` table at Gold layer...")
        logging.info(f"`products` schema: {location_areas_tbl_df.schema}")
        self._merge2db(location_areas_tbl_df, "public.location_areas", "area_id")
        logging.info("Load data to `location_areas` table at Gold layer done.")

        logging.info("Load data to `clients` table at Gold layer...")
        gold_clients_df = clients_tbl_df \
            .select(clients_tbl_df.id.alias("client_id")
                  , clients_tbl_df.fullname
                  , clients_tbl_df.location_area_id)
        self._merge2db(gold_clients_df, "public.clients", "client_id")
        logging.info("Load data to `clients` table at Gold layer done.")

        logging.info("Load data to `stores` table at Gold layer...")
        gold_stores_df = stores_tbl_df \
            .join(store_types_tbl_df, on="store_type_id") \
            .select(stores_tbl_df.store_id
                  , stores_tbl_df.location_area_id
                  , store_types_tbl_df.type.alias("store_id"))
        self._merge2db(gold_stores_df, "public.stores", "client_id")
        logging.info("Load data to `stores` table at Gold layer done.")

        logging.info("Load data to `dates` table at Gold layer...")
        gold_dates_df = orders_tbl_df \
            .sort(orders_tbl_df.order_date) \
            .select(orders_tbl_df.order_date
                  , F.weekofyear(orders_tbl_df.order_date).alias('date_week')
                  , F.month(orders_tbl_df.order_date).alias('date_month')
                  , F.year(orders_tbl_df.order_date).alias('date_year')
                  , F.dayofweek(orders_tbl_df.order_date).alias('date_weekday'))
        self._merge2db(gold_dates_df, "public.dates", ["order_date"])
        logging.info("Load data to `dates` table at Gold layer done.")

        logging.info("Load data to `orders` table at Gold layer...")
        gold_orders_df = orders_tbl_df \
            .select(orders_tbl_df.product_id
                  , orders_tbl_df.client_id
                  , orders_tbl_df.order_date
                  , orders_tbl_df.store_id
                  , orders_tbl_df.quantity)
        self._merge2db(gold_orders_df, "public.orders", ["product_id", "client_id", "order_date", "store_id"])
        logging.info("Load data to `orders` table at Gold layer done.")

        logging.info("Load silver data to DataFrames done.")


class LoadOOSOperator(LoadOperator):

    def load_data(self, src_path:pathlib.Path) -> bool:
        logging.info("Load silver data to DataFrames...")
        src_file_name = ".".join([self.load_date.isoformat(), "parquet"])
        oos_src_df = open_file_as_df(self.spark, src_path / pathlib.Path(src_file_name))

        logging.info("Load data to `dates` table at Gold layer...")
        gold_dates_df = oos_src_df \
            .sort(oos_src_df.date) \
            .select(oos_src_df.date.cast(DateType()).alias("order_date")
                  , F.weekofyear(oos_src_df.date).alias('date_week')
                  , F.month(oos_src_df.date).alias('date_month')
                  , F.year(oos_src_df.date).alias('date_year')
                  , F.dayofweek(oos_src_df.date).alias('date_weekday'))
        unique_dates_df = gold_dates_df.dropDuplicates(["order_date"])
        self._merge2db(unique_dates_df, "public.dates", "order_date")
        logging.info("Done loading data to `dates` table at Gold layer")

        logging.info("Load data to `out_of_stock` table at Gold layer...")
        gold_oos_df = oos_src_df \
            .select(oos_src_df.product_id.cast(IntegerType())
                  , oos_src_df.date.cast(DateType()).alias("order_date")) \
            .withColumn('store_id', F.lit(None).cast(IntegerType()))
        self._merge2db(gold_oos_df, "public.out_of_stock", ["product_id", "order_date"])
        logging.info("Done loading data to `out_of_stock` table at Gold layer")
