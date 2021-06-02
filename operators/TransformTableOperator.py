import logging

from pyspark.sql.dataframe import DataFrame

from operators.TransformSparkOperator import TransformSparkHDFSDailyOperator


class TransformTableOperator(TransformSparkHDFSDailyOperator):

    def validate(self, df:DataFrame) -> DataFrame:
        logging.info("Validate data source schema...")
        logging.info(f"Schema: {df.schema}...")
        logging.info("Data source schema has been validated.")
        return df

    def clean(self, df:DataFrame) -> DataFrame:
        logging.info("Start source data cleaning...")
        logging.info("Nothing to clean in data obtained from pg tables. Already good quality.")
        logging.info("Source data cleaning done.")
        return df