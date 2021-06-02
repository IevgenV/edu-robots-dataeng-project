import logging

from pyspark.sql.dataframe import DataFrame

from operators.TransformSparkOperator import TransformSparkHDFSDailyOperator


class TransformOOSOperator(TransformSparkHDFSDailyOperator):

    def validate(self, df:DataFrame) -> DataFrame:
        logging.info("Validate data source schema...")
        logging.info(f"Schema: {df.schema}...")
        logging.info("Data source schema has been validated.")
        return df

    def clean(self, df:DataFrame) -> DataFrame:
        logging.info("Start source data cleaning...")
        logging.info("Removing records with unknown values...")
        df = df.dropna()
        logging.info("Removing records with unknown values done.")
        logging.info("Removing duplicates...")
        df = df.dropDuplicates()
        logging.info(f"Removing duplicates done.")
        logging.info("Source data cleaning done.")
        return df
