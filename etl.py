from os import environ as env
from pyspark.sql import SparkSession
import os


def convert_to_spark(pandas_df):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Spark y Redshift") \
        .config("spark.jars", os.getenv('DRIVER_PATH')) \
        .config("spark.executor.extraClassPath", os.getenv('DRIVER_PATH')) \
        .config("spark.driver.extraClassPath", os.getenv('DRIVER_PATH')) \
        .getOrCreate()

    return spark.createDataFrame(pandas_df)


class Etl:

    def __init__(self, pandas_df, table):
        self.table = table
        self.spark_df = convert_to_spark(pandas_df)

    def execute(self):
        self.transform()
        self.connector()

    def transform(self):
        print("INICIO: transformando datos...")
        # Removemos los duplicados y los nulos

        self.spark_df = self \
            .spark_df.distinct() \
            .na.drop()
        print("FIN: datos transformados.")

    def connector(self):
        print("INICIO: insertando datos...")
        self.spark_df.write \
            .format("jdbc") \
            .option("url",
                    f"jdbc:postgresql://{os.getenv('AWS_REDSHIFT_HOST')}:{os.getenv('AWS_REDSHIFT_PORT')}/{os.getenv('AWS_REDSHIFT_DATABASE')}") \
            .option("dbtable", f"{os.getenv('AWS_REDSHIFT_SCHEMA')}.{self.table}") \
            .option("user", os.getenv('AWS_REDSHIFT_USER')) \
            .option("password", os.getenv('AWS_REDSHIFT_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("FIN: datos insertados")
