from os import environ as env
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

def convert_to_spark(pandas_df):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Spark y Redshift") \
        .config("spark.jars", env['DRIVER_PATH']) \
        .config("spark.executor.extraClassPath", env['DRIVER_PATH']) \
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
        # Removemos los duplicados y los nulos

        self.spark_df = self \
            .spark_df.distinct() \
            .na.drop()

    def connector(self):
        self.spark_df.write \
            .format("jdbc") \
            .option("url",
                    f"jdbc:postgresql://{env['AWS_REDSHIFT_HOST']}:{env['AWS_REDSHIFT_PORT']}/{env['AWS_REDSHIFT_DATABASE']}") \
            .option("dbtable", f"{env['AWS_REDSHIFT_SCHEMA']}.{self.table}") \
            .option("user", env['AWS_REDSHIFT_USER']) \
            .option("password", env['AWS_REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
