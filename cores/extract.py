from pyspark.sql import SparkSession
from cores.step import Step
from cores.utils import DataSourceType

class Extract(Step):

    def __init__(self, source_type: DataSourceType, path, spark: SparkSession):
        self.kind = "extract"
        self.data_source_type = source_type
        self.path = path
        self.spark = spark  # SparkSession injectée

    def execute(self, data=None):
        if self.data_source_type == DataSourceType.FILE:
            # Lire CSV avec Spark
            df = self.spark.read.option("header", True).option("inferSchema", True).csv(self.path)
        return df
