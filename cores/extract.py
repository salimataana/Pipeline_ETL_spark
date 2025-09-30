from typing import Dict

from pyspark.sql import SparkSession
from cores.step import Step
from cores.utils import FileType
import os  # pour dvoir l'extension agir avec notre SE

class Extract(Step):

    def __init__(self, file_type: FileType, path, spark: SparkSession):
        self.kind = "extract"
        self.file_type= file_type
        self.path = path
        self.spark = spark  # SparkSession ajouter


    def execute(self, data=None):

        df = self.spark.read.format(self.file_type.value).option("header", True).load(self.path)

        return df
