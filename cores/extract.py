from pyspark.sql import SparkSession
from cores.step import Step
from cores.utils import DataSourceType
import os  # pour dvoir l'extension agir avec notre SE

class Extract(Step):

    def __init__(self, source_type: DataSourceType, path, spark: SparkSession):
        self.kind = "extract"
        self.data_source_type = source_type
        self.path = path
        self.spark = spark  # SparkSession ajouter

    def execute(self, data=None):
        if self.data_source_type == DataSourceType.FILE:
            # Détecter l'extension du fichier
            ext = os.path.splitext(self.path)[1].lower()

            if ext == ".csv":
                df = self.spark.read.option("header", True).option("inferSchema", True).csv(self.path)
            elif ext == ".json":
                df = self.spark.read.option("inferSchema", True).json(self.path)
            elif ext == ".parquet":
                df = self.spark.read.parquet(self.path)
            elif ext == ".orc":
                df = self.spark.read.orc(self.path)
            elif ext == ".avro":
                df = self.spark.read.format("avro").load(self.path)  # avro nécessite le package spark-avro sinon on aura des souci donc on doit l'installer
            elif ext == ".xml":
                df = self.spark.read.format("xml").option("rowTag", "row").load(self.path)  # xml nécessite spark-xml pour pouvoir lire le fichier Xml parce que c'est pas inclus dans sparlk donc on doit l'installer pour pouvoir utiliser le fichier de format xml
            else:
                raise ValueError(f"Format de fichier non reconnu : {ext}")

        return df
