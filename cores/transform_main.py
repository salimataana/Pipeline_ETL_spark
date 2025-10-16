from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_csv import transform_dataframe
from cores.transform_bd import transform_base_de_donnes


class TransformCSV(Step):
    def __init__(self):
        self.kind = "transform"

    def execute(self, df: DataFrame) -> DataFrame:
        # Appel de la fonction transform_dataframe
        return transform_dataframe(df)


class TransformBD(Step):
    def __init__(self):
        self.kind = "transform"

    def execute(self, dataf: DataFrame) -> DataFrame:
        # Appel de la fonction transform_base_de_donnes
        return transform_base_de_donnes(dataf)
