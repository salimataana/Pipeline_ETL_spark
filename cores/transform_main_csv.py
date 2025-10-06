from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_csv import transform_dataframe



class TransformCSV(Step):

    def __init__(self):
        self.kind = "transform"


    def execute(self, df: DataFrame) -> DataFrame:
        # Appel de la fonction transform_dataframe pour appliquer les transformations
        return transform_dataframe(df)
