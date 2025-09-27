from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_dataframe import transform_dataframe
from cores.utils import DataSourceType

class Transform(Step):

    def __init__(self, source_type: DataSourceType):
        self.kind = "transform"
        self.data_source_type = source_type

    def execute(self, df: DataFrame) -> DataFrame:
        # Appel de la fonction transform_dataframe pour appliquer les transformations
        return transform_dataframe(df)
