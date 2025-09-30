from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_dataframe import transform_dataframe
from cores.utils import FileType


class Transform(Step):

    def __init__(self, file_type: FileType):
        self.kind = "transform"
        self.file_type = file_type

    def execute(self, df: DataFrame) -> DataFrame:
        # Appel de la fonction transform_dataframe pour appliquer les transformations
        return transform_dataframe(df)
