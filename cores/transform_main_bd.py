from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_bd import transform_base_de_donnes
from cores.utils import FileType

class TransformBD(Step):

    def __init__(self):
        self.kind = "transform"


    def execute(self, dataf: DataFrame) -> DataFrame:
        # Appel de la fonction transform_base_de_donnes pour appliquer les transformations
        return transform_base_de_donnes(dataf)
