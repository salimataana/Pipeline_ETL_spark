from pyspark.sql import DataFrame
from cores.step import Step
from cores.transform_csv import transform_dataframe
from cores.transform_base_de_donnes import transform_base_de_donnes
from cores.utils import FileType


class Transform(Step):

    def execute(self, df: DataFrame, dataf: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Applique les transformations sur deux DataFrames différents :
        - df : transform_dataframe
        - dataf : transform_base_de_donnes
        Retourne un tuple avec les deux DataFrames transformés.
        """
        # Transformer le premier DataFrame
        df_transformed = transform_dataframe(df)

        # Transformer le deuxième DataFrame
        dataf_transformed = transform_base_de_donnes(dataf)

        # Retourner les deux résultats
        return df_transformed, dataf_transformed
