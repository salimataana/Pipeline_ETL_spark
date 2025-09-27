from cores.step import Step
from cores.utils import DataSourceType

class Load(Step):

    def __init__(self, source_type: DataSourceType, output_path):
        self.kind = "load"
        self.data_source_type = source_type
        self.output_path = output_path

    def execute(self, df):
        # On va écrire le DataFrame Spark  en CSV pour la sortie
        df.write.option("header", True).csv(self.output_path)

        print(f"Données enregistrées dans : {self.output_path}")
        return df
