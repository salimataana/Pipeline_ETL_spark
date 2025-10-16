from cores.step import Step
from cores.utils import FileType


class Load(Step):

    def __init__(self, file_type: FileType, output_path):
        self.kind = "load"
        self.file_type= file_type
        self.output_path = output_path

    def execute(self, df):
        # On va écrire le DataFrame Spark  en CSV pour la sortie
        # Écriture du DataFrame en CSV :
        # - mode "overwrite" : écrase le fichier si il existe déjà
        # - option "header=True" : inclut les noms de colonnes dans le CSV
        df.write.mode("overwrite").option("header", True).csv(self.output_path)
        print(f"Données enregistrées dans : {self.output_path}")
        return df
