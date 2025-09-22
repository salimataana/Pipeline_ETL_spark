from cores.step import Step
from cores.utils import DataSourceType

class Load(Step):

    def __init__(self, source_type: DataSourceType, output_path):
        self.kind = "load"
        self.data_source_type = source_type
        self.output_path = output_path

    def execute(self, df):
        # Coalesce pour avoir une seule partition et écrire en un seul fichier CSV
        df.coalesce(1).write.option("header", True).mode("overwrite").csv(self.output_path + "_temp")

        # Renommer le fichier unique généré par Spark
        import os
        import shutil
        temp_folder = self.output_path + "_temp"
        for filename in os.listdir(temp_folder):
            if filename.endswith(".csv"):
                shutil.move(os.path.join(temp_folder, filename), self.output_path)
        # Supprimer le dossier temporaire
        shutil.rmtree(temp_folder)

        print(f"Données enregistrées dans : {self.output_path}")
        return df
