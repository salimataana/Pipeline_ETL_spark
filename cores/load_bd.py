from cores.step import Step
from cores.utils import FileType


class LoadBD(Step):

    def __init__(self, file_type: FileType, output_path):
        self.kind = "load"
        self.file_type= file_type
        self.output_path = output_path

    def execute(self, dataf_transformed):
        # On va écrire le DataFrame Spark en json pour la sortie
        dataf_transformed.write.mode("overwrite").json(self.output_path)

        print(f"Données enregistrées dans : {self.output_path}")
        return dataf_transformed
