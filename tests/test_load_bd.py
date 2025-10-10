import unittest
import shutil
import os
from pyspark.sql import SparkSession, Row
from cores.load_bd import LoadBD
from cores.utils import FileType

class TestLoadBD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Crée la session Spark pour tous les tests
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestLoadBD") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Ferme la session Spark après tous les tests
        cls.spark.stop()

    def setUp(self):
        # Crée un DataFrame de test
        data = [
            Row(name="Alice", age=35),
            Row(name="Bob", age=40)
        ]
        self.df = self.spark.createDataFrame(data)

        # Dossier temporaire pour le test (dans ton chemin spécifique)
        self.test_output = "/home/salimata/ana_path/test_output_loadbd"

        # Supprime le dossier s’il existe déjà
        if os.path.exists(self.test_output):
            shutil.rmtree(self.test_output)

    def tearDown(self):
        # Nettoyage du dossier après chaque test
        if os.path.exists(self.test_output):
            shutil.rmtree(self.test_output)

    def test_execute(self):
        # Instancie LoadBD
        loader = LoadBD(file_type=FileType.JSON, output_path=self.test_output)

        # Appelle la méthode execute
        df_result = loader.execute(self.df)

        # Vérifie que le DataFrame retourné est le même
        self.assertEqual(df_result.collect(), self.df.collect())

        # Vérifie que le dossier de sortie a été créé
        self.assertTrue(os.path.exists(self.test_output))

if __name__ == "__main__":
    unittest.main()
