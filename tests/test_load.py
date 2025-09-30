import unittest
from pyspark.sql import SparkSession, Row
from cores.utils import FileType
from cores.load import Load  # adapte le nom du fichier si nécessaire


class TestLoadStepSimple(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Créer une SparkSession
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("LoadUnitTestSimple") \
            .getOrCreate()

        # DataFrame de test
        cls.test_df = cls.spark.createDataFrame([
            Row(col1=1, col2="A"),
            Row(col1=2, col2="B")
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_execute_returns_df(self):
        # Créer l'instance Load avec un chemin fictif
        load_step = Load(file_type=FileType.CSV, output_path="/home/salimata/ana_path")

        # Appeler execute()
        result_df = load_step.execute(self.test_df)

        # Vérifier que le DataFrame retourné est le même (nombre de lignes)
        self.assertEqual(result_df.count(), self.test_df.count())


if __name__ == "__main__":
    unittest.main()
