import unittest
import os
from pyspark.sql import SparkSession
from cores.utils import FileType
from cores.extract_csv import Extract



class TestExtract(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 1 SparkSession pour les tests
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("ExtractUnitTest") \
            .getOrCreate()

        # 2️⃣ Créer un petit fichier CSV temporaire pour le test
        cls.test_csv_path = "test_data.csv"
        with open(cls.test_csv_path, "w") as f:
            f.write("name,age\nAlice,30\nBob,25\n")

    @classmethod
    def tearDownClass(cls):
        # 5 Nettoie après tous les tests
        cls.spark.stop()
        if os.path.exists(cls.test_csv_path):
            os.remove(cls.test_csv_path)

    def test_extract_csv(self):
        # instancie la classe Extract avec le fichier CSV
        extract_step = Extract(file_type=FileType.CSV,path=self.test_csv_path, spark=self.spark)

        # 4️Executer la méthode
        df = extract_step.execute()

        # Verifier que le DataFrame est correct
        self.assertEqual(df.count(), 2)  # 2 lignes
        self.assertEqual(len(df.columns), 2)  # 2 colonnes
        self.assertListEqual(df.columns, ["name", "age"])

        result = df.where(df.name.isin("Alice", "Bob")).collect()
        # Vérifier les valeurs
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["name"], "Bob")
        self.assertEqual(int(result[1]["age"]), 25)



if __name__ == "__main__":
    unittest.main()
