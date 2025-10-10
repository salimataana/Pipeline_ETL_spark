import unittest
from pyspark.sql import SparkSession, Row
from cores.transform_bd import transform_base_de_donnes

class TestTransformBD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Crée une session Spark avant tous les tests
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestTransform") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Ferme la session Spark après tous les tests
        cls.spark.stop()

    def test_transform_base_de_donnes(self):
        # 1️⃣ Préparer des données de test
        data = [
            Row(name="Alice", age=25),    # doit être filtré
            Row(name="Bob", age=35),
            Row(name="Charlie", age=40)
        ]
        df = self.spark.createDataFrame(data)

        # 2️⃣ Appliquer la fonction de transformation
        df_transformed = transform_base_de_donnes(df)

        # 3️⃣ Collecter les résultats pour les vérifier
        result = df_transformed.collect()

        # 4️⃣ Vérifier que toutes les personnes filtrés ont bien plus de 30 ans (filtrage)
        self.assertTrue(all(row.age > 30 for row in result))

        # 5️⃣ Vérifier que la nouvelle colonne "age_in_5_years" est correcte
        for row in result:
            self.assertEqual(row.age_in_5_years, row.age + 5)

        # 6️⃣ Vérifier que les noms sont bien en majuscules
        for row in result:
            self.assertTrue(row.name.isupper())

        # 7️⃣ Vérifier que le DataFrame est trié par âge décroissant
        ages = [row.age for row in result]
        self.assertEqual(ages, sorted(ages, reverse=True))

if __name__ == "__main__":
    # Permet d’exécuter le test directement depuis ce fichier
    unittest.main()
