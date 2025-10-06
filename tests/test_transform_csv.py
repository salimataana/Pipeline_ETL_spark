import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from cores.transform_csv import transform_dataframe

class TestTransformDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Créer SparkSession pour les tests
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TransformDFUnitTest") \
            .getOrCreate()

        # Créer un DataFrame de test
        cls.test_df = cls.spark.createDataFrame([
            Row(**{
                "Common Name": "lion",
                "Scientific Name": "panthera leo",
                "Date of Observation": "12-05-2020",
                "Observed Weight (kg)": 190,
                "Age Class": "Adult",
                "Habitat Type": "Swamps"
            }),
            Row(**{
                "Common Name": "elephant",
                "Scientific Name": "loxodonta africana",
                "Date of Observation": "01-01-2019",
                "Observed Weight (kg)": 500,
                "Age Class": "Juvenile",
                "Habitat Type": "Mangroves"
            }),
            Row(**{
                "Common Name": "tiger",
                "Scientific Name": "panthera tigris",
                "Date of Observation": "15-03-2021",
                "Observed Weight (kg)": 90,
                "Age Class": "Adult",
                "Habitat Type": "Mangroves"
            }),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_dataframe(self):
        # Appliquer la fonction
        result_df = transform_dataframe(self.test_df)
        result_data = result_df.collect()

        # Vérifier que seules les lignes Adultes et des habitats sélectionnés sont conservées
        self.assertEqual(len(result_data), 2)  # lion et tiger

        # Vérifier que les noms sont en majuscules
        names = [row["Common Name"] for row in result_data]
        self.assertTrue(all(name.isupper() for name in names))

        scientific_names = [row["Scientific Name"] for row in result_data]
        self.assertTrue(all(name.isupper() for name in scientific_names))

        # Vérifier la colonne Weight_Category
        weights = [row["Weight_Category"] for row in result_data]
        self.assertListEqual(weights, ["Medium", "Light"])

if __name__ == "__main__":
    unittest.main()
