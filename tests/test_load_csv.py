import unittest
import tempfile
from pyspark.sql import SparkSession, Row
from cores.utils import FileType
from cores.load_csv import Load


class TestLoadStep(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("LoadUnitTestSimple") \
            .getOrCreate()

        cls.test_df = cls.spark.createDataFrame([
            Row(col1=1, col2="A"),
            Row(col1=2, col2="B")
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_execute_returns_df(self):
        # ✅ Utilisation d’un répertoire temporaire
        with tempfile.TemporaryDirectory() as tmpdir:
            load_step = Load(file_type=FileType.CSV, output_path=tmpdir)
            result_df = load_step.execute(self.test_df)
            self.assertEqual(result_df.count(), self.test_df.count())


if __name__ == "__main__":
    unittest.main()
