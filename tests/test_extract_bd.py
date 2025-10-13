from unittest.mock import MagicMock
from pyspark.sql import DataFrame
from cores.extract_bd import ExtractDB

class TestExtractDBSimple:
    def test_execute_with_data_bd(self):
        """Test que le paramètre data est ignoré (non utilisé dans cette classe)"""
        # Given
        spark_mock = MagicMock()
        mock_df = MagicMock(spec=DataFrame)

        # Tout appel en chaîne à spark.read.format().option().option()... load() retourne mock_df
        spark_mock.read.format().option().option().option().option().option().load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="test_user",
            password="test_password"
        )

        # When - appel avec paramètre data qui doit être ignoré
        dummy_data = MagicMock()
        result = extract_db.execute(data=dummy_data)

        # Then - vérifier que le résultat est bien mock_df
        assert result == mock_df
