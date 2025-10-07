
from unittest.mock import Mock
from pyspark.sql import DataFrame
from cores.extract_bd import ExtractDB


class TestExtractDBSimple:

    def test_execute_success(self):
        """Test simplifié qui fonctionne"""
        # Créer un mock complet de Spark
        spark_mock = Mock()

        # Mock de toute la chaîne: spark.read.format().option().load()
        mock_reader = Mock()
        mock_format = Mock()
        mock_df = Mock(spec=DataFrame)

        spark_mock.read = mock_reader
        mock_reader.format.return_value = mock_format
        mock_format.option.return_value = mock_format  # Permet le chaînage
        mock_format.load.return_value = mock_df

        # Créer l'instance à tester
        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="test_user",
            password="test_password"
        )

        # Exécuter la méthode
        result = extract_db.execute()

        # Vérifications
        # 1. Vérifier l'appel à format("jdbc")
        spark_mock.read.format.assert_called_once_with("jdbc")

        # 2. Vérifier les options JDBC
        expected_options = [
            ("url", "jdbc:mysql://localhost:3306/test_db"),
            ("dbtable", "test_table"),
            ("user", "test_user"),
            ("password", "test_password"),
            ("driver", "com.mysql.cj.jdbc.Driver")
        ]

        for option_name, option_value in expected_options:
            mock_format.option.assert_any_call(option_name, option_value)

        # 3. Vérifier l'appel à load()
        mock_format.load.assert_called_once()

        # 4. Vérifier le retour
        assert result == mock_df

    def test_initialization_values(self):
        """Test des valeurs d'initialisation"""
        spark_mock = Mock()

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:postgresql://host/db",
            table="users",
            user="admin",
            password="secret"
        )

        assert extract_db.kind == "extract"
        assert extract_db.url == "jdbc:postgresql://host/db"
        assert extract_db.table == "users"
        assert extract_db.user == "admin"
        assert extract_db.password == "secret"
        assert extract_db.spark == spark_mock

    def test_execute_with_data_parameter_ignored(self):
        """Test que le paramètre data est ignoré (non utilisé dans cette classe)"""
        # Given
        spark_mock = Mock()
        mock_df = Mock(spec=DataFrame)
        spark_mock.read.format.return_value.option.return_value.load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="test_user",
            password="test_password"
        )

        # When - appel avec paramètre data qui doit être ignoré
        dummy_data = Mock()
        result = extract_db.execute(data=dummy_data)

        # Then - doit fonctionner normalement malgré le paramètre data
        spark_mock.read.format.assert_called_once_with("jdbc")
        assert result == mock_df

    def test_different_jdbc_urls(self):
        """Test avec différentes URLs JDBC"""
        spark_mock = Mock()
        mock_df = Mock(spec=DataFrame)
        spark_mock.read.format.return_value.option.return_value.load.return_value = mock_df

        # Test avec PostgreSQL
        extract_db_pg = ExtractDB(
            spark=spark_mock,
            url="jdbc:postgresql://localhost:5432/mydb",
            table="users",
            user="postgres",
            password="postgres"
        )

        result = extract_db_pg.execute()
        spark_mock.read.format.return_value.option.assert_any_call(
            "url", "jdbc:postgresql://localhost:5432/mydb"
        )
        assert result == mock_df

    def test_mysql_driver_specified(self):
        """Test que le driver MySQL est bien spécifié"""
        spark_mock = Mock()
        mock_df = Mock(spec=DataFrame)
        spark_mock.read.format.return_value.option.return_value.load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="test_user",
            password="test_password"
        )

        extract_db.execute()

        # Vérifier que le driver MySQL est bien spécifié
        spark_mock.read.format.return_value.option.assert_any_call(
            "driver", "com.mysql.cj.jdbc.Driver"
        )

    def test_method_call_chain(self):
        """Test l'ordre et la chaîne des méthodes appelées"""
        spark_mock = Mock()
        mock_reader = Mock()
        mock_format = Mock()
        mock_df = Mock(spec=DataFrame)

        spark_mock.read = mock_reader
        mock_reader.format.return_value = mock_format
        mock_format.option.return_value = mock_format
        mock_format.load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="test_user",
            password="test_password"
        )

        result = extract_db.execute()

        # Vérifier l'ordre des appels
        mock_reader.format.assert_called_once_with("jdbc")
        assert mock_format.option.call_count == 5  # 5 options
        mock_format.load.assert_called_once()
        assert result == mock_df


class TestExtractDBEdgeCases:

    def test_special_characters_in_credentials(self):
        """Test avec des caractères spéciaux dans les credentials"""
        spark_mock = Mock()
        mock_df = Mock(spec=DataFrame)
        spark_mock.read.format.return_value.option.return_value.load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="test_table",
            user="user@domain.com",
            password="p@ssw0rd!#$%"
        )

        result = extract_db.execute()

        spark_mock.read.format.return_value.option.assert_any_call(
            "user", "user@domain.com"
        )
        spark_mock.read.format.return_value.option.assert_any_call(
            "password", "p@ssw0rd!#$%"
        )
        assert result == mock_df

    def test_empty_table_name(self):
        """Test avec un nom de table vide"""
        spark_mock = Mock()
        mock_df = Mock(spec=DataFrame)
        spark_mock.read.format.return_value.option.return_value.load.return_value = mock_df

        extract_db = ExtractDB(
            spark=spark_mock,
            url="jdbc:mysql://localhost:3306/test_db",
            table="",  # Table vide
            user="test_user",
            password="test_password"
        )

        result = extract_db.execute()

        spark_mock.read.format.return_value.option.assert_any_call("dbtable", "")
        assert result == mock_df