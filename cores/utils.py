from enum import Enum

# Type de source de données : base de données ou fichier
class DataSourceType(Enum):
    DB = "db"
    FILE = "file"

# Type des fichier pour les source FILE
class FileType(Enum):
    CSV = "csv"
    AVRO = "avro"
    PARQUET = "parquet"
    ORC = "orc"
    JSON = "json"
    XML = "xml"
