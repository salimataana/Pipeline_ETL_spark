from enum import Enum

class DataSourceType(Enum):
    DB = "db"
    FILE = "file"

#je dois specifier chaque type de fichier : (csv avro parquet orc json xml)