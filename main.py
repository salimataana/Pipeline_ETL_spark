from pyspark.sql import SparkSession
from cores.extract import Extract
from cores.transform import Transform
from cores.load import Load
from cores.utils import DataSourceType
from pipeline import Pipeline

# 1 créer la SparkSession
spark = SparkSession.builder \
    .appName("ETL Crocodiles") \
    .getOrCreate()

# Étapes du pipeline pour un seul fichier
extract = Extract(
    source_type=DataSourceType.FILE,
    path=r"/home/salimata/Bureau/crocodile_dataset.csv",
    spark=spark
)
transform = Transform(source_type=DataSourceType.FILE)
load = Load(
    source_type=DataSourceType.FILE,
    output_path=r"/home/salimata/Bureau/Result_Crocodile"
)

# Création du pipeline
pipeline = Pipeline(steps=[extract, transform, load])

# Exécution du pipeline
pipeline.run()
