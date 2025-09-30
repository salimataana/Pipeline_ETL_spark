from pyspark.sql import SparkSession
from cores.extract import Extract
from cores.transform import Transform
from cores.load import Load
from cores.utils import FileType
from pipeline import Pipeline

# 1 créer la SparkSession
spark = SparkSession.builder \
    .appName("ETL Crocodiles") \
    .getOrCreate()

# Étapes du pipeline pour un seul fichier
extract = Extract(
    file_type=FileType.CSV,
    path=r"/home/salimata/Bureau/crocodile_dataset.csv",
    spark=spark
)
transform = Transform(file_type=FileType.CSV)
load = Load(
    file_type=FileType.CSV,
    output_path=r"/home/salimata/Bureau/Result_Crocodile"
)

# Création du pipeline
pipeline = Pipeline(steps=[extract, transform, load])

# Exécution du pipeline
pipeline.run()
