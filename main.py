from pyspark.sql import SparkSession
from cores.extract import Extract
from cores.load_bd import LoadBD
from cores.transform import Transform
from cores.load import Load
from cores.utils import FileType
from pipeline import Pipeline
from cores.extract_bd import ExtractDB

# 1 créer la SparkSession
spark = SparkSession.builder \
    .appName("ETL Crocodiles") \
    .config("spark.jars", "/home/salimata/PycharmProjects/Pipeline_ETL_spark/jars/mysql-connector-j-8.3.0.jar") \
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
extract_db = ExtractDB(
    spark=spark,
    url="jdbc:mysql://localhost:3306/test_db",
    table="people",
    user= "spark_user",
    password="SANOUsalimata1998!"
)

transform_bd = Transform()
load_bd = LoadBD(file_type=FileType.JSON,
    output_path=r"/home/salimata/Bureau/Result_BD")

# Test extraction seule (avant le pipeline)
df_db = extract_db.execute()
df_db.show()

# Création du pipeline
pipeline = Pipeline(steps=[extract, transform, load])

# Exécution du pipeline
pipeline.run()
