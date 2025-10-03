from pyspark.sql import SparkSession
from cores.step import Step

class ExtractDB(Step):
    def __init__(self, spark: SparkSession, url: str, table: str, user: str, password: str):
        self.kind = "extract"
        self.spark = spark
        self.url = url        # URL JDBC de la base pour la connexion
        self.table = table    # Table à lire
        self.user = user        # utilisateur de la base de données qui a les droits sur les tables
        self.password = password

    def execute(self, data=None):
        """
        Lit une table depuis une base de données via JDBC et retourne un DataFrame Spark
        """
        dataf = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", self.table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        return dataf
