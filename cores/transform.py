from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, to_date, when
from cores.step import Step
from cores.utils import DataSourceType

class Transform(Step):

    def __init__(self, source_type: DataSourceType):
        self.kind = "transform"
        self.data_source_type = source_type

    def execute(self, df: DataFrame):
        """
        Transformations sur le DataFrame Spark :
        - Supprimer les valeurs manquantes
        - Mettre les noms en majuscules
        - Convertir la date en type date
        - Créer une colonne 'Weight_Category'
        - Filtrer Adultes et certains habitats
        """

        # 1. Supprimer les lignes avec des valeurs nulles
        df = df.dropna()

        # 2. Mettre les colonnes de texte en majuscule
        df = df.withColumn("Common Name", upper(col("Common Name")))
        df = df.withColumn("Scientific Name", upper(col("Scientific Name")))

        # 3. Convertir 'Date of Observation' en type date
        df = df.withColumn("Date of Observation", to_date(col("Date of Observation"), "dd-MM-yyyy"))

        # 4. Créer une nouvelle colonne 'Weight_Category'
        df = df.withColumn(
            "Weight_Category",
            when(col("Observed Weight (kg)") < 100, "Light")
            .when((col("Observed Weight (kg)") >= 100) & (col("Observed Weight (kg)") < 300), "Medium")
            .otherwise("Heavy")
        )

        # 5. Filtrer uniquement les adultes
        df = df.filter(col("Age Class") == "Adult")

        # 6. Filtrer les observations dans certains habitats
        df = df.filter(col("Habitat Type").isin("Swamps", "Mangroves"))

        return df
