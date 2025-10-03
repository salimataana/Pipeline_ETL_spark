from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, to_date, when

def transform_dataframe(df: DataFrame) -> DataFrame:
    """
    Transformations sur le DataFrame Spark :
    - Supprimer les valeurs manquantes
    - Mettre les noms en majuscules
    - Convertir la date en type date
    - Créer une colonne 'Weight_Category'
    - Filtrer Adultes et certains habitats
    """
    # Supprimer les lignes avec des valeurs nulles
    df = df.dropna()

    # Mettre certaines colonnes en majuscules
    df = df.withColumn("Common Name", upper(col("Common Name")))
    df = df.withColumn("Scientific Name", upper(col("Scientific Name")))

    # Convertir la colonne Date en type date
    df = df.withColumn("Date of Observation", to_date(col("Date of Observation"), "dd-MM-yyyy"))

    # Créer une colonne Weight_Category
    df = df.withColumn(
        "Weight_Category",
        when(col("Observed Weight (kg)") < 100, "Light")
        .when((col("Observed Weight (kg)") >= 100) & (col("Observed Weight (kg)") < 300), "Medium")
        .otherwise("Heavy")
    )

    # Filtrer uniquement les Adultes
    df = df.filter(col("Age Class") == "Adult")

    # Filtrer certains habitats
    df = df.filter(col("Habitat Type").isin("Swamps", "Mangroves"))

    return df
