from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, col

def transform_base_de_donnes(dataf: DataFrame) -> DataFrame:
    """
    Applique une série de transformations sur le DataFrame :
    - Filtrer les personnes âgées de plus de 30 ans
    - Ajouter une colonne "age_in_5_years"
    - Mettre les noms en majuscules
    - Trier par âge décroissant
    """
    # Filtrer les personnes âgées de plus de 30 ans
    dataf = dataf.filter(col("age") > 30)

    # Ajouter une colonne "age_in_5_years"
    dataf = dataf.withColumn("age_in_5_years", col("age") + 5)

    #  Transformer les noms en majuscules
    dataf = dataf.withColumn("name", upper(col("name")))

    #  Trier par âge décroissant
    dataf = dataf.orderBy(col("age").desc())

    return dataf
