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
    # 1️⃣ Filtrer les personnes âgées de plus de 30 ans
    dataf_transformed = dataf.filter(col("age") > 30)

    # 2️⃣ Ajouter une colonne "age_in_5_years"
    dataf_transformed = dataf_transformed.withColumn("age_in_5_years", col("age") + 5)

    # 3️⃣ Transformer les noms en majuscules
    dataf_transformed = dataf_transformed.withColumn("name", upper(col("name")))

    # 4️⃣ Trier par âge décroissant
    dataf_transformed = dataf_transformed.orderBy(col("age").desc())

    return dataf_transformed
