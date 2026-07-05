"""Création de la session Spark utilisée par le projet MovieLens.

Ce module centralise la configuration de Spark afin d'éviter de recréer
une SparkSession différente dans chaque fichier. Le projet est exécuté en
mode local, ce qui correspond au cadre demandé pour le rendu.
"""

from pyspark.sql import SparkSession


def creer_session_spark(
    nom_application: str = "Projet individuel Spark - MovieLens",
    partitions_shuffle: int = 64,
) -> SparkSession:
    """Crée et retourne une SparkSession configurée pour une exécution locale.

    Le paramètre `spark.sql.shuffle.partitions` est abaissé à 64 afin d'éviter
    de créer trop de petites tâches sur un ordinateur portable. Cette valeur
    reste suffisante pour observer les mécanismes de shuffle dans la Spark UI.
    """
    spark = (
        SparkSession.builder
        .appName(nom_application)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(partitions_shuffle))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
