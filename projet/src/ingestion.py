"""Fonctions d'ingestion de la couche bronze.

La couche bronze correspond ici aux fichiers CSV MovieLens lus tels qu'ils
sont fournis. Les données ne sont pas encore nettoyées à cette étape.
"""

from pyspark.sql import DataFrame, SparkSession

from src.schemas import schema_films, schema_notes


def lire_notes_brutes(spark: SparkSession, chemin_notes: str) -> DataFrame:
    """Lit le fichier ratings.csv avec un schéma explicite.

    Ce fichier contient les notes attribuées par les utilisateurs aux films.
    Le timestamp est conservé sous forme numérique à l'ingestion ; il sera
    transformé en date lisible pendant l'étape de nettoyage.
    """
    return (
        spark.read
        .option("header", "true")
        .schema(schema_notes)
        .csv(chemin_notes)
    )


def lire_films_bruts(spark: SparkSession, chemin_films: str) -> DataFrame:
    """Lit le fichier movies.csv avec un schéma explicite.

    Ce fichier contient les informations descriptives des films. La colonne
    `genres` contient plusieurs genres séparés par le caractère `|`, ce qui
    sera exploité plus tard dans les analyses métier.
    """
    return (
        spark.read
        .option("header", "true")
        .schema(schema_films)
        .csv(chemin_films)
    )


def afficher_controle_ingestion(notes: DataFrame, films: DataFrame) -> None:
    """Affiche les premiers contrôles de qualité après ingestion.

    Ces affichages servent à documenter le travail dans le rapport :
    schéma lu par Spark, exemples de lignes et volumes bruts.
    """
    print("\n=== Contrôle d'ingestion : notes brutes ===")
    notes.printSchema()
    notes.show(5, truncate=False)
    print(f"Nombre de lignes dans ratings.csv : {notes.count()}")

    print("\n=== Contrôle d'ingestion : films bruts ===")
    films.printSchema()
    films.show(5, truncate=False)
    print(f"Nombre de lignes dans movies.csv : {films.count()}")
