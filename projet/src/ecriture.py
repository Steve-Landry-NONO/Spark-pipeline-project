"""Fonctions d'écriture des couches silver et gold.

Les écritures sont centralisées dans ce module afin de garder le pipeline
principal lisible et facile à suivre.
"""

from pyspark.sql import DataFrame


def ecrire_silver_notes(notes_propres: DataFrame, chemin_sortie: str) -> None:
    """Écrit les notes nettoyées en Parquet partitionné par année.

    Le partitionnement par année est pertinent car la colonne possède une faible
    cardinalité et peut faciliter certaines lectures filtrées dans Spark.
    """
    (
        notes_propres
        .write
        .mode("overwrite")
        .partitionBy("annee_note")
        .parquet(chemin_sortie)
    )

    print(f"Couche silver des notes écrite dans : {chemin_sortie}")


def ecrire_silver_films(films_propres: DataFrame, chemin_sortie: str) -> None:
    """Écrit les films nettoyés en Parquet.

    La table des films est petite et ne nécessite pas de partitionnement
    particulier dans le cadre de ce projet.
    """
    (
        films_propres
        .write
        .mode("overwrite")
        .parquet(chemin_sortie)
    )

    print(f"Couche silver des films écrite dans : {chemin_sortie}")
    
def ecrire_resultat_gold(resultat: DataFrame, chemin_sortie: str) -> None:
    """Écrit un résultat analytique gold au format Parquet.

    Les résultats gold sont de petite taille. L'utilisation de coalesce(1) est
    donc acceptable ici pour faciliter la consultation des fichiers produits.
    Cette pratique ne serait pas adaptée sur un gros DataFrame.
    """
    (
        resultat
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(chemin_sortie)
    )

    print(f"Résultat gold écrit dans : {chemin_sortie}")

