"""Point d'entrée principal du pipeline Spark MovieLens.

Cette première version exécute l'étape d'ingestion bronze. Elle permet de
vérifier que Spark lit correctement les fichiers CSV avec des schémas
explicites et que la Spark UI est accessible pendant l'exécution.
"""

from src.ingestion import (
    afficher_controle_ingestion,
    lire_films_bruts,
    lire_notes_brutes,
)
from src.session_spark import creer_session_spark


CHEMIN_NOTES = "data/datasets/ml-latest-small/ratings.csv"
CHEMIN_FILMS = "data/datasets/ml-latest-small/movies.csv"


def main() -> None:
    """Orchestre la première étape du pipeline : l'ingestion bronze."""
    spark = creer_session_spark()

    print("\n==============================================")
    print(" Projet individuel Spark - MovieLens")
    print(" Étape actuelle : ingestion bronze")
    print("==============================================")
    print("Spark UI disponible pendant l'exécution : http://localhost:4040")

    notes_brutes = lire_notes_brutes(spark, CHEMIN_NOTES)
    films_bruts = lire_films_bruts(spark, CHEMIN_FILMS)

    afficher_controle_ingestion(notes_brutes, films_bruts)

    input("\nSpark UI ouverte sur http://localhost:4040 - Appuyer sur Entrée pour terminer...")

    spark.stop()


if __name__ == "__main__":
    main()
