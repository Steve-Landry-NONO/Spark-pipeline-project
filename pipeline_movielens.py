"""Point d'entrée principal du pipeline Spark MovieLens.

Le pipeline suit une logique bronze, silver et gold. Cette version exécute :
1. l'ingestion bronze des fichiers CSV ;
2. le nettoyage des données ;
3. l'écriture de la couche silver au format Parquet.

La pause finale permet de consulter la Spark UI avant l'arrêt de la session.
"""

from src.ecriture import (
    ecrire_resultat_gold,
    ecrire_silver_films,
    ecrire_silver_notes,
)
from src.ingestion import (
    afficher_controle_ingestion,
    lire_films_bruts,
    lire_notes_brutes,
)
from src.nettoyage import (
    afficher_controle_nettoyage,
    nettoyer_films,
    nettoyer_notes,
)
from src.session_spark import creer_session_spark
from src.analyses import (
    afficher_resultats_analyses,
    analyser_films_les_mieux_notes,
    analyser_popularite_par_genre,
    analyser_top_films_par_genre,
)


CHEMIN_NOTES_BRUTES = "data/datasets/ml-latest-small/ratings.csv"
CHEMIN_FILMS_BRUTS = "data/datasets/ml-latest-small/movies.csv"

CHEMIN_SILVER_NOTES = "data/output/silver/notes"
CHEMIN_SILVER_FILMS = "data/output/silver/films"

CHEMIN_GOLD_FILMS_MIEUX_NOTES = "data/output/gold/films_mieux_notes"
CHEMIN_GOLD_POPULARITE_GENRES = "data/output/gold/popularite_genres"
CHEMIN_GOLD_TOP_FILMS_PAR_GENRE = "data/output/gold/top_films_par_genre"


def main() -> None:
    """Orchestre les premières étapes du pipeline MovieLens."""
    spark = creer_session_spark()

    print("\n==============================================")
    print(" Projet individuel Spark - MovieLens")
    print(" Étapes actuelles : bronze -> silver")
    print("==============================================")
    print("Spark UI disponible pendant l'exécution : http://localhost:4040")

    print("\n--- Étape 1 : ingestion bronze ---")
    notes_brutes = lire_notes_brutes(spark, CHEMIN_NOTES_BRUTES)
    films_bruts = lire_films_bruts(spark, CHEMIN_FILMS_BRUTS)
    afficher_controle_ingestion(notes_brutes, films_bruts)

    print("\n--- Étape 2 : nettoyage silver ---")
    notes_propres = nettoyer_notes(notes_brutes)
    films_propres = nettoyer_films(films_bruts)
    afficher_controle_nettoyage(
        notes_brutes,
        notes_propres,
        films_bruts,
        films_propres,
    )

    print("\n--- Étape 3 : écriture silver en Parquet ---")
    ecrire_silver_notes(notes_propres, CHEMIN_SILVER_NOTES)
    ecrire_silver_films(films_propres, CHEMIN_SILVER_FILMS)
    
    print("\n--- Étape 4 : analyses métier gold ---")
    notes_silver = spark.read.parquet(CHEMIN_SILVER_NOTES)
    films_silver = spark.read.parquet(CHEMIN_SILVER_FILMS)

    films_mieux_notes = analyser_films_les_mieux_notes(notes_silver, films_silver)
    popularite_genres = analyser_popularite_par_genre(notes_silver, films_silver)
    top_films_par_genre = analyser_top_films_par_genre(notes_silver, films_silver)

    afficher_resultats_analyses(
        films_mieux_notes,
        popularite_genres,
        top_films_par_genre,
    )

    print("\n--- Étape 5 : écriture gold en Parquet ---")
    ecrire_resultat_gold(films_mieux_notes, CHEMIN_GOLD_FILMS_MIEUX_NOTES)
    ecrire_resultat_gold(popularite_genres, CHEMIN_GOLD_POPULARITE_GENRES)
    ecrire_resultat_gold(top_films_par_genre, CHEMIN_GOLD_TOP_FILMS_PAR_GENRE)
    
    input("\nSpark UI ouverte sur http://localhost:4040 - Appuyer sur Entrée pour terminer...")
    
    

    spark.stop()


if __name__ == "__main__":
    main()
