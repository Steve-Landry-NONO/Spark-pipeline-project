"""Analyses métier de la couche gold.

Les analyses sont réalisées à partir de la couche silver, c'est-à-dire à partir
des données nettoyées et stockées en Parquet. Cette séparation permet de ne pas
relire directement les fichiers CSV bruts pour les traitements analytiques.

Les trois analyses couvrent les attendus du projet :
- une agrégation ;
- une jointure ;
- une window function.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SEUIL_MINIMUM_VOTES_FILM = 50
SEUIL_MINIMUM_VOTES_GENRE = 20


def analyser_films_les_mieux_notes(notes: DataFrame, films: DataFrame) -> DataFrame:
    """Analyse 1 : identifier les films les mieux notés.

    Cette analyse utilise une agrégation par film pour calculer :
    - le nombre de notes reçues ;
    - la note moyenne.

    Un seuil minimal de votes est appliqué afin d'éviter de mettre en avant
    des films ayant une excellente moyenne mais très peu d'avis.
    """
    notes_par_film = (
        notes
        .groupBy("movieId")
        .agg(
            F.count("*").alias("nombre_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
        )
        .filter(F.col("nombre_votes") >= SEUIL_MINIMUM_VOTES_FILM)
    )

    resultat = (
        notes_par_film
        .join(films, on="movieId", how="inner")
        .select(
            "movieId",
            "title",
            "genres",
            "annee_sortie",
            "nombre_votes",
            "note_moyenne",
        )
        .orderBy(F.desc("note_moyenne"), F.desc("nombre_votes"))
    )

    return resultat


def analyser_popularite_par_genre(notes: DataFrame, films: DataFrame) -> DataFrame:
    """Analyse 2 : mesurer la popularité et la note moyenne par genre.

    Cette analyse repose sur une jointure entre les notes et les films.
    Comme un film peut appartenir à plusieurs genres, la colonne genres_tableau
    est explosée afin d'obtenir une ligne par couple film-genre.
    """
    notes_films = notes.join(films, on="movieId", how="inner")

    resultat = (
        notes_films
        .withColumn("genre", F.explode(F.col("genres_tableau")))
        .filter(F.col("genre") != "(no genres listed)")
        .groupBy("genre")
        .agg(
            F.count("*").alias("nombre_notes"),
            F.countDistinct("movieId").alias("nombre_films"),
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
        )
        .orderBy(F.desc("nombre_notes"))
    )

    return resultat


def analyser_top_films_par_genre(notes: DataFrame, films: DataFrame) -> DataFrame:
    """Analyse 3 : produire un classement des meilleurs films par genre.

    Cette analyse utilise une window function. Après avoir calculé la note
    moyenne par film et par genre, on classe les films dans chaque genre avec
    row_number(). Le résultat conserve les cinq premiers films de chaque genre.
    """
    notes_films = notes.join(films, on="movieId", how="inner")

    films_par_genre = (
        notes_films
        .withColumn("genre", F.explode(F.col("genres_tableau")))
        .filter(F.col("genre") != "(no genres listed)")
        .groupBy("genre", "movieId", "title", "annee_sortie")
        .agg(
            F.count("*").alias("nombre_votes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne"),
        )
        .filter(F.col("nombre_votes") >= SEUIL_MINIMUM_VOTES_GENRE)
    )

    fenetre_genre = Window.partitionBy("genre").orderBy(
        F.desc("note_moyenne"),
        F.desc("nombre_votes"),
        F.asc("title"),
    )

    resultat = (
        films_par_genre
        .withColumn("rang", F.row_number().over(fenetre_genre))
        .filter(F.col("rang") <= 5)
        .select(
            "genre",
            "rang",
            "movieId",
            "title",
            "annee_sortie",
            "nombre_votes",
            "note_moyenne",
        )
        .orderBy("genre", "rang")
    )

    return resultat


def afficher_resultats_analyses(
    films_mieux_notes: DataFrame,
    popularite_genres: DataFrame,
    top_films_par_genre: DataFrame,
) -> None:
    """Affiche des extraits des analyses pour alimenter le rapport."""
    print("\n=== Analyse 1 : films les mieux notés ===")
    films_mieux_notes.show(10, truncate=False)

    print("\n=== Analyse 2 : popularité par genre ===")
    popularite_genres.show(20, truncate=False)

    print("\n=== Analyse 3 : top 5 films par genre ===")
    top_films_par_genre.show(50, truncate=False)
