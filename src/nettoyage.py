"""Nettoyage des données MovieLens pour produire la couche silver.

La couche silver contient des données plus fiables que les fichiers bruts :
- doublons supprimés ;
- valeurs critiques manquantes retirées ;
- notes filtrées selon l'échelle attendue ;
- colonnes dérivées ajoutées pour faciliter les analyses.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def nettoyer_notes(notes_brutes: DataFrame) -> DataFrame:
    """Nettoie le fichier ratings.csv.

    Les notes MovieLens sont normalement comprises entre 0.5 et 5.0.
    Le nettoyage vérifie donc cette contrainte afin d'éviter que des valeurs
    incohérentes ne perturbent les moyennes calculées ensuite.

    Le timestamp Unix est aussi converti en timestamp lisible, puis en année.
    Cette colonne d'année servira au partitionnement de la couche silver.
    """
    notes_propres = (
        notes_brutes
        .dropDuplicates(["userId", "movieId", "timestamp"])
        .na.drop(subset=["userId", "movieId", "rating", "timestamp"])
        .filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))
        .withColumn(
            "date_note",
            F.from_unixtime(F.col("timestamp")).cast("timestamp")
        )
        .withColumn("annee_note", F.year(F.col("date_note")))
    )

    return notes_propres


def nettoyer_films(films_bruts: DataFrame) -> DataFrame:
    """Nettoie le fichier movies.csv.

    La colonne genres contient plusieurs valeurs séparées par le caractère `|`.
    Elle est transformée en tableau afin de pouvoir utiliser explode() dans les
    analyses par genre.

    L'année de sortie est extraite depuis le titre lorsque l'information est
    présente. Certains titres ne contiennent pas d'année exploitable ; dans ce
    cas, la valeur est conservée à NULL plutôt que de provoquer une erreur de
    cast.
    """
    films_propres = (
        films_bruts
        .dropDuplicates(["movieId"])
        .na.drop(subset=["movieId", "title"])
        .withColumn(
            "genres_tableau",
            F.split(F.col("genres"), "\\|")
        )
        .withColumn(
            "annee_sortie_texte",
            F.regexp_extract(F.col("title"), r"\((\d{4})\)", 1)
        )
        .withColumn(
            "annee_sortie",
            F.when(
                F.col("annee_sortie_texte") != "",
                F.col("annee_sortie_texte").cast("int")
            ).otherwise(None)
        )
        .drop("annee_sortie_texte")
    )

    return films_propres


def afficher_controle_nettoyage(
    notes_brutes: DataFrame,
    notes_propres: DataFrame,
    films_bruts: DataFrame,
    films_propres: DataFrame,
) -> None:
    """Affiche un bilan simple du nettoyage.

    Ces chiffres seront repris dans le rapport afin de documenter précisément
    le passage de la couche bronze à la couche silver.
    """
    nb_notes_brutes = notes_brutes.count()
    nb_notes_propres = notes_propres.count()
    nb_films_bruts = films_bruts.count()
    nb_films_propres = films_propres.count()

    print("\n=== Bilan du nettoyage : notes ===")
    print(f"Lignes brutes ratings.csv      : {nb_notes_brutes}")
    print(f"Lignes après nettoyage         : {nb_notes_propres}")
    print(f"Lignes écartées                : {nb_notes_brutes - nb_notes_propres}")

    print("\n=== Bilan du nettoyage : films ===")
    print(f"Lignes brutes movies.csv       : {nb_films_bruts}")
    print(f"Lignes après nettoyage         : {nb_films_propres}")
    print(f"Lignes écartées                : {nb_films_bruts - nb_films_propres}")

    print("\n=== Aperçu des notes nettoyées ===")
    notes_propres.show(5, truncate=False)

    print("\n=== Aperçu des films nettoyés ===")
    films_propres.show(5, truncate=False)
