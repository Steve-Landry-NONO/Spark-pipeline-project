"""Exploration complémentaire : fonction native Spark contre UDF Python.

Cette exploration répond à l'exigence du projet qui demande d'aller au-delà
des TP. L'objectif est de comparer deux façons de réaliser la même
transformation :
- une fonction native Spark avec regexp_extract ;
- une UDF Python.

La transformation choisie consiste à extraire l'année de sortie depuis le titre
du film, par exemple "Toy Story (1995)" -> 1995.
"""

import re
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def extraire_annee_python(titre: str) -> int | None:
    """Extrait l'année d'un titre de film avec du Python classique.

    Cette fonction sera utilisée dans une UDF. Elle est volontairement simple
    afin de comparer le coût d'exécution avec une fonction native Spark.
    """
    if titre is None:
        return None

    resultat = re.search(r"\((\d{4})\)", titre)

    if resultat is None:
        return None

    return int(resultat.group(1))


def mesurer_temps(description: str, fonction_action) -> float:
    """Mesure le temps d'exécution d'une action Spark.

    Une action est nécessaire car Spark évalue les transformations de manière
    paresseuse. Ici, count() permet de forcer l'exécution réelle du calcul.
    """
    debut = time.perf_counter()
    fonction_action()
    fin = time.perf_counter()

    duree = fin - debut
    print(f"{description} : {duree:.4f} secondes")

    return duree


def comparer_fonction_native_et_udf(films: DataFrame) -> dict:
    """Compare l'extraction d'année avec une fonction native et une UDF Python.

    Le même résultat fonctionnel est produit de deux manières différentes.
    La comparaison permet d'observer le coût potentiel d'une UDF Python dans
    un pipeline Spark.
    """
    print("\n=== Exploration : fonction native Spark vs UDF Python ===")

    films_sans_annee = films.drop("annee_sortie")

    films_avec_fonction_native = (
        films_sans_annee
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

    udf_extraire_annee = F.udf(extraire_annee_python, IntegerType())

    films_avec_udf = (
        films_sans_annee
        .withColumn("annee_sortie", udf_extraire_annee(F.col("title")))
    )

    duree_fonction_native = mesurer_temps(
        "Extraction avec fonction native Spark",
        lambda: films_avec_fonction_native.count(),
    )

    duree_udf = mesurer_temps(
        "Extraction avec UDF Python",
        lambda: films_avec_udf.count(),
    )

    ecart = duree_udf - duree_fonction_native

    if duree_fonction_native > 0:
        ecart_pourcentage = (ecart / duree_fonction_native) * 100
    else:
        ecart_pourcentage = 0.0

    print("\n=== Résumé de l'exploration ===")
    print(f"Temps fonction native Spark : {duree_fonction_native:.4f} secondes")
    print(f"Temps UDF Python            : {duree_udf:.4f} secondes")
    print(f"Écart observé               : {ecart:.4f} secondes")
    print(f"Écart relatif               : {ecart_pourcentage:.2f} %")

    print("\n=== Plan physique avec fonction native Spark ===")
    films_avec_fonction_native.explain(mode="formatted")

    print("\n=== Plan physique avec UDF Python ===")
    films_avec_udf.explain(mode="formatted")

    return {
        "temps_fonction_native": duree_fonction_native,
        "temps_udf_python": duree_udf,
        "ecart_secondes": ecart,
        "ecart_pourcentage": ecart_pourcentage,
    }
