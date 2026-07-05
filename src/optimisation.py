"""Mesure d'une optimisation Spark : broadcast join.

Cette exploration technique compare une jointure classique avec une jointure
utilisant explicitement broadcast(). La table des films étant beaucoup plus
petite que la table des notes, elle constitue un bon candidat au broadcast.
"""

import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def mesurer_temps_execution(description: str, fonction_action) -> float:
    """Mesure le temps d'exécution d'une action Spark.

    Spark étant paresseux, les transformations seules ne déclenchent pas de
    calcul. La fonction reçue en paramètre doit donc contenir une action
    comme count(), write(), show() ou collect() sur un petit résultat.
    """
    debut = time.perf_counter()
    fonction_action()
    fin = time.perf_counter()

    duree = fin - debut
    print(f"{description} : {duree:.4f} secondes")

    return duree


def mesurer_broadcast_join(notes: DataFrame, films: DataFrame) -> dict:
    """Compare une jointure classique et une jointure broadcast.

    La jointure est volontairement simple : on relie les notes aux films par
    la clé movieId. Le count() sert uniquement à déclencher réellement le job
    Spark et donc à obtenir un temps mesurable.
    """
    print("\n=== Optimisation : comparaison de jointure ===")

    duree_sans_broadcast = mesurer_temps_execution(
        "Jointure classique sans broadcast",
        lambda: notes.join(films, on="movieId", how="inner").count(),
    )

    duree_avec_broadcast = mesurer_temps_execution(
        "Jointure avec broadcast explicite",
        lambda: notes.join(F.broadcast(films), on="movieId", how="inner").count(),
    )

    gain = duree_sans_broadcast - duree_avec_broadcast

    if duree_sans_broadcast > 0:
        gain_pourcentage = (gain / duree_sans_broadcast) * 100
    else:
        gain_pourcentage = 0.0

    print("\n=== Résumé de l'optimisation ===")
    print(f"Temps sans broadcast : {duree_sans_broadcast:.4f} secondes")
    print(f"Temps avec broadcast : {duree_avec_broadcast:.4f} secondes")
    print(f"Écart observé        : {gain:.4f} secondes")
    print(f"Écart relatif        : {gain_pourcentage:.2f} %")

    print("\n=== Plan physique avec broadcast explicite ===")
    (
        notes
        .join(F.broadcast(films), on="movieId", how="inner")
        .explain(mode="formatted")
    )

    return {
        "temps_sans_broadcast": duree_sans_broadcast,
        "temps_avec_broadcast": duree_avec_broadcast,
        "gain_secondes": gain,
        "gain_pourcentage": gain_pourcentage,
    }
