# Projet Spark — Pipeline data MovieLens

Ce dépôt contient mon projet individuel réalisé dans le cadre du jour 4 de la formation Apache Spark.

L’objectif est de construire un pipeline data complet avec PySpark, depuis l’ingestion de fichiers CSV bruts jusqu’à la production de résultats analytiques exploitables. Le projet suit une logique bronze, silver et gold, avec une attention particulière portée au typage des données, au nettoyage, aux jointures, aux agrégations, aux window functions, à l’optimisation Spark et à la lecture de la Spark UI.

## Réalisation

Projet réalisé individuellement.

## Jeu de données

Le projet utilise le jeu de données MovieLens small, composé notamment des fichiers :

- `ratings.csv` : notes attribuées par les utilisateurs ;
- `movies.csv` : informations sur les films ;
- `tags.csv` : tags utilisateurs ;
- `links.csv` : identifiants externes.

## Architecture cible

```text
bronze CSV brut
        ↓
silver Parquet nettoyé
        ↓
gold résultats analytiques
        ↓
rapport écrit et observations Spark UI
