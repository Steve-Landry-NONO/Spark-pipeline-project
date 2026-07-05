# Projet Spark — Pipeline data MovieLens

## Présentation du projet

Ce dépôt contient mon projet individuel réalisé dans le cadre du jour 4 de la formation Apache Spark.

L'objectif du projet est de construire un pipeline data complet avec PySpark, depuis l'ingestion de fichiers CSV bruts jusqu'à la production de résultats analytiques exploitables. Le travail suit une logique en trois couches : bronze, silver et gold.

Le projet met l'accent sur la qualité de l'ingestion, le typage explicite des données, le nettoyage, les agrégations, les jointures, les window functions, l'optimisation Spark, l'observation de la Spark UI et une exploration complémentaire au-delà du cours.

---

## Réalisation

Projet réalisé individuellement.

J'ai donc pris en charge l'ensemble de la chaîne de traitement :

- choix du jeu de données ;
- préparation de l'environnement sous Ubuntu ;
- structuration du dépôt ;
- ingestion des fichiers bruts ;
- nettoyage et écriture de la couche silver ;
- analyses métier ;
- optimisation mesurée ;
- exploration complémentaire ;
- captures Spark UI ;
- rédaction du rapport.

---

## Jeu de données utilisé

Le projet utilise le jeu de données **MovieLens small**, fourni par GroupLens.

Les principaux fichiers exploités sont :

| Fichier | Rôle |
|---|---|
| `ratings.csv` | Notes attribuées par les utilisateurs aux films |
| `movies.csv` | Informations sur les films : identifiant, titre, genres |
| `tags.csv` | Tags ajoutés par les utilisateurs, non utilisé dans cette version |
| `links.csv` | Identifiants externes, non utilisé dans cette version |

Volumes utilisés :

| Table | Nombre de lignes |
|---|---:|
| `ratings.csv` | 100 836 |
| `movies.csv` | 9 742 |

---

## Architecture du pipeline

Le pipeline suit une architecture bronze, silver et gold.

```text
Fichiers CSV bruts
        ↓
Bronze : ingestion avec schémas explicites
        ↓
Silver : nettoyage, typage, enrichissement, écriture Parquet
        ↓
Gold : analyses métier et résultats de synthèse
        ↓
Rapport écrit, mesures et captures Spark UI
