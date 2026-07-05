# Projet Spark — Pipeline data MovieLens

> Projet Apache Spark réalisé en PySpark : ingestion, nettoyage, stockage Parquet, analyses métier, optimisation, lecture de la Spark UI et exploration complémentaire.

---

## Vue rapide

| Élément | Détail |
|---|---|
| Réalisation | Steve Landry KOUOKAM NONO & Sean |
| Coordination| Antoine LUCSKO |
| Jeu de données | MovieLens small |
| Technologie | PySpark |
| Mode d'exécution | Local |
| Système utilisé | Ubuntu / Linux |
| Architecture | Bronze → Silver → Gold |
| Rapport Markdown | [`docs/rapport.md`](docs/rapport.md) |
| Rapport PDF | [`docs/rapport.pdf`](docs/rapport.pdf) |
| Dépôt GitHub | `https://github.com/Steve-Landry-NONO/Spark-pipeline-project` |
| Point d'entrée du pipeline | [`projet/pipeline_movielens.py`](projet/pipeline_movielens.py) |

---

## Présentation du projet

Ce dépôt contient mon projet réalisé dans le cadre la formation Apache Spark.

L'objectif est de construire un pipeline data complet avec PySpark, depuis l'ingestion de fichiers CSV bruts jusqu'à la production de résultats analytiques exploitables. Le projet suit une logique en trois couches : **bronze**, **silver** et **gold**.

Le travail met l'accent sur plusieurs points importants :

- lecture de fichiers CSV avec schémas explicites ;
- nettoyage et typage des données ;
- écriture d'une couche silver au format Parquet ;
- analyses métier avec agrégations, jointures et window functions ;
- optimisation Spark mesurée ;
- observation de la Spark UI ;
- exploration complémentaire au-delà des TP ;
- rédaction d'un rapport clair et documenté.

---

## Organisation générale du dépôt

Le dépôt a été réorganisé afin de séparer clairement :

- le projet individuel ;
- les données et sorties générées ;
- la documentation du rendu ;
- les ressources fournies dans le cadre du cours.

```text
.
├── README.md
├── requirements.txt
├── projet/
│   ├── pipeline_movielens.py
│   └── src/
│       ├── session_spark.py
│       ├── schemas.py
│       ├── ingestion.py
│       ├── nettoyage.py
│       ├── ecriture.py
│       ├── analyses.py
│       ├── optimisation.py
│       └── exploration.py
├── docs/
│   ├── rapport.md
│   ├── rapport.pdf
│   └── captures/
├── data/
│   ├── download.sh
│   ├── sources-open-data.md
│   ├── datasets/
│   └── output/
└── ressources_cours/
    ├── demos/
    ├── tps/
    ├── slides/
    ├── exercises/
    ├── projects/
    ├── starter-code/
    └── student-facing/
```

Les ressources du cours sont conservées dans `ressources_cours/` afin de garder le contexte pédagogique sans mélanger les fichiers fournis avec les fichiers spécifiques au projet.

---

## Jeu de données utilisé

Le projet utilise le jeu de données **MovieLens small**, fourni par GroupLens.

Les fichiers exploités sont situés dans :

```text
data/datasets/ml-latest-small/
```

| Fichier | Utilisation |
|---|---|
| `ratings.csv` | Notes attribuées par les utilisateurs aux films |
| `movies.csv` | Informations sur les films : identifiant, titre, genres |
| `tags.csv` | Non utilisé dans cette version |
| `links.csv` | Non utilisé dans cette version |

Volumes observés pendant l'ingestion :

| Table | Nombre de lignes |
|---|---:|
| `ratings.csv` | 100 836 |
| `movies.csv` | 9 742 |

---

## Architecture du pipeline

Le pipeline suit une architecture **bronze → silver → gold**.

```text
Fichiers CSV bruts MovieLens
        ↓
Bronze : ingestion avec schémas explicites
        ↓
Silver : nettoyage, typage, enrichissement, écriture Parquet
        ↓
Gold : analyses métier et résultats de synthèse
        ↓
Rapport, mesures et captures Spark UI
```

### Couche bronze

La couche bronze correspond à la lecture des fichiers CSV bruts.

Les fichiers sont lus avec des schémas explicites définis dans :

```text
projet/src/schemas.py
```

L'objectif est d'éviter `inferSchema` et de garder un contrôle précis sur les types manipulés par Spark.

### Couche silver

La couche silver contient les données nettoyées, typées et enrichies.

Elle est écrite au format Parquet dans :

```text
data/output/silver/
```

La table des notes est partitionnée par `annee_note`.

### Couche gold

La couche gold contient les résultats analytiques produits à partir de la couche silver.

Les résultats sont écrits dans :

```text
data/output/gold/
```

Ces sorties sont générées localement par le pipeline et ne sont pas destinées à être versionnées dans Git.

---

## Installation sous Ubuntu

### 1. Cloner le dépôt

```bash
git clone git@github.com:Steve-Landry-NONO/Spark-pipeline-project.git
cd Spark-pipeline-project
```

### 2. Se placer sur la branche du projet

```bash
git checkout projet-spark-movielens
```

### 3. Créer un environnement virtuel Python

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 5. Télécharger les données

```bash
bash data/download.sh data/datasets
```

Le script télécharge notamment MovieLens small dans `data/datasets/`.

---

## Exécution du pipeline

Depuis la racine du dépôt :

```bash
python3 projet/pipeline_movielens.py
```

Pendant l'exécution, la Spark UI est accessible à l'adresse suivante :

```text
http://localhost:4040
```

Le script garde volontairement la session Spark ouverte à la fin de l'exécution afin de permettre l'observation des jobs, stages et requêtes SQL/DataFrame dans la Spark UI.

---

## Modules du projet

| Fichier | Rôle |
|---|---|
| `projet/pipeline_movielens.py` | Point d'entrée principal du pipeline |
| `projet/src/session_spark.py` | Création et configuration de la SparkSession |
| `projet/src/schemas.py` | Définition des schémas explicites |
| `projet/src/ingestion.py` | Lecture bronze des fichiers CSV |
| `projet/src/nettoyage.py` | Nettoyage, typage et enrichissement des données |
| `projet/src/ecriture.py` | Écriture des couches silver et gold |
| `projet/src/analyses.py` | Analyses métier de la couche gold |
| `projet/src/optimisation.py` | Mesure de l'optimisation par broadcast join |
| `projet/src/exploration.py` | Exploration complémentaire fonction native Spark vs UDF Python |

---

## Analyses réalisées

Trois analyses métier sont produites.

| Analyse | Notion Spark utilisée | Objectif |
|---|---|---|
| Films les mieux notés | `groupBy` + `agg` | Identifier les films les mieux notés avec un seuil minimal de votes |
| Popularité par genre | `join` + `explode` + `groupBy` | Mesurer les genres les plus représentés dans les notes |
| Top films par genre | `Window` + `row_number` | Classer les meilleurs films à l'intérieur de chaque genre |

---

## Résultats principaux

### Film le mieux noté avec un seuil de votes

```text
Shawshank Redemption, The (1994)
Nombre de votes : 317
Note moyenne    : 4.43
```

### Genres les plus populaires

```text
Drama      : 41 928 notes
Comedy     : 39 053 notes
Action     : 30 635 notes
Thriller   : 26 452 notes
Adventure  : 24 161 notes
```

### Exemple de classement pour le genre Action

```text
1. Logan (2017)
2. Fight Club (1999)
3. Dark Knight, The (2008)
4. Star Wars: Episode IV - A New Hope (1977)
5. Princess Bride, The (1987)
```

---

## Optimisation Spark

L'optimisation étudiée est le **broadcast join**.

La table `movies` contient 9 742 lignes, tandis que la table `ratings` contient 100 836 lignes. La table des films est donc un bon candidat pour être diffusée avec `broadcast()` pendant la jointure.

Mesure obtenue en local :

| Jointure | Temps |
|---|---:|
| Sans broadcast | 0.1806 s |
| Avec broadcast | 0.1476 s |
| Gain relatif | 18.24 % |

Le plan physique Spark confirme l'utilisation de :

```text
BroadcastExchange
BroadcastHashJoin
```

Cette optimisation est documentée dans :

```text
projet/src/optimisation.py
```

---

## Exploration complémentaire

L'exploration complémentaire compare deux méthodes d'extraction de l'année depuis le titre d'un film :

1. fonction native Spark avec `regexp_extract` ;
2. UDF Python.

Mesure obtenue en local :

| Méthode | Temps |
|---|---:|
| Fonction native Spark | 0.0898 s |
| UDF Python | 0.0570 s |

Sur ce petit volume, l'UDF Python a été mesurée plus rapide. Ce résultat est interprété avec prudence, car la table `movies` ne contient que 9 742 lignes et les temps sont très courts.

L'intérêt principal de l'exploration se situe dans la lecture du plan physique : la version UDF fait apparaître `BatchEvalPython`, ce qui montre le passage par une fonction Python utilisateur.

Cette exploration est documentée dans :

```text
projet/src/exploration.py
```

---

## Spark UI

Les captures Spark UI sont disponibles dans :

```text
docs/captures/
```

Elles illustrent différentes étapes du pipeline :

| Capture | Description |
|---|---|
| `spark-ui-jobs-ingestion-bronze.png` | Jobs déclenchés pendant l'ingestion bronze |
| `spark-ui-jobs-analyses-gold.png` | Jobs déclenchés pendant les analyses gold |
| `spark-ui-jobs-pipeline-complet.png` | Exécution complète du pipeline |
| `spark-ui-jobs-exploration-complete.png` | Exécution incluant l'exploration complémentaire |

Ces captures sont utilisées dans le rapport pour commenter l'exécution Spark.

---

## Rapport

Le rapport complet est disponible en deux formats :

- Markdown : [`docs/rapport.md`](docs/rapport.md)
- PDF : [`docs/rapport.pdf`](docs/rapport.pdf)

Le rapport détaille :

- le choix du jeu de données ;
- le schéma cible ;
- l'architecture bronze/silver/gold ;
- le nettoyage appliqué ;
- les analyses métier ;
- l'optimisation mesurée ;
- la lecture de la Spark UI ;
- l'exploration complémentaire ;
- les limites et pistes d'amélioration.

---

## Historique de travail

Le projet a été construit progressivement avec des commits en français.

Quelques commits représentatifs :

```text
Ajouter l'ingestion bronze des fichiers MovieLens
Nettoyer les données et écrire la couche silver en Parquet
Corriger l'extraction robuste de l'année des films
Ajouter les analyses métier gold avec agrégation jointure et window function
Mesurer l'optimisation par broadcast join
Comparer une fonction native Spark et une UDF Python
Ajouter le rapport final du projet Spark
Réorganiser le dépôt en séparant projet et ressources du cours
```

Cette progression permet de suivre les étapes du projet et les choix techniques réalisés au fur et à mesure.

---

## Limites

Le projet est exécuté en local avec MovieLens small. Les volumes restent donc modestes par rapport à un contexte Big Data réel.

Les mesures de performance doivent être interprétées avec prudence, car elles peuvent être influencées par :

- le cache disque ;
- le faible volume de données ;
- l'ordre des exécutions ;
- les conditions locales de la machine ;
- le coût fixe de démarrage des tâches Spark.

Les fichiers `tags.csv` et `links.csv` ne sont pas exploités dans cette version.

---

## Pistes d'amélioration

Avec plus de temps, plusieurs améliorations seraient possibles :

- intégrer `tags.csv` pour enrichir l'analyse des films ;
- exploiter `links.csv` pour relier MovieLens à d'autres référentiels ;
- ajouter une recommandation simple avec MLlib ALS ;
- comparer plusieurs formats de stockage ;
- mesurer l'impact du nombre de partitions de shuffle ;
- ajouter une exploration sur le predicate pushdown Parquet ;
- exporter certains résultats gold en CSV pour une consultation directe.

---

## Conclusion

Ce projet met en œuvre un pipeline Spark complet, structuré et reproductible.

Il démontre les principales notions attendues : ingestion typée, nettoyage, stockage Parquet, analyses métier, optimisation mesurée, lecture de plan physique, observation de la Spark UI et exploration complémentaire.

L'objectif principal n'était pas de produire un pipeline inutilement complexe, mais un pipeline clair, fiable, mesurable et correctement expliqué.
