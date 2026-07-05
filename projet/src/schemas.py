"""Schémas explicites des fichiers CSV MovieLens.

L'énoncé du projet demande de ne pas utiliser `inferSchema`.
Les schémas sont donc déclarés manuellement avec StructType afin de garder
un contrôle clair sur les types utilisés par Spark.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


schema_notes = StructType(
    [
        StructField("userId", IntegerType(), nullable=False),
        StructField("movieId", IntegerType(), nullable=False),
        StructField("rating", DoubleType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),
    ]
)


schema_films = StructType(
    [
        StructField("movieId", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("genres", StringType(), nullable=True),
    ]
)
