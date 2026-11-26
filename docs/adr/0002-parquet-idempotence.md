# ADR-0002 : Parquet partitionné et réécriture dynamique pour l'idempotence

**Statut** : acceptée

## Contexte

Le rejeu d'un même jour ne doit pas créer de doublons. Delta Lake et Iceberg ne font pas partie
du programme du module ; le stockage reste HDFS + Parquet.

## Décision

Toute écriture batch cible des partitions dérivées de l'entrée (`ingestion_date` en bronze et
silver) avec `spark.sql.sources.partitionOverwriteMode=dynamic` et `mode("overwrite")` : une
exécution remplace exactement les partitions qu'elle produit et ne touche pas aux autres. Les
tables non partitionnées et de petite taille (`products_scd2`, dimensions) sont recalculées en
entier dans un répertoire temporaire puis échangées par renommage HDFS (opération atomique sur
le NameNode).

## Conséquences

- Rejouer J2 trois fois laisse exactement le même contenu qu'une exécution unique (testé).
- Une exécution interrompue laisse au pire une partition incomplète que la suivante remplace.
- La table d'audit, journal par nature, est la seule écriture en `append`.

## Alternatives écartées

- `append` + déduplication en lecture : coût reporté sur chaque requête, doublons visibles.
- Delta Lake (`MERGE`) : hors programme, ajoute une dépendance et un format.
