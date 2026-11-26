# ADR-0001 : image unique Hadoop 3.3.6 + Spark 3.5.3

**Statut** : acceptée

## Contexte

Le kit proposait `bitnami/spark:3.5`, qui ne fournit ni HDFS ni YARN, alors que le sujet impose
un cluster NameNode, DataNodes, ResourceManager, NodeManagers et des traitements finaux sur YARN.
Les images Bitnami ne sont par ailleurs plus publiées sous ce nom depuis 2025. Les versions
doivent rester celles du cours : Spark 3.5.3 (`pyspark==3.5.3` dans le kit).

## Décision

Le projet construit **une seule image** (`docker/Dockerfile`) contenant Java 11, Hadoop 3.3.6,
Spark 3.5.3 en build `without-hadoop` (branché sur `hadoop classpath`) et Python 3.11. Chaque
service du `docker-compose.yml` lance un rôle différent de la même image. Les archives Apache
sont vérifiées par leur somme SHA-512.

## Conséquences

- Une seule version de Hadoop sur tout le cluster, client compris : pas de conflit de jars.
- Les exécuteurs YARN ont le même Python que le driver, condition de PySpark sur YARN.
- Le premier build télécharge environ 800 Mo ; il est mis en cache ensuite.

## Alternatives écartées

- Images séparées `apache/hadoop` + `apache/spark` : deux versions de Hadoop à aligner, Python
  absent des NodeManagers.
- Spark standalone : ne répond pas à la contrainte YARN.
