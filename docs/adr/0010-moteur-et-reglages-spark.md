# ADR-0010 : Spark pour les traitements batch, réglages retenus après les Lots 6 et 7

**Statut** : acceptée

Cet ADR consigne après coup des décisions prises pendant les Lots 6 (#8) et 7
(KAN-44 à KAN-47). Les mesures sont dans [MapReduce contre Spark](../mapreduce-vs-spark.md) et
[performance et optimisation](../performance.md). Il distingue ce que la plateforme applique de
ce que les mesures recommandent sans que le code l'applique.

## Contexte

Le sujet demande de comparer MapReduce et Spark (Lot 6) et de classer les leviers d'optimisation
par impact (Lot 7). Les mesures tournent sur une seule machine : 2 NodeManagers, 2 exécuteurs de
2 cœurs, 4 tâches en parallèle. Une différence de durée n'est retenue que si le p bilatéral d'un
test exact de Mann-Whitney, 5 exécutions contre 5, est inférieur à 0,05
([protocole](../performance.md#protocole-commun)).

## Décision

**Moteur.** Les traitements batch sont écrits en Spark. La campagne de 45 exécutions ne montre
aucune différence de durée détectable sur 10 M de lignes (p = 0,095 et 0,841), et MapReduce est
plus rapide sur 20 110 lignes. Le choix tient au CPU par ligne, environ 1,7 à 2,0 fois plus bas
en Spark sur 10 M de lignes (inféré, la part Python du mapper étant mesurée hors du cluster), et au
code de plomberie distribuée que Spark n'oblige pas à écrire
([conclusion](../mapreduce-vs-spark.md#conclusion)).

**Réglages Spark.** Ce que la plateforme applique, et la mesure qui le justifie :

| Réglage | Valeur appliquée | Où | Mesure |
| --- | --- | --- | --- |
| Partitions de shuffle | 16 | `conf/env/cluster.toml` | de 4 à 64, aucune différence de durée détectable, 200 : +9 % (p = 0,016) ; balayage fait avec le regroupement d'AQE désactivé ([partitions](../performance.md#nombre-de-partitions-de-shuffle)) |
| AQE | active, regroupement des partitions et skew join actifs, seuils par défaut | `docker/conf/spark/spark-defaults.conf`, `conf/app.toml` | avec les seuils par défaut (facteur 5, 256 Mo), AQE ne détecte pas la partition chaude de la jointure de `fact_sales` (déduit des seuils, aucune variante mesurée avec eux) ([skew](../performance.md#déséquilibre-de-clé-kan-45)) |
| Diffusion | seuil par défaut de Spark, 10 Mo ; aucun indice de diffusion dans le code | aucun réglage | diffusion −48 % contre une jointure triée sans AQE (p = 0,008) ; le plan du code gold sur le lac `dev` diffuse `dim_product` ([plan](../performance.md#jointure-de-la-construction-de-fact_sales)) |
| Cache | gold persiste ses tables (`MEMORY_AND_DISK`) et relit `fact_sales` au moins 10 fois | `src/gtdp/jobs/gold.py` | sur la lignée diffusée, le cache perd à 3 indicateurs (+52,0 %, p = 0,008) et ne se distingue pas à 6 (p = 0,421) ; le seuil d'environ 7 relectures est inféré des durées ([cache](../performance.md#cache)) |
| Codec Parquet | snappy | `src/gtdp/spark.py` | voir les recommandations ci-dessous |

**Recommandations non appliquées.** Les mesures les justifient, mais le code ne les applique pas :

| Recommandation | Mesure | État |
| --- | --- | --- |
| Lister les partitions côté driver (`spark.sql.sources.parallelPartitionDiscovery.threshold` relevé) | +1,4 % contre Parquet non partitionné (p = 0,690), contre +79,4 % (p = 0,008) quand Spark liste par un job | réglage du banc seulement (`src/gtdp/perf/bench.py`) |
| zstd pour les tables froides | −24,7 % d'octets stockés, durée sans différence détectable (p = 0,841) ; coût d'écriture non mesuré | snappy pour toutes les tables du lac |
| Erasure coding `XOR-2-1` pour les gros fichiers froids | −50 % d'octets physiques, lecture sans différence détectable (p = 0,095) | posé sur `/benchmarks/ec` seulement ; le lac reste en réplication 3 |
| `_record_hash` stocké en binaire | −47,4 % sur la colonne | constat transmis, bronze inchangé |

## Conséquences

- `gold/fact_sales` compte 367 dossiers de dates sur le lac `dev`, au-delà des 32 chemins où
  Spark liste par un job. D'après la mesure du Lot 7, une lecture de cette table paie ce listage.
  C'est déduit, pas mesuré sur gold.
- Sur le cluster, AQE peut regrouper les 16 partitions d'une agrégation. La seule variante mesurée
  avec regroupement (200 partitions ramenées à 5 tâches) est 2,9 % plus lente que 16 partitions
  fixes (p = 0,016). 16 partitions avec regroupement n'a pas été mesuré.
- Le gain du cache dans le job gold n'est pas mesuré : le choix s'appuie sur le seuil inféré.
- La décision de diffusion du job gold porte sur la dimension qu'il vient de construire et de
  persister, dont l'estimation de taille n'est pas mesurée. Au-delà de 10 Mo, la jointure redevient
  triée, et AQE skew join ne se déclenche qu'avec des seuils adaptés au volume.

## Alternatives écartées

- MapReduce pour les traitements batch : plus de CPU par ligne, et chaque optimisation coûte du
  code (tri, combiner, cache distribué, arithmétique entière)
  ([arbitrage](../mapreduce-vs-spark.md#arbitrage-complexité-contre-performance)).
- 200 partitions de shuffle, la valeur par défaut de Spark : +9 % sur l'agrégation mesurée.
- Salage de la clé chaude : plus lent dans les trois séries (p = 0,016, 0,016 et 0,008) et +34 %
  de shuffle ([skew](../performance.md#ce-que-disent-ces-chiffres-1)).
