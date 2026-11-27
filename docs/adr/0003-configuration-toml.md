# ADR-0003 : configuration TOML par environnement

**Statut** : acceptée

## Contexte

Le sujet demande une configuration externalisée. Le kit fournissait `conf/app.json`. Les mêmes
jobs tournent en local (tests) et sur le cluster (YARN, chemins HDFS).

## Décision

`conf/app.toml` porte les valeurs communes et `conf/env/<env>.toml` les valeurs d'un
environnement (`local`, `cluster`). Les fichiers sont lus par `tomllib` (bibliothèque standard de
Python 3.11), donc sans dépendance. Chaque clé peut être surchargée, et la dernière source
l'emporte :

1. `conf/app.toml` ;
2. `conf/env/<env>.toml`, où `<env>` est l'option `--env` de `run.py` ;
3. une variable d'environnement `GTDP_<SECTION>__<CLE>`, par exemple
   `GTDP_SPARK__SHUFFLE_PARTITIONS=64` ;
4. l'option `run.py --set <section>.<clé>=<valeur>`.

Une section ou une clé inconnue, ou une valeur du mauvais type, arrête le job avant le démarrage
de Spark (`ConfigError`, code de sortie 2).

Chemins : une valeur avec schéma (`hdfs://…`, `file://…`) est gardée telle quelle. Sur le
cluster, un chemin absolu sans schéma reçoit le système de fichiers de `paths.default_fs`
(`hdfs://namenode:8020`), et il est refusé si `default_fs` manque. En local, un chemin relatif
part de la racine du dépôt. Aucun chemin ne dépend donc du `fs.defaultFS` du conteneur.

Propriétés Spark : une propriété passée à `spark-submit` (`--conf` ou `spark-defaults.conf`)
l'emporte sur la configuration gtdp. `build_spark` journalise chaque valeur effective avec son
origine (`spark-submit` ou `gtdp`).

Secrets : le sel de pseudonymisation n'est jamais dans les fichiers, seulement dans la variable
`GTDP_PSEUDO_SALT`. Il est lu quand un job le lit, pas au chargement de la configuration. Il est
obligatoire sur le cluster pour les jobs qui pseudonymisent (silver), et vérifié avant le
démarrage de Spark : `@job(..., pseudonymizes=True)`, posé par le seul job `silver`, fait lire le
sel à `run.py` juste après la configuration, et son absence rend le code de sortie 2. Les autres
jobs (bronze, streaming, housekeeping, comparaison du Lot 6) et les rapports bronze et streaming
n'en ont pas besoin. Le rapport silver ne le lit que pour `--customer` et `--explain`, après le
démarrage de Spark. En local, un sel de développement fixe le remplace, avec un avertissement.

## Conséquences

- Un job reçoit `--env cluster` et ne contient aucun chemin en dur.
- La configuration effective est journalisée au démarrage, le sel masqué (`***`).
- Les journaux sont des lignes JSON (`gtdp.log`) écrites sur la sortie d'erreur du driver
  Python. Sous `spark-submit`, la JVM recopie cette sortie sur sa propre sortie standard : les
  lignes JSON sont sur la sortie standard de `spark-submit`, les lignes log4j de Spark sur sa
  sortie d'erreur.

## Alternatives écartées

- JSON du kit : pas de commentaires, peu lisible pour un tiers.
- YAML : dépendance supplémentaire à embarquer sur les exécuteurs.
