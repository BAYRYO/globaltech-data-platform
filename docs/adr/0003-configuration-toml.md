# ADR-0003 : configuration TOML par environnement

**Statut** : acceptée

## Contexte

Le sujet demande une configuration externalisée. Le kit fournissait `conf/app.json`. Les mêmes
jobs tournent en local (tests) et sur le cluster (YARN, chemins HDFS).

## Décision

`conf/app.toml` porte les valeurs communes, `conf/env/<env>.toml` les surcharges (`local`,
`cluster`), et toute clé peut être surchargée par une variable `GTDP_<SECTION>__<CLE>`. Le
fichier est lu par `tomllib` (bibliothèque standard de Python 3.11), donc sans dépendance. Les
secrets (sel de pseudonymisation) ne sont jamais dans les fichiers : uniquement en variable
d'environnement.

## Conséquences

- Un job reçoit `--env cluster` et ne contient aucun chemin en dur.
- La configuration effective est journalisée au démarrage (secrets masqués).

## Alternatives écartées

- JSON du kit : pas de commentaires, peu lisible pour un tiers.
- YAML : dépendance supplémentaire à embarquer sur les exécuteurs.
