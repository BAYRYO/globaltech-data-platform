# Architecture cible

Plateforme de pilotage logistique et industriel de GlobalTech Industries : un data lake HDFS en
trois couches (bronze, silver, gold) alimenté par des dépôts batch quotidiens et par un flux
d'événements capteurs, traité par Spark sur YARN.

Ce document est la référence de conception. Les décisions structurantes sont détaillées dans
[`docs/adr/`](adr/). Le format exact des données sources est fixé par le
[contrat de données](data-contract.md) et le modèle cible par le [modèle de données](data-model.md).

## Vue d'ensemble

```mermaid
flowchart LR
  subgraph Sources
    GEN[Générateur déterministe<br/>J1 / J2 / J3]
    SIM[Simulateur capteurs<br/>1 fichier / 5 s]
  end
  subgraph HDFS["HDFS (NameNode + 3 DataNodes)"]
    LAND[/landing/]
    BRZ[(bronze<br/>Parquet brut)]
    SLV[(silver<br/>typé, dédupliqué, SCD2)]
    REJ[(rejects<br/>motif de rejet)]
    GLD[(gold<br/>étoile + KPI)]
    STR[(streaming<br/>rejets, checkpoints, archive)]
    OPS[(ops<br/>audit)]
  end
  subgraph YARN["YARN (ResourceManager + 2 NodeManagers)"]
    SB[Spark batch]
    SS[Spark Structured Streaming]
    MR[Hadoop Streaming]
  end
  GEN -->|hdfs dfs -put| LAND
  SIM -->|put + mv atomique| LAND
  LAND --> SB --> BRZ
  BRZ --> SB --> SLV
  SB --> REJ
  SLV --> SB --> GLD
  LAND --> SS --> GLD
  SS --> STR
  LAND --> MR
  SB & SS --> OPS
  SB & SS -.event logs.-> SHS[Spark History Server]
```

## Composants

| Composant | Rôle | Instances |
| --- | --- | --- |
| NameNode | Métadonnées HDFS, ACL, quotas, politiques d'erasure coding | 1 |
| DataNode | Stockage des blocs | 3 (minimum pour réplication 3 et EC `XOR-2-1`) |
| ResourceManager | Ordonnancement YARN | 1 |
| NodeManager | Exécution des conteneurs Spark et MapReduce | 2 |
| Spark History Server | Historique des applications Spark (`hdfs:///spark-logs`) | 1 |
| MapReduce JobHistory | Historique et compteurs des jobs MapReduce | 1 |
| Client (edge) | `spark-submit`, `hdfs`, `yarn`, générateur. Conteneur éphémère qui monte le dépôt courant | à la demande |

Versions : Hadoop 3.3.6, Spark 3.5.3 (build `without-hadoop`, branché sur le classpath Hadoop du
cluster), Python 3.11, Java 11. Voir [ADR-0001](adr/0001-image-hadoop-spark.md). Le budget mémoire
(tas de chaque démon, mémoire et vcores des NodeManagers, taille des exécuteurs) est calculé dans
le guide de la plateforme : Docker dispose de 15 444 Mio, alors que les valeurs par défaut de
Hadoop 3.3.6 feraient annoncer 8 Gio à chaque NodeManager.

## Arborescence HDFS

```
/datalake
├── landing/<source>/ingestion_date=YYYY-MM-DD/   fichiers déposés tels quels (CSV, log, JSON)
├── landing/_manifests/ingestion_date=…/          _MANIFEST.json du dépôt
├── landing/sensors/incoming/                     fichiers capteurs surveillés par le streaming
├── bronze/<source>/ingestion_date=YYYY-MM-DD/    Parquet, colonnes brutes en string + métadonnées
├── silver/<entité>/                              données typées et fiables
├── silver/rejects/<source>/ingestion_date=…/     lignes rejetées + motifs
├── silver/_days/ingestion_date=…/                <source>.json par source publiée, _SUCCESS si le jour est complet
├── gold/<table>/                                 modèle en étoile et indicateurs
├── gold/_state.json                              état de publication de gold (PUBLISHING, PUBLISHED)
├── _gold.lock                                    verrou d'une exécution gold, absent au repos
├── gold/sensor_aggregates/, gold/sensor_alerts/  agrégats et alertes du flux capteurs
├── streaming/sensors/checkpoints/, rejects/      checkpoints et lignes rejetées du flux capteurs
├── streaming/archive/sensors/arrival_date=…/     fichiers capteurs validés, jour courant et 2 jours précédents
├── streaming/control/                            marqueur d'arrêt du flux
├── ops/audit/run_date=…/run_id=…/                table d'audit, une ligne par exécution
└── ops/pseudonymization/salt_fingerprint.json    empreinte du sel de pseudonymisation
/spark-logs                                       event logs Spark (History Server)
/benchmarks                                       jeux de mesure (réplication contre EC)
```

`<source>` ∈ `orders`, `customers`, `products`, `weblogs`. Pendant une écriture, chaque table a
pour frères cachés son verrou `_<table>.lock` et ses répertoires de transit
(`_<table>.staging-<id>`, `_<table>.tmp`, `_<table>.old`). Le temps d'une prise ou d'une
libération du verrou s'y ajoutent la copie du verrou en cours d'écriture
(`_<table>.lock.tmp-<id>`) et la réclamation (`_<table>.lock.takeover-<empreinte>`), voir
[ADR-0002](adr/0002-parquet-idempotence.md). Les event logs de `/spark-logs` sont compressés et
découpés ([guide de la plateforme](platform.md#arborescence-hdfs-droits-et-quotas)).

Justification :

- **Une racine par couche** : les droits, quotas et politiques de stockage s'appliquent à un
  répertoire. Séparer les couches permet de donner l'écriture à l'ETL et la seule lecture de
  `gold` aux analystes.
- **Partition `ingestion_date` en landing et bronze** : c'est la clé de rejeu. Rejouer un jour
  réécrit exactement une partition, d'où l'absence de doublon (voir [ADR-0002](adr/0002-parquet-idempotence.md)).
- **Partitions métier en gold** (`order_date`) : les requêtes analytiques filtrent par date de
  commande, pas par date d'arrivée.

### Droits et quotas

`scripts/cluster/init-hdfs.sh` porte la table qui fait foi, reprise en entier dans le
[guide de la plateforme](platform.md#arborescence-hdfs-droits-et-quotas). Une ACL « par défaut
aussi » est héritée par chaque fichier et répertoire créé ensuite.

| Chemin | Propriétaire | Mode | ACL | Quota espace | Quota de noms |
| --- | --- | --- | --- | --- | --- |
| `/datalake/landing` | `etl:datalake` | `770` | — | 20 Go | 100 000 |
| `/datalake/bronze` | `etl:datalake` | `750` | — | 30 Go | 200 000 |
| `/datalake/silver` | `etl:datalake` | `750` | `group:analysts:r-x`, par défaut aussi | 30 Go | 200 000 |
| `/datalake/silver/rejects` | `etl:datalake` | `750` | `group:analysts:---`, par défaut aussi (lignes brutes, données personnelles en clair) | — | — |
| `/datalake/gold` | `etl:datalake` | `755` | `group:analysts:r-x`, par défaut aussi | 10 Go | 200 000 |
| `/datalake/streaming` | `etl:datalake` | `750` | — | 5 Go | 500 000 |
| `/datalake/ops` | `etl:datalake` | `750` | `group:ops:r-x`, par défaut aussi | 2 Go | 100 000 |
| `/spark-logs` | `spark:hadoop` | `1777` | — | 15 Go | 100 000 |
| `/benchmarks` | `etl:datalake` | `755` | — | 20 Go | 100 000 |

Les quotas d'espace HDFS comptent l'espace physique (réplication incluse) : 30 Go de quota pour
la couche bronze correspondent à 10 Go logiques en réplication 3.

## Flux batch

Pour chaque jour d'ingestion `D` (J1 = 2026-01-01, J2 = 2026-01-02, J3 = 2026-01-03) :

1. **Dépôt** : `scripts/cluster/land.sh <dépôt> D` vérifie le dépôt local du générateur contre
   son manifeste, puis le publie vers `/datalake/landing/<source>/ingestion_date=D/` en remplaçant
   le landing précédent de `D` d'un bloc ([guide de la plateforme](platform.md#déposer-un-jour-de-données)).
2. **Bronze** : un fichier différent de son manifeste (SHA-256, nombre de lignes), hors UTF-8 ou
   dont l'en-tête n'a pas une colonne obligatoire est refusé avant toute écriture. Chaque fichier
   est lu avec un schéma tout en `string` dérivé de son en-tête (aucune perte, changement de
   schéma toléré, [ADR-0007](adr/0007-evolution-schema.md)), avec ajout de `_ingested_at`,
   `_source_file`, `_run_id`, `_record_hash`, `_corrupt_record`. Écriture Parquet dans la
   partition `ingestion_date=D`, remplacée en entier.
3. **Silver** ([règles et disposition](data-model.md#règles-de-qualité-et-disposition)) :
   - refus avant toute écriture si le sel est inutilisable ou différent de celui de l'empreinte,
     si un jour antérieur n'est pas complet ou si un jour postérieur l'est
     ([ADR-0005](adr/0005-silver-incremental.md)) ;
   - sources traitées dans l'ordre `customers`, `products`, `orders`, `weblogs` : les commandes
     sont contrôlées contre les dimensions publiées pour `D` ;
   - doublons exacts, contrôles qualité en une seule projection (registre de règles), séparation
     propres / rejets, doublons de clé intra-jour et inter-jours contre les jours complets ;
   - réconciliation `propres + rejets + doublons = total bronze(D)` et unicité de la clé,
     vérifiées avant l'écriture puis sur les données relues, et tracées dans l'audit ; un échec
     après l'écriture retire la publication de la source ;
   - produits historisés en SCD type 2, clients en SCD type 1 pseudonymisés, logs web parsés et
     pseudonymisés ;
   - un marqueur par source publiée, puis `_SUCCESS` quand le jour est complet.
4. **Gold** ([modèle](data-model.md#gold--modèle-en-étoile)) :
   - lit les seuls jours silver terminés, dont `D`, sous le verrou `/datalake/_gold.lock` tenu
     pendant toute l'exécution ;
   - recalcule tout depuis ces jours : dimensions, fait des ventes au prix applicable à la date
     de commande, logs web, neuf indicateurs ;
   - contrôle le grain, les clés, les colonnes obligatoires et les références avant toute
     écriture ;
   - réécrit les seules dates `order_date` dont le contenu change et supprime les dates sans
     commande, échange les autres tables en entier, relit `fact_sales` ;
   - `gold/_state.json` vaut `PUBLISHING` pendant l'écriture, puis `PUBLISHED`.
5. **Audit** : chaque job ajoute une ligne à `/datalake/ops/audit` (identifiant d'exécution,
   application YARN, compteurs, durée, statut `SUCCESS`, `FAILED` ou `STOPPED`), même quand il
   échoue ou qu'un signal l'arrête ([interroger l'audit](runbook/README.md#interroger-laudit)).

Chaque étape est idempotente : la rejouer converge vers le même état (voir
[ADR-0002](adr/0002-parquet-idempotence.md) et [ADR-0005](adr/0005-silver-incremental.md)).

## Flux streaming

Le simulateur rejoue les fichiers capteurs pré-générés vers `/datalake/landing/sensors/incoming`
à raison d'un fichier toutes les 5 secondes (écriture dans un répertoire de transit puis `mv`
atomique, pour que Spark ne lise jamais un fichier partiel).

Le job Structured Streaming `streaming-sensors` lit ce répertoire avec un schéma explicite,
toutes les 30 secondes et au plus 12 fichiers par micro-batch. Il applique un watermark de
2 minutes sur `event_time` et déduplique par `event_id` dans la limite du watermark. Il calcule
des agrégats sur des fenêtres d'une minute par machine et métrique (mode `append` : chaque fenêtre
est écrite une fois, définitive). Il émet une alerte par capteur et par fenêtre d'une minute, dès
le micro-batch où arrive la première valeur hors seuil, et range les lignes hors contrat dans
`streaming/rejects`. Il écrit en `foreachBatch` vers `gold/sensor_aggregates`,
`gold/sensor_alerts` et `streaming/rejects`, dans des partitions `<jour>=<d>/batch_id=<n>`
remplacées : la réécriture d'un micro-batch rejoué est idempotente. Le checkpoint HDFS permet la
reprise après arrêt, et un marqueur d'arrêt (`scripts/streaming/stop.sh`) l'arrête proprement.

Le job `streaming-housekeeping`, lancé toutes les heures à côté du flux, archive les fichiers
lus par les trois requêtes, purge l'archive et fusionne les partitions des micro-batches que Spark
ne rejoue plus. Détails mesurés : [ADR-0008](adr/0008-streaming.md) et le
[runbook streaming](runbook/streaming.md).

## Comparaison MapReduce contre Spark (Lot 6)

Un même calcul, deux moteurs, mêmes entrées et mêmes règles :

| Élément | Définition |
| --- | --- |
| Entrée | fichiers CSV de commandes en landing (J1, ou J1 à J3), lus bruts par les deux moteurs |
| Jointure pays | `customer_id` → pays par le référentiel clients : cache distribué (`-files`) et jointure côté map en Hadoop Streaming, jointure diffusée en Spark ; un client sans région (anomalie `MISSING_REGION`, 0,99 % des lignes valides) est compté dans le pays `UNKNOWN` par les deux moteurs, comme en silver |
| Mesure | montant facturé `quantity × unit_price_eur × (1 − discount_pct)` des lignes `COMPLETED` (pas de SCD2, impossible dans un mapper sans état) |
| Lignes invalides | un compteur par motif, chaque ligne comptée sous la première règle qu'elle enfreint, dans cet ordre : `MALFORMED_ROW` (nombre de champs différent de 8 ou 9 ; le contrat garantit qu'une ligne malformée n'a jamais 8 ni 9 champs), `BAD_QUANTITY`, `BAD_PRICE`, `BAD_TIMESTAMP`, `NULL_CUSTOMER`, `UNKNOWN_CUSTOMER`, `BAD_ENUM`, `BAD_DISCOUNT` ; compteurs Hadoop (`reporter:counter:`, groupe `Quality`) contre une agrégation Spark |
| Comparaison | durée (application YARN), lignes lues (`Map input records` contre métriques d'entrée Spark), lignes de code, résultats identiques au centime |

## Banc de performance (Lot 7)

Indicateur mesuré : CA mensuel par pays et catégorie au prix applicable, soit la jointure
commandes × `products_scd2` × clients puis l'agrégation, sur le profil `full` (10 M de lignes,
`P00042` porte 20 % des lignes synthétiques).

| Version | Réglages |
| --- | --- |
| Naïve | CSV avec `inferSchema`, jointure triée sans diffusion (`autoBroadcastJoinThreshold=-1`), AQE désactivée, 200 partitions de shuffle, aucun cache, sortie CSV gzip |
| Optimisée | leviers ajoutés un par un et mesurés séparément |

Leviers, chacun mesuré contre la version précédente (médiane de 3 exécutions sur YARN, durée,
octets de shuffle, ratio max/médiane des tâches) :

1. format : Parquet contre CSV ;
2. partitionnement : élagage de partitions sur la date contre lecture complète ;
3. skew, deux méthodes comparées : **salage** de la clé chaude, et **AQE skew join** avec seuils
   ajustés ; la jointure diffusée, qui supprime le shuffle, est mesurée comme référence. Le
   diagnostic (ratio max/médiane des enregistrements par tâche) n'est visible qu'avec diffusion et
   AQE désactivées : la revue du Lot 0 a mesuré 49,7 dans ce cas contre 1,4 avec AQE ;
4. cache : seulement pour un DataFrame réutilisé par plusieurs indicateurs, gain mesuré ;
5. nombre de partitions de shuffle : balayage 8, 16, 32, 64, 200, choix par la mesure ;
6. compression : snappy, zstd, gzip (taille et durée).

Trois plans `explain` commentés : jointure triée avec skew, jointure diffusée, jointure salée.
Réplication 3 contre erasure coding : voir [ADR-0006](adr/0006-topologie-hdfs.md).

## Organisation du code

```
src/gtdp/
├── config.py        configuration TOML, surcharges (variables GTDP_*, --set), chemins par environnement
├── log.py           journalisation JSON, une ligne par événement
├── spark.py         construction de la SparkSession (--conf de spark-submit prioritaire)
├── io.py            écritures sous verrou (write_partitions, replace_table, drop_partitions), lecture typée (read_table)
├── audit.py         table d'audit, une ligne par exécution (SUCCESS, FAILED, STOPPED)
├── schemas.py       colonnes canoniques des sources, schémas bronze et silver
├── datagen/         générateur J1/J2/J3, vérificateur, rejeu des capteurs
├── bronze/          lecture du landing contre le manifeste, colonnes techniques, rapport
├── silver/          quality.py (registre de règles, disposition), un module par source,
│                    days.py (marqueurs des jours complets), pseudonymize.py, reconcile.py, report.py
├── perf/            calcul Spark du Lot 6 et outil de comparaison avec MapReduce
├── streaming/       sensors.py (lecture, fenêtres, alertes), stream.py (requêtes, puits, arrêt), housekeeping.py
├── gold/            tables.py (schémas, clés, références), dimensions.py, facts.py, kpis.py,
│                    checks.py (invariants, empreintes), state.py (_state.json), report.py
└── jobs/            run.py (point d'entrée spark-submit), registry.py, catalog.py, un module par job
mapreduce/           mapper et reducer Hadoop Streaming
scripts/             exploitation du cluster
conf/                configuration externalisée
tests/               pytest (Spark local)
```

Les transformations sont des fonctions pures `DataFrame -> DataFrame`, testées en Spark local.
Les jobs sont des coquilles minces qui lisent la configuration, appellent les transformations,
écrivent et tracent l'audit. Les traitements de référence s'exécutent sur YARN.

## Contraintes du sujet et réponse

| Contrainte | Réponse |
| --- | --- |
| Traitements finaux sur YARN | `scripts/submit.sh` soumet en `--master yarn` depuis un client éphémère |
| Pas d'UDF Python si une fonction native existe | Contrôles, parsing de logs (`regexp_extract`), pseudonymisation (`sha2`) en fonctions natives |
| Choix justifiés par une mesure | `docs/benchmarks.md`, `docs/mapreduce-vs-spark.md`, ADR chiffrés |
| Versions du cours | Spark 3.5.3 (celle du kit), Hadoop 3.3.6 |
| Historique Git lisible | Une branche et une PR par story Jira, commits conventionnels référencés `KAN-xx` |
