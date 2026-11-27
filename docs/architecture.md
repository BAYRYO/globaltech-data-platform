# Architecture cible

Plateforme de pilotage logistique et industriel de GlobalTech Industries : un data lake HDFS en
trois couches (bronze, silver, gold) alimenté par des dépôts batch quotidiens et par un flux
d'événements capteurs, traité par Spark sur YARN.

Ce document est la référence de conception. Les décisions structurantes sont détaillées dans
[`docs/adr/`](adr/). Le format exact des données sources est fixé par le
[contrat de données](data-contract.md) et le modèle cible par le [modèle de données](data-model.md).

## Vue d'ensemble

Le schéma montre les services du cluster, les zones du lake et les flux qui les relient. La
légende sous le schéma explique chaque forme, chaque couleur et chaque trait. Chaque couleur de
trait a un contraste d'au moins 3:1 sur le fond clair et sur le fond sombre de GitHub.

```mermaid
flowchart TB
  subgraph EDGE["Client edge : conteneur lancé à la demande"]
    GEN(["Générateur<br/>dépôts J1, J2, J3"]):::program
    SIM(["Simulateur capteurs<br/>1 fichier toutes les 5 s"]):::program
    CLI[["land.sh, submit.sh,<br/>spark-submit, hdfs, yarn"]]:::client
  end
  subgraph YARN["YARN"]
    RM["ResourceManager"]:::service
    NM1["NodeManager 1"]:::service
    NM2["NodeManager 2"]:::service
    subgraph APPS["Applications dans les conteneurs des NodeManagers"]
      subgraph SPARK["Jobs Spark de run.py"]
        JB(["job bronze"]):::job
        JS(["job silver"]):::job
        JG(["job gold"]):::job
        SS(["jobs streaming-sensors<br/>et streaming-housekeeping"]):::job
        RBC(["Lot 6 : revenue-by-country-spark"]):::job
      end
      MRS(["Lot 6 : job Hadoop Streaming"]):::job
    end
  end
  subgraph HDFS["HDFS"]
    NN["NameNode"]:::service
    DN1["DataNode 1"]:::service
    DN2["DataNode 2"]:::service
    DN3["DataNode 3"]:::service
    subgraph LAKE["/datalake, en blocs sur les DataNodes"]
      LAND[/"landing"/]:::landing
      BRZ[("bronze")]:::bronze
      SLV[("silver")]:::silver
      REJ[("silver/rejects")]:::rejects
      GLD[("gold")]:::gold
      STR[("streaming")]:::streaming
      OPS[("ops")]:::ops
    end
    SLOG[("/spark-logs")]:::logs
    ALOG[("/app-logs")]:::logs
    MLOG[("/mr-history")]:::logs
  end
  subgraph HIST["Historique"]
    SHS["Spark History Server"]:::service
    JHS["MapReduce JobHistory"]:::service
  end

  CLI -->|"soumet"| RM
  RM -->|"alloue les conteneurs"| NM1
  RM --> NM2
  NN --- DN1
  NN --- DN2
  NN --- DN3
  GEN ==>|"dépôts J1 à J3"| CLI
  CLI ==>|"land.sh"| LAND
  LAND ==> JB
  JB ==> BRZ
  BRZ ==> JS
  JS ==> SLV
  JS ==> REJ
  JS ==>|"empreinte du sel"| OPS
  SLV ==> JG
  REJ ==>|"kpi_data_quality"| JG
  JG ==> GLD
  SIM ==>|"hdfs dfs -put puis -mv"| LAND
  LAND ==>|"sensors/incoming"| SS
  SS ==>|"agrégats, alertes"| GLD
  SS ==>|"rejets, checkpoints, archive"| STR
  LAND ==>|"commandes, clients"| MRS
  LAND ==>|"commandes, clients"| RBC
  MRS ==>|"mr_vs_spark/mapreduce"| OPS
  RBC ==>|"mr_vs_spark/spark"| OPS
  SPARK -.->|"audit"| OPS
  SPARK -.->|"event logs"| SLOG
  MRS -.->|"historique, compteurs"| MLOG
  NM1 -.->|"journaux des conteneurs"| ALOG
  NM2 -.-> ALOG
  SLOG -.-> SHS
  MLOG -.-> JHS
  ALOG -.-> JHS

  classDef service fill:#dbeafe,stroke:#1e40af,color:#111827
  classDef client fill:#fce7f3,stroke:#9d174d,color:#111827
  classDef program fill:#ffffff,stroke:#6b7280,color:#111827
  classDef job fill:#cffafe,stroke:#0e7490,color:#111827
  classDef landing fill:#ffffff,stroke:#6b7280,stroke-dasharray:4 3,color:#111827
  classDef bronze fill:#fed7aa,stroke:#9a3412,color:#111827
  classDef silver fill:#cbd5e1,stroke:#334155,color:#111827
  classDef rejects fill:#fecaca,stroke:#b91c1c,color:#111827
  classDef gold fill:#fde68a,stroke:#a16207,color:#111827
  classDef streaming fill:#e9d5ff,stroke:#7e22ce,color:#111827
  classDef ops fill:#bbf7d0,stroke:#15803d,color:#111827
  classDef logs fill:#f3f4f6,stroke:#6b7280,color:#111827
  classDef frame fill:#f8fafc,stroke:#94a3b8,color:#111827
  class EDGE,YARN,APPS,SPARK,HDFS,LAKE,HIST frame
  linkStyle 0,1,2 stroke:#db2777,stroke-width:1px
  linkStyle 3,4,5 stroke:#0284c7,stroke-width:1px
  linkStyle 6,7,8,9,10,11,12,13,14,15,16 stroke:#2563eb,stroke-width:3px
  linkStyle 17,18,19,20 stroke:#9333ea,stroke-width:3px
  linkStyle 21,22,23,24 stroke:#ea580c,stroke-width:3px
  linkStyle 25 stroke:#15803d,stroke-width:2px
  linkStyle 26,27,28,29,30,31,32 stroke:#6b7280,stroke-width:2px
```

| Élément du schéma | Signification |
| --- | --- |
| Cadre gris clair | Groupe de composants : client edge, YARN, HDFS, serveurs d'historique. Un cadre dans un cadre en est une partie : les applications tournent dans les conteneurs des NodeManagers, les jobs Spark de `run.py` sont des applications, les zones du lake sont stockées en blocs sur les DataNodes. |
| Rectangle bleu | Service du cluster, un démon par rectangle : NameNode, DataNode, ResourceManager, NodeManager, Spark History Server, MapReduce JobHistory. |
| Rectangle rose à double bord | Outils du client edge, conteneur lancé à la demande qui monte la copie du dépôt : `land.sh` dépose les données, `submit.sh` soumet les jobs par `spark-submit`. En mode client, le driver Spark tourne dans ce conteneur. |
| Ovale blanc | Programme Python du dépôt lancé dans le client edge : le générateur écrit les dépôts batch dans la copie du dépôt, le simulateur rejoue les fichiers capteurs dans HDFS. |
| Ovale cyan | Application soumise à YARN : un job Spark de `run.py`, ou le job Hadoop Streaming du Lot 6 (`scripts/mapreduce/run_mr.sh`). |
| Parallélogramme blanc à bord pointillé | Zone `landing` : fichiers déposés tels quels (CSV, logs, JSON Lines) et manifestes. |
| Cylindre orange | Couche `bronze` : Parquet, colonnes brutes en `string`. |
| Cylindre gris bleu | Couche `silver` : tables typées, dédupliquées, historisées, pseudonymisées. |
| Cylindre rouge | Rejets de silver (`silver/rejects/<source>`) : lignes brutes et motifs. |
| Cylindre jaune | Couche `gold` : modèle en étoile, indicateurs, agrégats et alertes capteurs. |
| Cylindre violet | Zone `streaming` : rejets du flux, checkpoints, fichiers capteurs archivés. |
| Cylindre vert | Zone `ops` : table d'audit, sorties du Lot 6, empreinte du sel. |
| Cylindre gris clair | Journaux des services : event logs Spark (`/spark-logs`), journaux des conteneurs agrégés par YARN (`/app-logs`), historique MapReduce (`/mr-history`). |
| Flèche épaisse bleue | Flux batch d'un dépôt J1, J2 ou J3 : dépôt en landing, puis jobs `bronze`, `silver` (tables, rejets, empreinte du sel dans `ops`) et `gold`, qui relit aussi les rejets pour `kpi_data_quality`. |
| Flèche épaisse violette | Flux capteurs : fichiers posés en landing par le simulateur, lus par le streaming, écrits en gold et dans la zone `streaming`. |
| Flèche épaisse orange | Comparaison MapReduce contre Spark du Lot 6 : les deux jobs lisent les mêmes fichiers du landing et écrivent sous `ops/mr_vs_spark`. |
| Flèche pointillée verte | Audit : chaque job Spark lancé par `run.py` ajoute une ligne à `ops/audit`, aussi en cas d'échec. Le job Hadoop Streaming n'en écrit pas. |
| Flèche pointillée grise | Journaux : event logs des jobs Spark, historique et compteurs du job Hadoop Streaming, journaux des conteneurs agrégés par les NodeManagers. Les serveurs d'historique les servent. |
| Flèche fine rose | Commande : le client soumet un job au ResourceManager, qui alloue les conteneurs des NodeManagers. |
| Trait fin bleu ciel sans pointe | Le NameNode tient les métadonnées des blocs que stockent les DataNodes. |
| Pointe de flèche | Sens du flux : la donnée ou la commande va vers la pointe. |
| Texte sur un trait | Ce qui passe : dépôts, commande, répertoire lu, tables écrites, ou l'indicateur pour lequel gold lit les rejets. |

## Composants

| Composant | Rôle | Instances |
| --- | --- | --- |
| NameNode | Métadonnées HDFS, ACL, quotas, politiques d'erasure coding | 1 |
| DataNode | Stockage des blocs | 3 (minimum pour réplication 3 et EC `XOR-2-1`) |
| ResourceManager | Ordonnancement YARN | 1 |
| NodeManager | Exécution des conteneurs Spark et MapReduce | 2 |
| Spark History Server | Historique des applications Spark (`hdfs:///spark-logs`) | 1 |
| MapReduce JobHistory | Historique et compteurs des jobs MapReduce | 1 |
| Client (edge) | `land.sh`, `submit.sh`, `spark-submit`, `hdfs`, `yarn`, le générateur et le simulateur capteurs. Conteneur éphémère qui monte le dépôt courant | à la demande |

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
