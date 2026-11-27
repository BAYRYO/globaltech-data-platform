# Dictionnaire de données

Une page par couche ou par domaine. Chaque page décrit chaque colonne de ses tables : nom, type,
règle, exemple. Les entités, les clés et le traitement de chaque anomalie sont fixés par le
[modèle de données](../data-model.md). L'exploitation des tables est décrite dans les
[runbooks](../runbook/README.md).

| Page | Contenu |
| --- | --- |
| [Bronze](bronze.md) | `bronze/<source>` : colonnes brutes des quatre sources, colonnes techniques, valeurs vides, lignes corrompues, colonnes inconnues |
| [Silver](silver.md) | `silver/orders`, `customers`, `products_scd2`, `weblogs` et `rejects/<source>` : jours complets, disposition d'une ligne, formats acceptés, règles, pseudonymisation |
| [Gold](gold.md) | modèle en étoile (`fact_sales`, `fact_web_events`, `dim_date`, `dim_customer`, `dim_product`), neuf indicateurs, `_state.json` |
| [Flux capteurs](gold-sensors.md) | `gold/sensor_aggregates`, `gold/sensor_alerts`, `streaming/rejects` : événements retenus, fenêtres, alertes et seuils, motifs de rejet, fichiers d'entrée archivés |
| [MapReduce contre Spark](mapreduce.md) | sorties du calcul du Lot 6 : chiffre d'affaires par pays, compteurs de lignes invalides, ligne d'audit du job Spark |
| [Ops](ops.md) | `ops/audit` (une ligne par exécution), `ops/smoke`, `ops/pseudonymization/salt_fingerprint.json` |

## Conventions

- **Types.** Les tables Parquet donnent les types Spark SQL : `string`, `int`, `bigint`,
  `decimal(p,s)`, `date`, `timestamp`, `boolean`, `array<string>`. Les montants sont des
  décimaux, jamais des flottants : `decimal(10,2)` pour un prix, `decimal(20,2)` pour un montant
  de ligne gold, `decimal(30,2)` pour une somme d'indicateur ; le dictionnaire gold donne le type
  de chaque moyenne. Les horodatages sont en UTC, fuseau de la session Spark. Les sorties texte du
  Lot 6 donnent le type de la valeur écrite.
- **Schémas.** Les tables lues par un job ont un schéma explicite (`StructType`) dans le module
  de leur couche : `src/gtdp/schemas.py` (bronze, silver), `src/gtdp/gold/tables.py` (étoile
  gold), `src/gtdp/streaming/sensors.py` (flux capteurs). Les jobs les lisent avec ce schéma par
  `read_table(spark, chemin, schéma)` ([ADR-0002](../adr/0002-parquet-idempotence.md)). Le
  schéma d'un indicateur gold est celui que calcule sa fonction (`KPIS` de
  `src/gtdp/gold/kpis.py`), celui de l'audit est `_ROW_SCHEMA` de `src/gtdp/audit.py` plus ses
  deux colonnes de partition.
- **Sections.** Le titre d'une section nomme entre accents graves le chemin de chaque table ou
  fichier qu'elle décrit, depuis la racine du lake (`gold/fact_sales`). Un segment `<nom>`
  vaut pour toute valeur (`bronze/<source>`). Les colonnes d'une table sont dans un tableau
  `Colonne | Type | Règle | Exemple`, les champs d'un fichier JSON dans un tableau
  `Champ | Type | Règle | Exemple`. La règle d'une colonne de partition commence par
  « Partition ».
- **Vérification.** `tests/docs/test_data_dictionary.py` compare ces tableaux aux registres du
  code : chaque table et chaque fichier JSON que le code écrit a une section, avec les mêmes
  colonnes, les mêmes types (`simpleString()` de Spark) et les mêmes colonnes de partition. Une
  colonne n'est décrite qu'une fois par chemin, et un tableau de colonnes est sous un titre qui
  nomme son chemin (sauf les sorties texte du Lot 6). Les jobs `bronze`, `silver` et `gold`
  écrivent chaque table par `gtdp.publish.publish`, qui refuse des lignes dont les colonnes, les
  types ou l'ordre diffèrent du schéma déclaré et prend les colonnes de partition dans le
  registre : ces jobs écrivent ce que le registre déclare. Le flux capteurs
  (`src/gtdp/streaming/stream.py`) met chaque micro-batch dans les colonnes de son schéma
  (`_as_table`), sans passer par `publish`. `tests/docs/test_dictionary_rules.py`
  compare les codes de chaque tableau de règles silver à `ENTITY.rules`, les motifs de rejet du
  flux à `REJECT_RULES` et les seuils à `[streaming.thresholds]`. Un écart fait échouer la CI.
  Les filtres de statut et les formules des indicateurs sont relus à la main.
- **Nullabilité.** Les schémas déclarent `nullable=False` pour une colonne jamais nulle. La
  colonne « Règle » dit quand une valeur est nulle : champ vide, valeur hors format, colonne
  absente avant J3. Relue depuis Parquet, toute colonne apparaît nullable : Spark ne garde pas la
  contrainte dans les fichiers.
- **Règles.** En silver, une valeur hors format devient nulle, puis la règle correspondante la
  signale. Chaque code de rejet ou drapeau vient du registre de règles de la source
  (`ENTITY.rules` de `src/gtdp/silver/<source>.py`), avec le code du manifeste qu'il compte.
- **Exemples.** Valeurs du profil `dev` (graine 42) publiées sur le cluster. Un hachage peut être
  abrégé (`ee04c102…eea549dad`).
- **Colonnes techniques.** Elles commencent par `_` (`_record_hash`, `_run_id`, `_source_file`).
  Une colonne de partition apparaît dans le chemin sous la forme `<colonne>=<valeur>`.

## Inventaire

Tout ce que la plateforme écrit, avec le code qui l'écrit. Chaque table et chaque fichier
structuré a sa section dans ce dictionnaire, sauf les fichiers de la dernière table, décrits
ailleurs pour la raison donnée.

| Table ou fichier | Format | Écrit par | Section |
| --- | --- | --- | --- |
| `bronze/<source>` (4 tables) | Parquet | job `bronze` (`src/gtdp/jobs/bronze.py`) | [bronze](bronze.md) |
| `silver/orders`, `silver/customers`, `silver/products_scd2`, `silver/weblogs` | Parquet | job `silver` (`src/gtdp/jobs/silver.py`) | [silver](silver.md) |
| `silver/rejects/<source>` (4 tables) | Parquet | job `silver` | [silver](silver.md#rejets-silverrejectssource) |
| `silver/_days/ingestion_date=<d>/<source>.json`, `_SUCCESS`, `silver/_days/rebuilt/<source>.json` | JSON | job `silver` (`src/gtdp/silver/days.py`) | [silver](silver.md#jours-complets) |
| `gold/fact_sales`, `gold/fact_web_events`, `gold/dim_date`, `gold/dim_customer`, `gold/dim_product` | Parquet | job `gold` (`src/gtdp/jobs/gold.py`) | [gold](gold.md) |
| `gold/kpi_*` (9 tables) | Parquet | job `gold` (`src/gtdp/gold/kpis.py`) | [gold](gold.md#indicateurs) |
| `gold/_state.json` | JSON | job `gold` (`src/gtdp/gold/state.py`) | [gold](gold.md#gold_statejson) |
| `gold/sensor_aggregates`, `gold/sensor_alerts`, `streaming/rejects` | Parquet | job `streaming-sensors` (`src/gtdp/streaming/stream.py`), compactées par `streaming-housekeeping` | [flux capteurs](gold-sensors.md) |
| `streaming/archive/sensors/arrival_date=<d>/` | JSON Lines, fichiers d'entrée inchangés | job `streaming-housekeeping` (`src/gtdp/streaming/housekeeping.py`) | [flux capteurs](gold-sensors.md#fichiers-dentrée-archivés) |
| `ops/audit` | Parquet | chaque job lancé par `run.py` (`src/gtdp/audit.py`) | [ops](ops.md#opsaudit) |
| `ops/smoke` | Parquet | job `smoke` (`src/gtdp/jobs/smoke.py`) | [ops](ops.md#opssmoke) |
| `ops/pseudonymization/salt_fingerprint.json` | JSON | job `silver` (`src/gtdp/silver/pseudonymize.py`) | [ops](ops.md#opspseudonymizationsalt_fingerprintjson) |
| `ops/mr_vs_spark/spark/revenue`, `ops/mr_vs_spark/spark/counters` | texte, tabulations | job `revenue-by-country-spark` (`src/gtdp/jobs/revenue_by_country.py`) | [MapReduce contre Spark](mapreduce.md) |
| `ops/mr_vs_spark/mapreduce` (sortie Hadoop Streaming, `part-00000`) | texte, tabulations | `scripts/mapreduce/run_mr.sh`, lancé par la campagne (`src/gtdp/perf/mr_compare.py`) ou à la main ([runbook](../runbook/mapreduce.md)) | [MapReduce contre Spark](mapreduce.md) |

Le test de cohérence couvre les tables Parquet et les fichiers JSON. Les fichiers archivés gardent
le format du contrat. Les sorties texte du Lot 6 n'ont ni en-tête ni schéma : `tests/mapreduce`
vérifie que Spark et MapReduce y écrivent les mêmes lignes.

| Hors du dictionnaire | Écrit par | Raison et référence |
| --- | --- | --- |
| `landing/<source>/`, `landing/_manifests/…/_MANIFEST.json`, `landing/sensors/incoming/` | générateur (`src/gtdp/datagen/`), `scripts/cluster/land.sh` | entrées de la plateforme, fixées par le [contrat de données](../data-contract.md) |
| verrous `_<table>.lock` et `_gold.lock` | `src/gtdp/io.py` | fichier de coordination entre exécutions, absent au repos : [ADR-0002](../adr/0002-parquet-idempotence.md) |
| répertoires de transit `_<table>.staging-<id>`, `_<table>.tmp`, `_<table>.old`, marqueur `_SUCCESS` d'une partition vide | `src/gtdp/io.py` | états intermédiaires d'une écriture : [ADR-0002](../adr/0002-parquet-idempotence.md) |
| `_<table>.compaction-<jour>=<d>/` et son `_journal.json` | `src/gtdp/streaming/housekeeping.py` | transit d'une compaction, absent au repos : [runbook streaming](../runbook/streaming.md#housekeeping) |
| checkpoints du flux (`offsets/`, `commits/`, `sources/`, `state/`) | Spark Structured Streaming | format interne de Spark : [ADR-0008](../adr/0008-streaming.md) |
| `streaming/control/stop-streaming-sensors` | `scripts/streaming/stop.sh` | fichier vide, sa présence arrête le flux : [runbook streaming](../runbook/streaming.md#arrêter) |
| `/spark-logs`, `/app-logs`, `/mr-history` | Spark, YARN, MapReduce JobHistory | journaux des services : [guide de la plateforme](../platform.md#arborescence-hdfs-droits-et-quotas) |
| `/user/etl/smoke/streaming`, `/user/etl/smoke/spark` | `scripts/cluster/smoke/run.sh`, `scripts/cluster/smoke/spark_smoke.py` | sorties du test de fumée du cluster, hors du lake, réécrites à chaque test : [guide de la plateforme](../platform.md#démarrage-en-moins-de-10-minutes) |
| `/apps/spark/spark-jars-3.5.3.zip` | `scripts/cluster/init-hdfs.sh` | archive des jars Spark que YARN distribue (`spark.yarn.archive`), pas une donnée : [guide de la plateforme](../platform.md#notes-de-conception) |
| `report.md`, `runs.json` et journaux de la campagne du Lot 6 | `src/gtdp/perf/mr_compare.py` | fichiers locaux de mesure, hors du lake : [MapReduce contre Spark](../mapreduce-vs-spark.md#campagne-de-mesure) |
| `/benchmarks` | outils de mesure des Lots 6 et 7 et de l'[ADR-0006](../adr/0006-topologie-hdfs.md) | jeux d'entrée copiés ou convertis pour les mesures, recréés par ces outils : [MapReduce contre Spark](../mapreduce-vs-spark.md#campagne-de-mesure) |
