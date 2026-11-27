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

## Conventions

- **Types.** Les tables Parquet donnent les types Spark SQL : `string`, `int`, `bigint`,
  `decimal(p,s)`, `date`, `timestamp`, `boolean`, `array<string>`. Les montants sont des
  décimaux, jamais des flottants : `decimal(10,2)` pour un prix, `decimal(20,2)` pour un montant
  de ligne gold, `decimal(30,2)` pour une somme d'indicateur ; le dictionnaire gold donne le type
  de chaque moyenne. Les horodatages sont en UTC, fuseau de la session Spark. Les sorties texte du
  Lot 6 donnent le type de la valeur écrite.
- **Schémas.** Chaque table a un schéma explicite (`StructType`) dans le module de sa couche :
  `src/gtdp/schemas.py` (bronze, silver), `src/gtdp/gold/tables.py` (gold),
  `src/gtdp/streaming/sensors.py` (flux capteurs). Les jobs la lisent avec ce schéma par
  `read_table(spark, chemin, schéma)` ([ADR-0002](../adr/0002-parquet-idempotence.md)).
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
