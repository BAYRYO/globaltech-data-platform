# Dictionnaire de données : flux capteurs

Le job `streaming-sensors` lit les fichiers JSON Lines de `/datalake/landing/sensors/incoming`
([contrat](../data-contract.md#capteurs-sensors)) et écrit trois tables Parquet (snappy) :

| Table | Chemin | Grain | Clé |
| --- | --- | --- | --- |
| Agrégats | `/datalake/gold/sensor_aggregates/batch_id=<n>/` | une fenêtre fixe de 1 minute × un capteur | `window_start`, `sensor_id` |
| Alertes | `/datalake/gold/sensor_alerts/batch_id=<n>/` | un capteur × une fenêtre de 1 minute ayant au moins une valeur hors seuil | `alert_id` |
| Rejets | `/datalake/streaming/rejects/batch_id=<n>/` | une ligne illisible ou incomplète | aucune (une ligne par ligne rejetée) |

Un capteur (`sensor_id`) est une métrique d'une machine : le grain « fenêtre × capteur » est le
grain « fenêtre × machine × métrique » du [modèle de données](../data-model.md). Chaque table est
partitionnée par `batch_id`, le micro-batch qui a écrit ses lignes : rejouer un micro-batch
remplace sa partition ([ADR-0008](../adr/0008-streaming.md)). Un micro-batch sans ligne pour une
table n'y crée pas de partition. L'exploitation est décrite dans le
[runbook du streaming](../runbook/streaming.md).

Les schémas font foi dans `src/gtdp/streaming/sensors.py` (`AGGREGATES_SCHEMA`,
`ALERTS_SCHEMA`, `REJECTS_SCHEMA`) : chaque micro-batch est écrit dans ces colonnes, cet ordre et
ces types. Toutes les colonnes sont nullables au sens Parquet. Les horodatages sont en UTC.

## Événements retenus

Une ligne entre dans les agrégats et les alertes si elle est un objet JSON lisible, avec ses
identifiants (`event_id`, `sensor_id`, `site`, `machine_id`, `metric`), son `event_time` et une
valeur (sinon elle est rejetée, voir plus bas), puis si elle passe deux filtres :

| Filtre | Règle | Compteur |
| --- | --- | --- |
| Watermark | `event_time` antérieur au watermark (plus grand `event_time` vu, moins 2 minutes) : l'événement est écarté | `numRowsDroppedByWatermark`, métrique `rows_dropped_by_watermark` de l'audit |
| Doublon | même `event_id` qu'un événement déjà vu dont l'état n'a pas expiré (`event_time` + 2 minutes > watermark) : écarté | `numDroppedDuplicateRows`, compteur `rows_duplicated` de l'audit |

Le filtre de retard d'un micro-batch utilise le watermark calculé deux micro-batches plus tôt :
les deux premiers micro-batches d'un checkpoint neuf n'écartent rien, ce que le contrat respecte
en ne plaçant aucun événement très en retard dans les 36 premiers fichiers.

## `gold/sensor_aggregates`

Une fenêtre est écrite une seule fois, dans le micro-batch où le watermark dépasse sa fin (mode
`append`). Les fenêtres des 3 dernières minutes d'un flux fini restent ouvertes tant qu'aucun
événement plus récent n'arrive.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `window_start` | timestamp | Début de la fenêtre, inclus, aligné sur la minute | `2026-01-01 00:03:00` |
| `window_end` | timestamp | Fin de la fenêtre, exclue : `window_start` + 1 minute | `2026-01-01 00:04:00` |
| `site` | string | Site de l'événement | `DE-HAMBURG` |
| `machine_id` | string | Machine de l'événement | `DE-HAMBURG-M04` |
| `sensor_id` | string | Capteur : machine et métrique | `DE-HAMBURG-M04-TEMP` |
| `metric` | string | `temperature`, `vibration` ou `pressure` | `temperature` |
| `event_count` | bigint | Nombre d'événements retenus de la fenêtre | `12` |
| `avg_value` | double | Moyenne de `value` | `95.15833333333335` |
| `min_value` | double | Minimum de `value` | `86.1` |
| `max_value` | double | Maximum de `value` | `105.2` |
| `batch_id` | int | Partition : micro-batch qui a écrit la fenêtre | `45` |

Un événement en retard ordinaire (jusqu'à 90 s) compte dans la fenêtre de son `event_time`, même
si elle est antérieure à minuit : sur la journée `dev`, 23 lignes portent sur les fenêtres
`2025-12-31 23:58` et `23:59`, à côté des 12 fenêtres × 60 capteurs de 00:00 à 00:11 (743 lignes).

## `gold/sensor_alerts`

Une alerte est émise dans le micro-batch où arrive le premier événement hors seuil d'un capteur
dans une fenêtre de 1 minute, sans attendre la fin de la fenêtre. Quand plusieurs événements hors
seuil de la fenêtre arrivent dans ce micro-batch, l'alerte porte celui de plus petit `event_time`
(puis de plus petit `event_id`). Les événements hors seuil des micro-batches suivants ne donnent
pas d'autre alerte : le micro-batch `n` écarte les `alert_id` déjà écrits par les micro-batches
inférieurs à `n`, ce qui donne le même résultat quand il est rejoué.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `alert_id` | string | SHA-256 hexadécimal de `sensor_id`, `\|` et `window_start` en secondes Unix : le même à chaque rejeu | `f93a3a7e6363320bd73b65a8bc6a5b8dd73ba29d9122f525f94181d952ecc950` |
| `sensor_id` | string | Capteur en alerte | `DE-HAMBURG-M04-TEMP` |
| `machine_id` | string | Machine du capteur | `DE-HAMBURG-M04` |
| `site` | string | Site du capteur | `DE-HAMBURG` |
| `metric` | string | Métrique du capteur | `temperature` |
| `window_start` | timestamp | Début de la fenêtre de 1 minute de l'événement | `2026-01-01 00:03:00` |
| `window_end` | timestamp | Fin exclue de la fenêtre | `2026-01-01 00:04:00` |
| `value` | double | Valeur de l'événement qui déclenche l'alerte | `92.3` |
| `threshold` | double | Seuil franchi, lu dans `[streaming.thresholds]` | `85.0` |
| `direction` | string | `above` : `value > threshold` ; `below` : `value < threshold` | `above` |
| `event_time` | timestamp | Horodatage de l'événement | `2026-01-01 00:03:00.112` |
| `event_id` | string | Identifiant de l'événement | `S20260101-002272` |
| `batch_id` | int | Partition : micro-batch qui a émis l'alerte | `22` |

### Seuils

Les seuils viennent de `conf/app.toml`, section `[streaming.thresholds]`, une ligne par métrique.
Ce sont ceux du contrat. Une valeur égale au seuil n'alerte pas.

| Métrique | `below` | `above` |
| --- | --- | --- |
| `temperature` | | 85.0 |
| `vibration` | | 7.1 |
| `pressure` | 4.0 | 8.0 |

## `streaming/rejects`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `reject_reason` | string | Premier motif vérifié, dans l'ordre du tableau ci-dessous | `MALFORMED_JSON` |
| `raw_line` | string | Ligne lue, telle quelle (ici sans accolade fermante) | `{"event_id":"S20260101-000086",…,"emitted_at":"2026-01-01T00:00:09.081Z"` |
| `source_file` | string | URI du fichier lu | `hdfs://namenode:8020/datalake/landing/sensors/incoming/sensors_20260101T000005.jsonl` |
| `batch_id` | int | Partition : micro-batch qui a lu la ligne | `1` |

| Motif | Règle (`REJECT_RULES`) | Anomalie du contrat |
| --- | --- | --- |
| `MALFORMED_JSON` | la ligne n'est pas un objet JSON lisible, ou elle est vide | `MALFORMED_JSON` |
| `MISSING_FIELD` | `event_id`, `sensor_id`, `site`, `machine_id`, `metric` ou `event_time` absent ou nul | aucune (règle défensive) |
| `NULL_VALUE` | `value` nulle | `NULL_VALUE` |
