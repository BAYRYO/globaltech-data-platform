# Dictionnaire de données : flux capteurs

Le job `streaming-sensors` lit les fichiers JSON Lines de `/datalake/landing/sensors/incoming`
([contrat](../data-contract.md#capteurs-sensors)) et écrit trois tables Parquet (snappy) :

| Table | Chemin | Grain | Clé |
| --- | --- | --- | --- |
| Agrégats | `/datalake/gold/sensor_aggregates/window_date=<d>/batch_id=<n>/` | une fenêtre fixe de 1 minute × un capteur | `window_start`, `sensor_id` |
| Alertes | `/datalake/gold/sensor_alerts/window_date=<d>/batch_id=<n>/` | un capteur × une fenêtre de 1 minute ayant au moins une valeur hors seuil | `alert_id` |
| Rejets | `/datalake/streaming/rejects/arrival_date=<d>/batch_id=<n>/` | une ligne illisible, incomplète ou hors contrat | aucune (une ligne par ligne rejetée) |

Un capteur (`sensor_id`) est une métrique d'une machine : le grain « fenêtre × capteur » est le
grain « fenêtre × machine × métrique » du [modèle de données](../data-model.md). Chaque table est
partitionnée par jour, puis par `batch_id`, le micro-batch qui a écrit ses lignes : rejouer un
micro-batch remplace ses partitions ([ADR-0008](../adr/0008-streaming.md)). Une partition tient
dans un fichier. Un micro-batch sans ligne pour une table n'y crée pas de partition. Lire un jour
(`window_date = '2026-01-01'`) ne lit que ses partitions. L'exploitation est décrite dans le
[runbook du streaming](../runbook/streaming.md).

Le job `streaming-housekeeping` fusionne les partitions `batch_id` anciennes d'un jour en une
seule, celle du dernier micro-batch fusionné : après lui, `batch_id` vaut le plus grand micro-batch
dont la partition contient les lignes, et les lignes, les clés et les valeurs restent les mêmes.

Les schémas font foi dans `src/gtdp/streaming/sensors.py` (`AGGREGATES_SCHEMA`,
`ALERTS_SCHEMA`, `REJECTS_SCHEMA`) : chaque micro-batch est écrit dans ces colonnes, cet ordre et
ces types. Toutes les colonnes sont nullables au sens Parquet. Les horodatages et les dates sont
en UTC.

## Événements retenus

Une ligne entre dans les agrégats et les alertes si elle est exactement un objet JSON strict, avec
ses identifiants (`event_id`, `sensor_id`, `site`, `machine_id`, `metric`), son `event_time`, son
`emitted_at` et une valeur numérique finie, datée au plus `future_tolerance_s` (60 s) après son
émission (sinon elle est rejetée, voir plus bas), puis si elle passe deux filtres :

| Filtre | Règle | Compteur |
| --- | --- | --- |
| Watermark | `event_time` antérieur au watermark (plus grand `event_time` vu, moins 2 minutes) : l'événement est écarté | `numRowsDroppedByWatermark`, métrique `rows_dropped_by_watermark` de l'audit |
| Doublon | même `event_id` qu'un événement déjà vu dont l'état n'a pas expiré (`event_time` + 2 minutes > watermark) : écarté | `numDroppedDuplicateRows`, compteur `rows_duplicated` de l'audit |

Le filtre de retard d'un micro-batch utilise le watermark calculé deux micro-batches plus tôt :
les deux premiers micro-batches d'un checkpoint neuf n'écartent rien, ce que le contrat respecte
en ne plaçant aucun événement très en retard dans les 36 premiers fichiers.

La déduplication tient tant que l'état de l'`event_id` existe, soit 2 minutes de watermark après
son `event_time`. Un doublon réémis avec un `event_time` décalé de plus de 2 minutes arrive après
l'expiration de cet état : il est compté une seconde fois, dans la fenêtre de son nouvel
`event_time`. Le contrat place ses doublons dans le fichier suivant, moins de 5 s plus tard.

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
| `window_date` | date | Partition : date UTC de `window_start` | `2026-01-01` |
| `batch_id` | int | Partition : micro-batch qui a écrit la fenêtre, ou dernier micro-batch fusionné par la compaction | `45` |

`avg_value` est une somme divisée par un nombre d'événements : l'ordre des additions dépend de la
façon dont les fichiers sont découpés en micro-batches et en partitions. Deux exécutions sur les
mêmes fichiers donnent la même moyenne à 1e-16 près en valeur relative, pas toujours au bit près.
Comparer deux exécutions sur `avg_value` se fait avec une tolérance de 1e-9 (c'est celle de
`scripts/streaming/report.py`) ; `event_count`, `min_value` et `max_value` sont exacts.

Un événement en retard ordinaire (jusqu'à 90 s) compte dans la fenêtre de son `event_time`, même
si elle est antérieure à minuit : sur la journée `dev`, 23 lignes portent sur les fenêtres
`2025-12-31 23:58` et `23:59` (partition `window_date=2025-12-31`), à côté des 12 fenêtres ×
60 capteurs de 00:00 à 00:11 (743 lignes).

## `gold/sensor_alerts`

Une alerte est émise dans le micro-batch où arrive le premier événement hors seuil d'un capteur
dans une fenêtre de 1 minute, sans attendre la fin de la fenêtre. Elle porte, parmi les événements
hors seuil de la fenêtre arrivés dans **ce** micro-batch, celui de plus petit `event_time` (puis
de plus petit `event_id`). Les événements hors seuil des micro-batches suivants ne donnent pas
d'autre alerte et ne la remplacent pas, même plus anciens : le micro-batch `n` écarte les
`alert_id` déjà écrits par les micro-batches inférieurs à `n`.

Les champs de l'événement d'une alerte (`value`, `event_time`, `event_id`) suivent donc l'ordre
d'arrivée : ils dépendent des fichiers et de leur découpage en micro-batches
(`max_files_per_trigger`, instant de déclenchement). Les mêmes fichiers dans les mêmes
micro-batches donnent la même alerte, et un micro-batch rejoué après un arrêt réécrit la même
alerte. Exemple (test `test_the_alert_carries_the_earliest_breach_of_the_micro_batch_where_its_window_breaches`) :
une valeur hors seuil à 00:01:40 arrive dans un fichier, une autre de la même fenêtre à 00:01:10
(35 s de retard, retard ordinaire) dans le fichier suivant. Lus un fichier par micro-batch,
l'alerte porte 00:01:40 ; lus ensemble, 00:01:10. Le nombre d'alertes, leurs `alert_id`, leurs
fenêtres et leurs seuils ne dépendent pas du découpage. Sur les données du contrat, un pic dure au
moins 2 minutes sans événement en retard sur le capteur en pic : les alertes y sont identiques
quel que soit le découpage.

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
| `window_date` | date | Partition : date UTC de `window_start` | `2026-01-01` |
| `batch_id` | int | Partition : micro-batch qui a émis l'alerte, ou dernier micro-batch fusionné | `22` |

### Seuils

Les seuils viennent de `conf/app.toml`, section `[streaming.thresholds]`, une ligne par métrique.
Ce sont ceux du contrat. Une valeur égale au seuil n'alerte pas. Une surcharge (fichier
d'environnement, `GTDP_STREAMING__THRESHOLDS__TEMPERATURE__ABOVE`,
`--set streaming.thresholds.temperature.above=90`) remplace les seuils qu'elle nomme et garde les
autres.

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
| `arrival_date` | date | Partition : date UTC d'arrivée du fichier (sa date de modification), la même à chaque rejeu | `2026-11-26` |
| `batch_id` | int | Partition : micro-batch qui a lu la ligne, ou dernier micro-batch fusionné | `1` |

| Motif | Règle (`REJECT_RULES`) | Anomalie du contrat |
| --- | --- | --- |
| `MALFORMED_JSON` | la ligne n'est pas exactement un objet JSON strict : JSON illisible ou vide, guillemets simples, `NaN` ou `Infinity` (nus ou entre guillemets), autre chose après l'objet (texte, second objet), tableau, valeur numérique écrite en chaîne, horodatage sans décalage UTC | `MALFORMED_JSON` |
| `MISSING_FIELD` | `event_id`, `sensor_id`, `site`, `machine_id`, `metric`, `event_time` ou `emitted_at` absent ou nul | aucune (règle défensive) |
| `NULL_VALUE` | `value` nulle | `NULL_VALUE` |
| `BAD_VALUE` | `value` non finie après lecture, par exemple `1e400` | aucune (règle défensive) |
| `FUTURE_EVENT` | `event_time` postérieur à `emitted_at` de plus de `future_tolerance_s` secondes (60) : un tel événement avancerait le watermark et ferait écarter la suite du flux comme en retard | aucune (règle défensive) |

Le lecteur JSON de Spark s'arrête à la fin du premier objet d'une ligne et ignore la suite : la
ligne est donc lue deux fois, comme valeur d'un objet (`{"event": <ligne>}`) et comme tableau
(`[<ligne>]`). Ce qui suit l'objet casse l'une des deux lectures. Le schéma reste celui de Spark
pour le reste : un nombre accepté là où le contrat attend une chaîne (`event_id`) ou un nombre de
secondes Unix pour `event_time` sont lus sans rejet. `FUTURE_EVENT` compare deux horodatages de
l'émetteur : un émetteur dont toute l'horloge avance n'est pas détecté.

## Fichiers d'entrée archivés

`streaming-housekeeping` déplace chaque fichier de `incoming` que les trois requêtes ont validé
vers `/datalake/streaming/archive/sensors/arrival_date=<d>/<nom du fichier>`, `<d>` étant sa date
UTC de modification (son arrivée). Le fichier est inchangé. Les jours plus anciens que
`archive_retention_days` (2) avant le jour courant sont supprimés. Les lignes rejetées restent
dans `streaming/rejects` ; les autres sont dans les agrégats et les alertes.
