# Runbook : streaming des capteurs

Le job `streaming-sensors` (couche `streaming`) lit en continu les fichiers capteurs déposés dans
`/datalake/landing/sensors/incoming` et écrit les agrégats par minute et les alertes en gold, et
les lignes rejetées en zone streaming. Le job `streaming-housekeeping`, lancé toutes les heures,
le garde dans ses quotas HDFS. Les tables sont décrites dans le
[dictionnaire de données](../data-dictionary/gold-sensors.md), la sémantique dans
[ADR-0008](../adr/0008-streaming.md). Les commandes se lancent depuis la racine du dépôt, sous
Git Bash ou Linux, cluster démarré ([guide de la plateforme](../platform.md)).

## Fonctionnement

Le job démarre trois requêtes Structured Streaming sur la même source, chacune avec son
checkpoint :

| Requête | Sortie | Checkpoint | État |
| --- | --- | --- | --- |
| `sensor_aggregates` | `/datalake/gold/sensor_aggregates` | `<checkpoints>/aggregates` | doublons (`event_id`), fenêtres ouvertes |
| `sensor_alerts` | `/datalake/gold/sensor_alerts` | `<checkpoints>/alerts` | doublons (`event_id`) ; les alertes déjà émises sont lues dans la table |
| `sensor_rejects` | `/datalake/streaming/rejects` | `<checkpoints>/rejects` | aucun |

`<checkpoints>` vaut `paths.checkpoints`, soit `/datalake/streaming/sensors/checkpoints` sur le
cluster. Toutes les `trigger_interval_s` secondes (30), chaque requête liste le répertoire
`incoming` et lit au plus `max_files_per_trigger` (12) nouveaux fichiers, les plus anciens
d'abord. Seuls les noms du contrat, `sensors_<yyyyMMdd>T<HHmmss>.jsonl`, sont lus (filtre
`pathGlobFilter`) : un fichier temporaire ou mal nommé reste ignoré dans `incoming`. Des fichiers
de même date de modification (à la milliseconde près) sont pris dans l'ordre de la liste du
répertoire : par nom sur HDFS, donc chronologique, et sans ordre garanti sur un système de
fichiers local. Un fichier
doit apparaître d'un bloc dans `incoming` (renommage atomique), sinon Spark peut en lire une
partie.

Chaque micro-batch écrit ses lignes dans les partitions `<jour>=<d>/batch_id=<n>` de chaque table,
un fichier par partition : `window_date` (date de la fenêtre) pour les agrégats et les alertes,
`arrival_date` (date d'arrivée du fichier) pour les rejets.

### Sémantique

| Choix | Valeur | Justification |
| --- | --- | --- |
| Watermark | 2 minutes sur `event_time` | Le contrat place les retards ordinaires entre 10 et 90 s (gardés) et les très grands retards au-delà de 5 min (écartés) : 2 minutes garde les premiers avec 30 s de marge et écarte les seconds. |
| Événements datés après leur émission | rejet `FUTURE_EVENT` au-delà de `future_tolerance_s` (60 s) | Un seul événement daté de plusieurs heures en avance avancerait le watermark d'autant et ferait écarter tout le reste du flux comme en retard. |
| Déduplication | `dropDuplicatesWithinWatermark("event_id")` | L'état d'un identifiant expire avec le watermark, donc reste borné. Un doublon réémis avec un autre `event_time` est aussi écarté, tant que l'état existe (2 minutes de watermark). |
| Fenêtres | fixes, 1 minute, par capteur | Grain du modèle de données (machine × métrique). |
| Mode de sortie | `append` | Chaque fenêtre est écrite une fois, définitive, quand le watermark dépasse sa fin. Le mode `update` réécrirait chaque fenêtre à chaque micro-batch, ce qui casse l'écriture idempotente par `batch_id` (mesuré dans ADR-0008). |
| Alertes | émises dans le micro-batch où arrive la première valeur hors seuil de la fenêtre, portant la plus ancienne valeur hors seuil de ce micro-batch | Latence d'un déclenchement au lieu de fenêtre + watermark (3 minutes). Les champs de l'événement suivent l'ordre d'arrivée et le découpage en micro-batches, identiques au rejeu (voir le [dictionnaire](../data-dictionary/gold-sensors.md#goldsensor_alerts)). |
| Écriture | `foreachBatch`, partitions `<jour>=<d>/batch_id=<n>` remplacées | Spark rejoue un micro-batch non validé avec le même numéro et les mêmes fichiers : la réécriture remplace la sortie partielle. |

Conséquence de `append` sur un flux fini : les fenêtres des 3 dernières minutes (watermark +
fenêtre) ne sont écrites que si des événements plus récents arrivent. Sur les 15 minutes de la
journée `dev`, les fenêtres de 00:00 à 00:11 sont écrites, celles de 00:12 à 00:14 restent
ouvertes dans l'état.

## Configuration

Section `[streaming]` de `conf/app.toml`, surchargeable comme toute clé
(`GTDP_STREAMING__<CLÉ>` ou `--set streaming.<clé>=<valeur>`) :

| Clé | Défaut | Effet |
| --- | --- | --- |
| `trigger_interval_s` | `30` | Secondes entre deux micro-batches : 6 fichiers de 5 s par micro-batch. `0` enchaîne les micro-batches. Chaque micro-batch coûte des noms HDFS et des entrées d'event log, voir [Quotas](#quotas--projection-sur-24-h). |
| `max_files_per_trigger` | `12` | Nouveaux fichiers lus au plus par micro-batch (une minute de flux). Au-delà de 18, un événement en retard de 5 minutes peut passer le watermark après une reprise. |
| `state_partitions` | `4` | Partitions de l'état. Figé par le checkpoint à sa création. |
| `stop_after_idle_triggers` | `0` | Arrêt après ce nombre de déclenchements sans données ni micro-batch à exécuter. `0` : jamais. |
| `future_tolerance_s` | `60` | Retard maximal de `emitted_at` sur `event_time` ; au-delà, rejet `FUTURE_EVENT`. |
| `stale_after_s` | `300` | Âge à partir duquel un dossier de staging d'une table du flux, ou un verrou dont YARN ne donne pas l'état, est celui d'une exécution morte (`spark.gtdp.io.staleAfterSeconds` des deux jobs). Une écriture attend un verrou tenu ce délai plus 60 s, puis échoue. Voir [Reprise](#reprise-après-arrêt-brutal). |
| `archive_retention_days` | `2` | Jours d'arrivée gardés dans l'archive en plus du jour courant. |
| `thresholds` | voir le dictionnaire | Seuils d'alerte par métrique (`above`, `below`). Une surcharge d'un seuil (`GTDP_STREAMING__THRESHOLDS__PRESSURE__BELOW=3.5`) garde les autres. |

## Démarrer le job

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh streaming-sensors
```

Le job tourne en mode client, avec un exécuteur pour laisser la place à un batch à côté
([budget mémoire](../platform.md#budget-mémoire)). Les jobs du flux et `report.py` ne
pseudonymisent rien : ils n'ont pas besoin de `GTDP_PSEUDO_SALT`. Pour qu'il s'arrête seul une
fois le flux épuisé :

```bash
scripts/cluster/submit.sh streaming-sensors --set streaming.stop_after_idle_triggers=4
```

Au démarrage, le job crée `incoming` s'il manque, supprime un marqueur d'arrêt resté d'une
demande précédente, puis journalise pour chaque requête le dernier micro-batch planifié et le
dernier validé de son checkpoint :

```json
{"message": "query starts", "query": "aggregates", "last_planned_batch": 30, "last_committed_batch": 29}
```

Un micro-batch planifié et non validé (ici 30) est celui que Spark rejoue.

## Alimenter le flux

Générer la journée `dev` (180 fichiers, 15 minutes de flux) si elle manque, puis la rejouer :

```bash
scripts/cluster/client.sh python3 -m gtdp.datagen generate --profile dev --seed 42 --out data/generated
scripts/cluster/client.sh python3 -m gtdp.datagen replay-sensors \
  --src data/generated/dev/sensors/2026-01-01 \
  --dst hdfs://namenode:8020/datalake/landing/sensors/incoming --interval 5
```

Le simulateur dépose chaque fichier dans `incoming_staging`, puis le renomme dans `incoming`. Il
saute les fichiers déjà présents : relancé, il reprend où il s'était arrêté. Chaque dépôt HDFS
lance deux commandes `hdfs` (environ 2,4 s mesurées) : en dessous de 3 s d'intervalle, le rythme
réel est celui des commandes. À la fin, ou sur Ctrl+C ou SIGTERM, il affiche ce qu'il a publié
et sort avec 0, 130 ou 143, sans trace d'erreur :

```
published 180 files, 11772 events to hdfs://namenode:8020/datalake/landing/sensors/incoming (902.1 s)
published 4 files, 258 events to hdfs://namenode:8020/user/etl/streaming-demo/replay-stop/incoming, stopped by SIGTERM (14.2 s)
```

## Suivre

| Quoi | Où |
| --- | --- |
| Micro-batches écrits | lignes JSON `"batch written"` (requête, `batch_id`, lignes, table) sur la sortie de `submit.sh` |
| Attente d'un verrou | lignes `"table locked, waiting for its lock"` puis `"table lock obtained"` (`waited_s`) |
| Progression, état, watermark | onglet *Structured Streaming* de l'interface Spark, par le lien *ApplicationMaster* de http://localhost:8088 |
| Application terminée | Spark History Server, http://localhost:18080 |
| Checkpoint | `scripts/cluster/client.sh hdfs dfs -ls /datalake/streaming/sensors/checkpoints/aggregates/commits` |
| Audit | une ligne par exécution dans `/datalake/ops/audit`, voir [Vérifier](#vérifier) |

## Arrêter

| Moyen | Commande | Ligne d'audit |
| --- | --- | --- |
| Marqueur d'arrêt (recommandé) | `scripts/streaming/stop.sh` | `SUCCESS`, `stop_reason` = `stop marker` |
| Inactivité | `--set streaming.stop_after_idle_triggers=N` | `SUCCESS`, `stop_reason` = `idle` |
| SIGTERM au seul processus Python du driver | `kill -TERM <pid du python>` dans le conteneur client | `STOPPED`, `stop_reason` = `signal SIGTERM`, compteurs remplis, code de sortie 143 |
| Arrêt brutal (conteneur tué, `kill -9`) | `docker kill <conteneur client>` | aucune, voir [Détecter un arrêt brutal](#détecter-un-arrêt-brutal) |

`stop.sh` crée `/datalake/streaming/control/stop-streaming-sensors`. Le job le voit en moins
d'un intervalle de déclenchement (30 s), attend la fin du micro-batch en cours (60 s au plus),
arrête ses requêtes, remplit ses compteurs, écrit sa ligne d'audit `SUCCESS` et supprime le
marqueur. Ctrl+C sur `submit.sh` envoie le signal à tout le groupe de processus : la JVM du driver
meurt avec le processus Python et la ligne d'audit est perdue, comme après un arrêt brutal. Les
données restent cohérentes dans tous les cas : le redémarrage reprend au dernier micro-batch
validé.

## Vérifier

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh scripts/streaming/report.py /datalake
```

Le rapport donne pour chaque table ses lignes, ses clés et ses clés en double, puis les alertes,
les lignes d'audit de `streaming-sensors` de `<racine>/ops/audit` et les applications sans ligne
d'audit. Attendu sur la journée `dev` rejouée en entier :

| Mesure | Attendu | Origine |
| --- | --- | --- |
| Lignes lues (`rows_in`) | 11 772 | lignes des 180 fichiers |
| Rejets | 108 : `MALFORMED_JSON` 54, `NULL_VALUE` 54 | manifeste |
| Doublons écartés (`rows_duplicated`) | 324 | `DUPLICATE_EVENT` du manifeste |
| Écartés par le watermark | 108 | `VERY_LATE_EVENT` du manifeste |
| Agrégats | 743 lignes, 9 010 événements : 12 fenêtres × 60 capteurs (00:00 à 00:11) et 23 lignes avant minuit | fenêtres finales du flux |
| Alertes | 10 : 5 capteurs en pic × 2 fenêtres (00:03 et 00:04) | 120 événements `SPIKE` |
| Clés en double | 0 dans chaque table | |

### Détecter un arrêt brutal

Un driver tué n'écrit pas de ligne d'audit, et YARN affiche une application en mode client dont
le driver a disparu comme `FINISHED`/`SUCCEEDED`. Le rapport liste donc, pour la racine donnée,
les applications `gtdp-streaming-sensors` connues du ResourceManager qui n'ont pas de ligne
d'audit dans `<racine>/ops/audit` : un arrêt brutal, ou une exécution encore en cours (état YARN
`RUNNING`). Il les attribue à la racine par la ligne de commande enregistrée dans leur event log
(`--set paths.landing=…`, sinon le landing configuré) et en donne les compteurs tirés de leurs
rapports de progression, tant que le log se lit (jusqu'à la coupure, ou depuis la dernière
compaction du History Server). Exemple sur une racine de test dont trois drivers ont été tués :

```
== gtdp-streaming-sensors applications of /user/etl/streaming-review/kill-a/landing without audit row (crashed or running)
  application_1795715756506_0207 YARN FINISHED/SUCCEEDED started 22:08:59  batches 5 in 1812 late 0 duplicated 47
  application_1795715756506_0213 YARN FINISHED/SUCCEEDED started 22:12:21  batches 3 in 1565 late 8 duplicated 40
  application_1795715756506_0216 YARN FINISHED/SUCCEEDED started 22:14:23  batches 14 in 5809 late 64 duplicated 146
```

Les lignes lues par une exécution tuée ne sont pas perdues : la suivante rejoue le micro-batch
non validé et lit les fichiers restants ; sa ligne d'audit ne compte que ce qu'elle a lu.

## Exploitation 24/7

### Housekeeping

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh streaming-housekeeping
```

À lancer toutes les heures (planificateur externe, par exemple cron), à côté du flux en marche.
Le job écrit une ligne d'audit (`archived_files`, `purged_days`, `compacted` par table ;
`rows_in` = `rows_out` = lignes fusionnées). Il fait trois choses :

1. **Archive** : il déplace de `incoming` vers
   `/datalake/streaming/archive/sensors/arrival_date=<d>/` les fichiers que les trois requêtes
   ont validés (journal de la source fichier de chaque checkpoint, `sources/0`, jusqu'au dernier
   micro-batch validé). Un fichier qu'une requête peut encore rejouer reste dans `incoming`.
   `cleanSource` de Spark n'est pas utilisé : avec trois requêtes sur le même répertoire, l'une
   déplacerait un fichier avant que les autres ne l'aient lu.
2. **Purge** : il supprime les jours d'arrivée plus anciens que `archive_retention_days` (2) avant
   le jour courant.
3. **Compaction** : pour chaque table et chaque jour, il fusionne en une seule partition (celle du
   dernier micro-batch fusionné) les partitions `batch_id` plus anciennes que le plus ancien
   micro-batch que le checkpoint garde (`offsets/`, 100 derniers micro-batches,
   `spark.sql.streaming.minBatchesToRetain`) : Spark ne les rejoue jamais. Pour les agrégats et
   les alertes, seulement les jours que le watermark du dernier micro-batch validé a dépassés :
   aucun micro-batch ne les écrit ni ne les lit plus. Lignes, clés et valeurs restent les mêmes ;
   le nombre de lignes fusionnées est vérifié avant l'échange.

La compaction d'un jour se fait sous le verrou de la table : le flux attend entre deux
micro-batches (`table locked, waiting for its lock`). Elle écrit d'abord les lignes fusionnées
dans `_<table>.compaction-<jour>=<d>/merged`, les compte, écrit un journal (`_journal.json`, les
partitions remplacées), puis supprime ces partitions et déplace la partition fusionnée. Tuée à
n'importe quel moment, la suivante termine l'échange d'un dossier qui a son journal, ou supprime
un dossier qui n'en a pas encore, avant toute nouvelle compaction : relancer le job suffit. Entre
la mort d'un housekeeping au milieu d'un échange et sa relance, les lignes du jour en cours
d'échange manquent aux lecteurs ; le flux n'en a pas besoin.

### Quotas : projection sur 24 h

Rythme : un fichier de 15,6 Ko (65 lignes) toutes les 5 s, soit 17 280 fichiers et 269 Mo par
jour ; un micro-batch toutes les 30 s, soit 2 880 par jour ; 1 440 fenêtres fermées par jour.
Housekeeping toutes les heures. Les quotas d'espace comptent les octets répliqués (× 3). Mesures
sur la journée `dev` (180 fichiers) ; « avant » = commit `a9134bc` (micro-batch toutes les 5 s,
sans housekeeping).

| Répertoire (quota noms, espace) | Avant : par jour | Avant : plein en | Après : par jour | Après : régime établi |
| --- | --- | --- | --- | --- |
| `/datalake/landing` (100 000, 20 Go) | 17 280 noms, 808 Mo | 5,8 jours (noms) | 0 : `incoming` vidé chaque heure | ≤ 720 fichiers, 34 Mo |
| `/datalake/streaming/archive` | n/a | n/a | 17 280 noms, 808 Mo | 3 jours gardés : 51 840 noms, 2,4 Go |
| `/datalake/streaming/rejects` | 8 300 partitions, 17 700 noms | | 2 880 partitions, 5 760 noms avant compaction | ≈ 450 noms vivants + 3 par jour |
| `/datalake/streaming/sensors/checkpoints` | borné (≈ 2 400 noms) | | borné (≈ 2 300 noms) ; journaux des sources : + 7 Mo bruts, 21 Mo répliqués | non borné en octets, voir plus bas |
| Total `/datalake/streaming` (500 000, 5 Go) | 17 700 noms | 28 jours (noms) | 23 000 noms avant housekeeping | ≈ 55 000 noms ; 2,4 Go + 21 Mo par jour, plein en 120 jours (octets) |
| `/datalake/gold` sensor (200 000, 10 Go) | 7 060 noms (1 440 partitions × 4,9) | 28 jours (noms) | 2 880 noms avant compaction | ≈ 3 000 noms vivants (le jour courant) + 3 par jour et table ; 6 Mo par jour |
| `/spark-logs` (100 000, 15 Go) | 2,75 Go stockés, 8,2 Go répliqués (159 Ko par micro-batch), croissant avec `incoming` | 1,8 jour | 363 Mo stockés, 1,1 Go répliqués (126 Ko par micro-batch) | sans compaction : plein en 14 jours ; avec la compaction du History Server : borné à environ 55 Mo stockés, 165 Mo répliqués |
| `/datalake/ops` (100 000, 2 Go) | ≈ 0 | | 24 lignes d'audit du housekeeping, ≈ 73 noms | plein en 3,7 ans (noms) |

Limite connue : le journal de la source fichier de chaque checkpoint (`sources/0`) garde une
entrée par fichier lu, sans fin (environ 135 octets, mesuré), et Spark le réécrit en entier tous
les 10 micro-batches. Il croît de 7 Mo par jour pour les trois requêtes (21 Mo répliqués) ;
avec l'archive, `/datalake/streaming` est plein en environ 120 jours. Repartir d'un nouveau
checkpoint remet ce journal à zéro, avec de nouvelles tables (voir
[Repartir de zéro](#repartir-de-zéro-ou-changer-le-plan)).

### Event logs Spark

Chaque micro-batch écrit environ 126 Ko d'event log (zstd, mesuré), 870 Ko non compressés :
363 Mo par jour pour le job, et le nettoyage du History Server (7 jours) ignore une application
en cours. `docker/conf/spark/spark-defaults.conf` règle donc la compaction des event logs roulés :
`spark.history.fs.eventLog.rolling.maxFilesToRetain` = 2. Au-delà de 2 fichiers de 128 Mo
(non compressés, environ 18 Mo stockés), le History Server fusionne les plus anciens en un
fichier `.compact` sans les jobs, étapes, tâches et requêtes SQL terminés ni les rapports de
progression. Mesuré avec un History Server lancé sur un répertoire d'essai et des fichiers de
10 Mo : après 127 micro-batches, 10 fichiers (14,4 Mo stockés) sont devenus un `.compact` de
96 Ko et les 2 derniers fichiers (2,5 Mo). L'event log d'un job 24/7 reste donc sous environ
3 × 18 Mo + 0,1 Mo. L'onglet du History Server et le rapport de crash ne voient plus que les
micro-batches des fichiers gardés. Le History Server lit ce réglage à son démarrage : il
s'applique au prochain démarrage du cluster (l'image `gtdp/hadoop-spark:3.3.6-3.5.3` le porte
déjà).

## Reprise après arrêt brutal

Après un arrêt brutal, relancer simplement le job. Chaque requête relit son checkpoint : un
micro-batch planifié (`offsets/<n>`) sans validation (`commits/<n>`) est rejoué avec les mêmes
fichiers, et son écriture remplace les partitions `batch_id=<n>` déjà écrites en partie ou en
entier. Les fichiers arrivés pendant l'arrêt sont lus ensuite, par paquets de
`max_files_per_trigger`.

Un driver tué pendant une écriture laisse le verrou de la table (`_<table>.lock`) et son dossier
de staging (`_<table>.staging-<id>`). Le job relancé reprend ce verrou (`taking over the lock`)
dès que YARN rapporte terminée l'application tuée, moins de 2 s après la mort de son driver,
conteneur client tué compris. Si le ResourceManager ne répond pas après trois essais
(`no state from YARN`), il attend (`table locked, waiting for its lock`) que le verrou ait
`stale_after_s` secondes (300). L'écriture suivante qui a lieu après le même âge supprime le
staging laissé (`removing …, left … s ago by a run that died`), sans arrêter le job.

Le flux attend un verrou tenu `stale_after_s` plus 60 s au plus (360 s), quel que soit son
détenteur. Le verrou d'un housekeeping `RUNNING` n'est jamais repris : si son étape dure plus
longtemps, la requête échoue avec `TableLockedError` et le job s'arrête (code 1). Relancez-le une
fois l'étape finie : il reprend à son checkpoint. Gardez donc `stale_after_s` au-dessus de l'étape
la plus longue du housekeeping (la compaction d'un jour), que YARN réponde ou non : sans réponse
de YARN, un verrou plus vieux que `stale_after_s` est repris, même si son housekeeping tourne
encore.

### Test de reprise

Le test compare une exécution sans arrêt et une exécution dont le driver est tué en plein flux,
sur la même journée, dans deux racines de lac :

```bash
scripts/streaming/demo.sh --interval 5 --housekeeping              # référence, chemins configurés
scripts/streaming/demo.sh --interval 3 --fresh --housekeeping \
  --root /user/etl/streaming-demo/crash --crash-after 10           # driver tué, puis relancé
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh scripts/streaming/report.py /datalake /user/etl/streaming-demo/crash
scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash /user/etl/streaming-demo
```

`demo.sh` démarre le job, attend que ses requêtes tournent, lance le simulateur, et avec
`--crash-after N` surveille le checkpoint par WebHDFS : dès que la requête des agrégats a
planifié le micro-batch `N`, il tue le conteneur client du driver (`docker kill`), affiche le
dernier micro-batch planifié et validé de chaque requête, laisse le simulateur déposer des
fichiers pendant 20 s, puis relance le job. Avec `--root`, tout va sous la racine donnée, lignes
d'audit comprises (`<racine>/ops/audit`) ; `--fresh` vide d'abord cette racine et est refusé sur
`/datalake`. Le rapport doit afficher `IDENTICAL` : même nombre de lignes, mêmes clés (fenêtre ×
capteur, `alert_id`, ligne rejetée) et mêmes valeurs dans les deux exécutions, sans clé en
double, et lister l'application tuée parmi celles sans ligne d'audit. Le test local
`test_a_batch_replayed_after_a_crash_rewrites_its_output_instead_of_adding_to_it` rejoue le même
cas à coup sûr : le micro-batch 2 est écrit, puis le driver « tombe » avant sa validation.

## Repartir de zéro ou changer le plan

Le checkpoint lie chaque requête à son plan et à son nombre de partitions d'état. Après un
changement des transformations (agrégation, clés de déduplication) ou de `state_partitions`,
démarrer sur un nouveau checkpoint **et** de nouvelles tables : un nouveau checkpoint repart du
micro-batch 0, dont les partitions remplaceraient celles du même numéro. Par exemple :

```bash
R=/datalake/streaming-v2   # une racine neuve
scripts/cluster/submit.sh streaming-sensors --set paths.checkpoints=$R/checkpoints \
  --set paths.gold=$R/gold --set paths.streaming=$R/streaming
```

Pour tout effacer et rejouer la journée dans une racine de test,
`scripts/streaming/demo.sh --fresh --root <racine>` supprime d'abord `incoming`,
`incoming_staging`, les deux tables gold, les rejets, l'archive, le checkpoint, le dossier
`control` et l'audit de cette racine.

## Dépannage

| Symptôme | Cause et correction |
| --- | --- |
| Aucun micro-batch n'écrit de ligne alors que des fichiers arrivent | Les noms ne suivent pas `sensors_<yyyyMMdd>T<HHmmss>.jsonl`, ou les fichiers sont déjà connus du checkpoint (un fichier n'est lu qu'une fois, même remplacé). |
| Les fenêtres des dernières minutes manquent | Normal en mode `append` : elles attendent des événements plus récents de 2 minutes. |
| Des événements très en retard sont agrégés au début d'un checkpoint neuf | Les deux premiers micro-batches n'ont pas encore de watermark pour filtrer les retards (ADR-0008). |
| Des lignes `FUTURE_EVENT` dans les rejets | L'horloge d'un émetteur date ses événements plus de 60 s après leur émission ; à corriger à la source. |
| Au redémarrage, une erreur de compatibilité d'état (`StateSchemaNotCompatible`, nombre de partitions) | Le plan a changé : nouveau checkpoint, voir ci-dessus. |
| Le job relancé n'écrit rien pendant quelques minutes, `table locked, waiting for its lock` | Le housekeeping compacte la table : l'attente dure une étape, 360 s au plus (`stale_after_s` + 60 s), puis la requête échoue avec `TableLockedError`. Ou le verrou d'une exécution tuée alors que le ResourceManager ne répond pas (`no state from YARN`) : il est repris après `stale_after_s` (300 s). |
| Des dossiers `_sensor_aggregates.staging-…` dans `/datalake/gold` | Écriture interrompue par un arrêt brutal. Spark les ignore (préfixe `_`) ; la première écriture après `stale_after_s` les supprime. |
| Un dossier `_sensor_aggregates.compaction-…` | Compaction d'un housekeeping tué : le housekeeping suivant la termine ou la supprime. Ne pas l'effacer à la main s'il contient `_journal.json` : il tient les seules copies des lignes en cours d'échange. |
| `incoming` garde des fichiers anciens | Le housekeeping ne tourne pas, ou une requête n'a pas validé leur micro-batch (job arrêté). |
| Le job s'arrête avec le code 1 | Une requête a échoué : son exception (`StreamingQueryException`) est dans la sortie du job et dans `error_message` de la ligne d'audit `FAILED`. Les autres requêtes sont arrêtées. |
