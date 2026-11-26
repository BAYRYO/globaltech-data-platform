# Runbook : streaming des capteurs

Le job `streaming-sensors` (couche `streaming`) lit en continu les fichiers capteurs déposés dans
`/datalake/landing/sensors/incoming` et écrit les agrégats par minute et les alertes en gold, et
les lignes rejetées en zone streaming. Les tables sont décrites dans le
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
cluster. Chaque requête lit au plus `max_files_per_trigger` nouveaux fichiers par micro-batch,
toutes les `trigger_interval_s` secondes, les plus anciens d'abord. Seuls les fichiers
`sensors_*.jsonl` sont lus : un fichier doit apparaître d'un bloc dans `incoming` (renommage
atomique), sinon Spark peut en lire une partie.

### Sémantique

| Choix | Valeur | Justification |
| --- | --- | --- |
| Watermark | 2 minutes sur `event_time` | Le contrat place les retards ordinaires entre 10 et 90 s (gardés) et les très grands retards au-delà de 5 min (écartés) : 2 minutes garde les premiers avec 30 s de marge et écarte les seconds. |
| Déduplication | `dropDuplicatesWithinWatermark("event_id")` | L'état d'un identifiant expire avec le watermark, donc reste borné. Un doublon réémis avec un autre `event_time` est aussi écarté. |
| Fenêtres | fixes, 1 minute, par capteur | Grain du modèle de données (machine × métrique). |
| Mode de sortie | `append` | Chaque fenêtre est écrite une fois, définitive, quand le watermark dépasse sa fin. Le mode `update` réécrirait chaque fenêtre à chaque micro-batch, ce qui casse l'écriture idempotente par `batch_id` (mesuré dans ADR-0008). |
| Alertes | premier événement hors seuil de chaque capteur et fenêtre, émis dans son micro-batch | Latence de quelques secondes au lieu de fenêtre + watermark (3 minutes). Entre événements arrivés ensemble, le plus petit `event_time` l'emporte. Le micro-batch `n` écarte les fenêtres déjà alertées par les micro-batches inférieurs, d'où le même résultat au rejeu. |
| Écriture | `foreachBatch`, partition `batch_id=<n>` remplacée | Spark rejoue un micro-batch non validé avec le même numéro et les mêmes fichiers : la réécriture remplace la sortie partielle. |

Conséquence de `append` sur un flux fini : les fenêtres des 3 dernières minutes (watermark +
fenêtre) ne sont écrites que si des événements plus récents arrivent. Sur les 15 minutes de la
journée `dev`, les fenêtres de 00:00 à 00:11 sont écrites, celles de 00:12 à 00:14 restent
ouvertes dans l'état.

## Configuration

Section `[streaming]` de `conf/app.toml`, surchargeable comme toute clé
(`GTDP_STREAMING__<CLÉ>` ou `--set streaming.<clé>=<valeur>`) :

| Clé | Défaut | Effet |
| --- | --- | --- |
| `trigger_interval_s` | `5` | Secondes entre deux micro-batches. `0` enchaîne les micro-batches. |
| `max_files_per_trigger` | `12` | Nouveaux fichiers lus au plus par micro-batch (une minute de flux). Au-delà de 18, un événement en retard de 5 minutes peut passer le watermark après une reprise. |
| `state_partitions` | `4` | Partitions de l'état. Figé par le checkpoint à sa création. |
| `stop_after_idle_triggers` | `0` | Arrêt après ce nombre de déclenchements sans données ni micro-batch à exécuter. `0` : jamais. |
| `thresholds` | voir le dictionnaire | Seuils d'alerte par métrique (`above`, `below`). |

## Démarrer le job

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh streaming-sensors
```

Le job tourne en mode client, avec un exécuteur pour laisser la place à un batch à côté
([budget mémoire](../platform.md#budget-mémoire)). `GTDP_PSEUDO_SALT` doit être défini comme pour
tout job `--env cluster`. Pour qu'il s'arrête seul une fois le flux épuisé :

```bash
scripts/cluster/submit.sh streaming-sensors --set streaming.stop_after_idle_triggers=12
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
python -m gtdp.datagen generate --profile dev --seed 42 --out data/generated
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
published 180 files, 11772 events to hdfs://namenode:8020/datalake/landing/sensors/incoming (542.6 s)
published 4 files, 258 events to hdfs://namenode:8020/user/etl/streaming-demo/replay-stop/incoming, stopped by SIGTERM (14.2 s)
```

## Suivre

| Quoi | Où |
| --- | --- |
| Micro-batches écrits | lignes JSON `"batch written"` (requête, `batch_id`, lignes, table) sur la sortie de `submit.sh` |
| Progression, état, watermark | onglet *Structured Streaming* de l'interface Spark, par le lien *ApplicationMaster* de http://localhost:8088 |
| Application terminée | Spark History Server, http://localhost:18080 |
| Checkpoint | `scripts/cluster/client.sh hdfs dfs -ls /datalake/streaming/sensors/checkpoints/aggregates/commits` |
| Audit | une ligne par exécution dans `/datalake/ops/audit`, voir [Vérifier](#vérifier) |

## Arrêter

| Moyen | Commande | Ligne d'audit |
| --- | --- | --- |
| Marqueur d'arrêt (recommandé) | `scripts/streaming/stop.sh` | `SUCCESS`, `stop_reason` = `stop marker` |
| Inactivité | `--set streaming.stop_after_idle_triggers=N` | `SUCCESS`, `stop_reason` = `idle` |
| SIGTERM au seul processus Python du driver | `kill -TERM <pid du python>` dans le conteneur client | `STOPPED`, code de sortie 143 |
| Arrêt brutal (conteneur tué, `kill -9`) | `docker kill <conteneur client>` | aucune |

`stop.sh` crée `/datalake/streaming/control/stop-streaming-sensors`. Le job le voit en moins
d'un intervalle de déclenchement, attend la fin du micro-batch en cours (60 s au plus), arrête
ses requêtes, remplit ses compteurs, écrit sa ligne d'audit `SUCCESS` et supprime le marqueur.
Ctrl+C sur `submit.sh` envoie le signal à tout le groupe de processus : la JVM du driver meurt
avec le processus Python et la ligne d'audit est perdue, comme après un arrêt brutal. Les données
restent cohérentes dans tous les cas : le redémarrage reprend au dernier micro-batch validé.

## Vérifier

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh scripts/streaming/report.py /datalake
```

Le rapport donne pour chaque table ses lignes, ses clés et ses clés en double, puis les alertes
et les lignes d'audit de `streaming-sensors`. Attendu sur la journée `dev` rejouée en entier :

| Mesure | Attendu | Origine |
| --- | --- | --- |
| Lignes lues (`rows_in`) | 11 772 | lignes des 180 fichiers |
| Rejets | 108 : `MALFORMED_JSON` 54, `NULL_VALUE` 54 | manifeste |
| Doublons écartés (`rows_duplicated`) | 324 | `DUPLICATE_EVENT` du manifeste |
| Écartés par le watermark | 108 | `VERY_LATE_EVENT` du manifeste |
| Agrégats | 743 lignes, 9 010 événements : 12 fenêtres × 60 capteurs (00:00 à 00:11) et 23 lignes avant minuit | fenêtres finales du flux |
| Alertes | 10 : 5 capteurs en pic × 2 fenêtres (00:03 et 00:04) | 120 événements `SPIKE` |
| Clés en double | 0 dans chaque table | |

## Reprise après arrêt brutal

Après un arrêt brutal, relancer simplement le job. Chaque requête relit son checkpoint : un
micro-batch planifié (`offsets/<n>`) sans validation (`commits/<n>`) est rejoué avec les mêmes
fichiers, et son écriture remplace la partition `batch_id=<n>` déjà écrite en partie ou en
entier. Les fichiers arrivés pendant l'arrêt sont lus ensuite, par paquets de
`max_files_per_trigger`.

### Test de reprise

Le test compare une exécution sans arrêt et une exécution dont le driver est tué en plein flux,
sur la même journée, dans deux racines de lac :

```bash
scripts/streaming/demo.sh --interval 3                       # référence, chemins configurés
scripts/streaming/demo.sh --interval 3 --fresh \
  --root /user/etl/streaming-demo/crash --crash-after 30      # driver tué, puis relancé
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
  scripts/cluster/submit.sh scripts/streaming/report.py /datalake /user/etl/streaming-demo/crash
scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash /user/etl/streaming-demo
```

`demo.sh` démarre le job, attend que ses requêtes tournent, lance le simulateur, et avec
`--crash-after N` surveille le checkpoint par WebHDFS : dès que la requête des agrégats a
planifié le micro-batch `N`, il tue le conteneur client du driver (`docker kill`), affiche le
dernier micro-batch planifié et validé de chaque requête, laisse le simulateur déposer des
fichiers pendant 20 s, puis relance le job. Le rapport doit afficher `IDENTICAL` : même nombre de
lignes, mêmes clés (fenêtre × capteur, `alert_id`, ligne rejetée) et mêmes valeurs dans les deux
exécutions, sans clé en double. Le test local
`test_a_batch_replayed_after_a_crash_rewrites_its_output_instead_of_adding_to_it` rejoue le même
cas à coup sûr : le micro-batch 2 est écrit, puis le driver « tombe » avant sa validation.

## Repartir de zéro ou changer le plan

Le checkpoint lie chaque requête à son plan et à son nombre de partitions d'état. Après un
changement des transformations (agrégation, clés de déduplication) ou de `state_partitions`,
démarrer sur un nouveau checkpoint et de nouvelles tables, par exemple :

```bash
scripts/cluster/submit.sh streaming-sensors \
  --set paths.checkpoints=/datalake/streaming/sensors/checkpoints_v2
```

Pour tout effacer et rejouer la journée, `scripts/streaming/demo.sh --fresh` supprime d'abord
`incoming`, `incoming_staging`, les deux tables gold, les partitions de rejets, le checkpoint et
le dossier `control` de sa racine.

## Dépannage

| Symptôme | Cause et correction |
| --- | --- |
| Aucun micro-batch n'écrit de ligne alors que des fichiers arrivent | Les noms ne correspondent pas à `sensors_*.jsonl`, ou les fichiers sont déjà connus du checkpoint (un fichier n'est lu qu'une fois, même remplacé). |
| Les fenêtres des dernières minutes manquent | Normal en mode `append` : elles attendent des événements plus récents de 2 minutes. |
| Des événements très en retard sont agrégés au début d'un checkpoint neuf | Les deux premiers micro-batches n'ont pas encore de watermark pour filtrer les retards (ADR-0008). |
| Au redémarrage, une erreur de compatibilité d'état (`StateSchemaNotCompatible`, nombre de partitions) | Le plan a changé : nouveau checkpoint, voir ci-dessus. |
| Des dossiers `_sensor_aggregates.staging-…` dans `/datalake/gold` | Écriture interrompue par un arrêt brutal. Spark les ignore (préfixe `_`). Les supprimer job arrêté. |
| Le job s'arrête avec le code 1 | Une requête a échoué : son exception (`StreamingQueryException`) est dans la sortie du job et dans `error_message` de la ligne d'audit `FAILED`. Les autres requêtes sont arrêtées. |
