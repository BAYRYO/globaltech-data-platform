# ADR-0008 : sémantique du streaming capteurs

**Statut** : acceptée

## Contexte

Le Lot 5 exige des agrégats par fenêtre, un watermark, la déduplication, la reprise après arrêt
avec checkpoint, des alertes sur seuil et une écriture idempotente (« prouvée » au niveau
Excellent). La revue adverse du Lot 0 a mesuré sur Spark 3.5.3, avec un flux de test conforme au
contrat :

- en mode `append`, une fenêtre n'est écrite que lorsque le watermark la dépasse : avec un délai de
  10 min sur 10 min de flux, aucune fenêtre n'était écrite ; avec 5 min, 4 fenêtres sur 10 ;
- en mode `update`, chaque fenêtre est réécrite à chaque micro-batch (7 082 lignes pour 679 clés),
  ce qui rend un puits indexé par `batch_id` non idempotent au sens métier ;
- les deux premiers micro-batches passent avant que le watermark soit établi : des événements
  très en retard y sont agrégés ;
- un arrêt brutal après l'écriture du batch 3 puis une reprise sur checkpoint donnent exactement
  les mêmes lignes qu'une exécution sans arrêt.

Le job tourne en continu (24/7) : ses fichiers d'entrée, ses partitions et ses event logs ne
doivent pas remplir les quotas HDFS.

## Décision

- **Source** : fichiers `sensors_<yyyyMMdd>T<HHmmss>.jsonl` de `/datalake/landing/sensors/incoming`,
  schéma explicite. Chaque requête liste le répertoire une fois par déclenchement, avec un filtre
  de noms (`pathGlobFilter`) : un fichier temporaire ou mal nommé reste ignoré. Une ligne qui n'est pas
  exactement un objet JSON strict, sans valeur finie, ou datée de plus de 60 s après son émission
  va dans `streaming/rejects` (`MALFORMED_JSON`, `BAD_VALUE`, `FUTURE_EVENT`, en plus de
  `MISSING_FIELD` et `NULL_VALUE`).
- **Déclenchement toutes les 30 s** (`trigger_interval_s`), soit 6 fichiers de 5 s par
  micro-batch. Chaque micro-batch ajoute des fichiers, des partitions et des entrées d'event log :
  avec un micro-batch toutes les 5 s et sans housekeeping, `/datalake/streaming` atteignait son
  quota de noms en 28 jours et `/spark-logs` le sien en 1,8 jour
  ([runbook streaming](../runbook/streaming.md#quotas--projection-sur-24-h)).
- **Au plus 12 fichiers par micro-batch** (`max_files_per_trigger`, une minute de flux), valeur
  à garder sous 18. Le filtre de retard du micro-batch `k` utilise le watermark calculé à la fin du
  micro-batch `k − 2`. Quand le job relit un arriéré après une reprise, deux micro-batches de `N`
  fichiers couvrent `2 × N × 5 s` de flux : un événement en retard de 5 minutes reste écarté tant
  que `2 min + 2 × N × 5 s ≤ 5 min`, soit `N ≤ 18`.
- **Watermark de 2 minutes** sur `event_time` : le contrat place les retards ordinaires entre 10 et
  90 s (acceptés) et les très grands retards au-delà de 5 min (écartés), hors des 36 premiers
  fichiers pour que le watermark soit établi.
- **Déduplication** par `dropDuplicatesWithinWatermark("event_id")` (Spark ≥ 3.5) : l'état d'un
  identifiant est purgé avec le watermark, sans exiger que les doublons portent le même
  `event_time`.
- **Agrégats** : fenêtres fixes de 1 minute par machine et métrique, mode **`append`** : chaque
  fenêtre est écrite une seule fois, quand elle est définitive.
- **Alertes** : par événement, après déduplication, dès le micro-batch où arrive le premier
  événement hors seuil d'un capteur dans une fenêtre fixe de 1 minute (latence d'un
  déclenchement, au lieu de fenêtre + watermark). Une seule alerte par (capteur, fenêtre),
  d'identifiant `alert_id` = SHA-256 de `sensor_id` et du début de la fenêtre : les
  micro-batches suivants n'en émettent pas d'autre pour cette fenêtre. L'alerte porte, parmi les
  événements hors seuil de la fenêtre arrivés dans ce micro-batch, celui de plus petit
  `event_time` (puis de plus petit `event_id`), et non le plus petit `event_time` de toute la
  fenêtre : ses champs d'événement suivent l'ordre d'arrivée et le découpage en micro-batches.
  Ils sont les mêmes pour les mêmes fichiers dans les mêmes micro-batches et au rejeu d'un
  micro-batch ; le nombre d'alertes, leurs clés, fenêtres et seuils ne dépendent pas du
  découpage. Les seuils sont une table de configuration par métrique, pas des conditions dans le
  code.
- **Écriture idempotente** : `foreachBatch` écrit chaque sortie dans `…/<jour>=<d>/batch_id=<n>/`
  (`window_date` pour les agrégats et les alertes, `arrival_date` pour les rejets), un fichier par
  partition, en remplaçant ces partitions par `write_partitions`. Le micro-batch `n` dédoublonne
  ses alertes contre celles des micro-batches `< n` seulement. Spark rejoue un batch non validé
  avec le même `batch_id` et les mêmes fichiers après une reprise : la réécriture remplace le
  résultat partiel au lieu de le dupliquer. Preuve : driver tué en plein flux, reprise,
  comparaison avec une exécution sans arrêt
  ([test de reprise](../runbook/streaming.md#test-de-reprise)).
- Les événements écartés par le watermark sont comptés par `numRowsDroppedByWatermark` (métriques
  de progression), hors des deux premiers micro-batches.
- **Arrêt propre par marqueur** : `scripts/streaming/stop.sh` crée
  `/datalake/streaming/control/stop-streaming-sensors`. Le job le voit en moins d'un
  déclenchement, attend la fin du micro-batch en cours (60 s au plus), arrête ses requêtes,
  remplit ses compteurs et supprime le marqueur. Sa ligne d'audit est `SUCCESS`, avec
  `stop_reason` = `stop marker`.
- **Housekeeping** : le job `streaming-housekeeping`, lancé toutes les heures à côté du flux,
  archive dans `streaming/archive/sensors/arrival_date=<d>/` les fichiers d'entrée validés par
  les trois requêtes, purge l'archive au-delà du jour courant et des 2 jours précédents
  (`archive_retention_days`), et fusionne par jour les partitions des micro-batches que Spark ne
  rejoue plus (pour les agrégats et les alertes, seulement les jours que le watermark a dépassés).
  Une compaction se fait sous le verrou
  de la table, et le flux attend ce verrou entre deux micro-batches
  ([ADR-0002](0002-parquet-idempotence.md)).

## Conséquences

- Le flux étant fini en démonstration, les fenêtres des 3 dernières minutes ne sont écrites que si
  des données plus récentes arrivent : la démonstration rejoue 15 minutes de flux pour en
  finaliser 12.
- Le checkpoint lie la requête à son plan : changer l'agrégation impose un nouveau répertoire de
  checkpoint (documenté dans le runbook).
- Un SIGTERM envoyé au seul processus Python du driver donne une ligne `STOPPED` avec ses
  compteurs. Limite : Ctrl+C sur `submit.sh` envoie le signal à tout le groupe de processus ; la
  JVM du driver meurt avec le processus Python et la ligne d'audit est perdue, comme après un
  arrêt brutal. Les données restent cohérentes : le redémarrage reprend au dernier micro-batch
  validé. Le marqueur d'arrêt est donc le moyen recommandé.
- Limite de Spark 3.5 : le journal de la source fichier de chaque checkpoint (`sources/0`) garde
  une entrée par fichier lu, sans fin. Il croît d'environ 7 Mo par jour pour les trois requêtes ;
  avec l'archive, `/datalake/streaming` est plein en environ 120 jours. Repartir d'un nouveau
  checkpoint remet ce journal à zéro, avec de nouvelles tables
  ([runbook streaming](../runbook/streaming.md#quotas--projection-sur-24-h)).
- L'event log d'une application en cours n'est jamais purgé par le History Server : il compacte
  les fichiers roulés au-delà de 2 (`spark.history.fs.eventLog.rolling.maxFilesToRetain`), ce qui
  borne le job 24/7 à environ 55 Mo stockés
  ([guide de la plateforme](../platform.md#arborescence-hdfs-droits-et-quotas)).

## Alternatives écartées

- Mode `update` avec écrasement par clé (fenêtre, machine, métrique) : idempotent, mais réécrit
  chaque fenêtre à chaque batch et exige une fusion par clé que Parquet seul ne fournit pas.
- Alertes sur la fenêtre : latence de 3 minutes, inutile pour une alerte industrielle.
- `dropDuplicates(["event_id", "event_time"])` : un doublon réémis avec un autre horodatage
  passerait.
- `cleanSource` de Spark pour archiver les fichiers lus : avec trois requêtes sur le même
  répertoire, l'une déplacerait un fichier avant que les autres ne l'aient lu. Le housekeeping
  n'archive que les fichiers que les trois checkpoints ont validés.
- Un micro-batch toutes les 5 s : voir le déclenchement ; 30 s divise par 6 le nombre de
  micro-batches, de partitions et d'entrées d'event log.
