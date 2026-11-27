# Runbooks

Commencez par le [runbook d'exploitation](exploitation.md), point d'entrée de l'équipe qui
exploite la plateforme : démarrage et arrêt, santé, traitement d'un jour, jobs, incidents.

Une page par couche ou par traitement : lancer, rejouer, vérifier, dépanner. Les commandes se
lancent depuis la racine du dépôt, cluster démarré ([guide de la plateforme](../platform.md)). Les
colonnes des tables sont décrites dans le [dictionnaire de données](../data-dictionary/README.md).

| Page | Contenu |
| --- | --- |
| [Exploitation](exploitation.md) | démarrer, arrêter et vérifier la plateforme, traiter un jour, lancer chaque job, codes de sortie, procédures d'incident, dépannage consolidé |
| [Bronze](bronze.md) | ingérer un jour du landing, rejouer, vérifier contre le manifeste et l'audit, verrou de table, exécution arrêtée ou tuée |
| [Silver](silver.md) | traiter les jours dans l'ordre, rejouer, réparer un jour incomplet, changer le sel, rapport de vérification |
| [Gold](gold.md) | construire gold depuis les jours silver terminés, rejouer, corriger une commande, état de publication, rapport, reprise après un arrêt brutal |
| [Streaming des capteurs](streaming.md) | démarrer, alimenter, suivre et arrêter le flux, housekeeping horaire, quotas sur 24 h, reprise après arrêt brutal |
| [MapReduce contre Spark](mapreduce.md) | lancer la comparaison du Lot 6 sur `dev` et `full`, rejouer, vérifier les résultats |
| [HDFS](hdfs.md) | perte d'un DataNode, DataNode de secours, décommission, retour à l'état nominal, rejouer la panne |

## Interroger l'audit

Chaque exécution de `run.py` ajoute une ligne à `/datalake/ops/audit`, même en cas d'échec
([ADR-0002](../adr/0002-parquet-idempotence.md#table-daudit-et-arrêt-dune-exécution)). Ses
colonnes sont `job_name`, `layer`, `source`, `ingestion_date`, `env`, `status` (`SUCCESS`,
`FAILED`, `STOPPED`), `started_at`, `ended_at`, `duration_s`, `rows_in`, `rows_out`,
`rows_rejected`, `rows_duplicated`, `yarn_app_id`, `git_sha`, `error_message` et `metrics_json`.
`run_date` et `run_id` sont les colonnes de partition. `source` est nul quand le job a traité
toutes les sources du jour.

La table tient en quelques Ko : la requête tourne en mode local dans le conteneur client, sans
prendre de conteneur YARN aux jobs en cours. `pyspark` lit le script sur son entrée standard,
d'où `GTDP_CLIENT_STDIN=1`. Sans terminal, le shell ne crée pas la variable `spark` : le script
crée la session lui-même.

```bash
GTDP_CLIENT_STDIN=1 scripts/cluster/client.sh pyspark --master 'local[2]' \
	--conf spark.ui.showConsoleProgress=false 2>/dev/null <<'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.parquet("hdfs://namenode:8020/datalake/ops/audit").createOrReplaceTempView("audit")

# Exécutions d'un jour d'ingestion
spark.sql("""
SELECT run_id, job_name, source, status, duration_s,
       rows_in, rows_out, rows_rejected, rows_duplicated, yarn_app_id
FROM audit
WHERE ingestion_date = DATE'2026-01-02'
ORDER BY started_at
""").show(50, truncate=False)

# Échecs et arrêts, avec leur message
spark.sql("""
SELECT started_at, job_name, source, ingestion_date, status, error_message
FROM audit
WHERE status <> 'SUCCESS'
ORDER BY started_at DESC
""").show(20, truncate=False)

# Comptes silver par source, à côté de la réconciliation bronze = propres + rejets + doublons
spark.sql("""
SELECT a.ingestion_date, a.run_id, a.status, s.source,
       s.counts.rows_in, s.counts.clean, s.counts.rejected, s.counts.duplicated,
       s.counts.rows_in = s.counts.clean + s.counts.rejected + s.counts.duplicated AS adds_up,
       s.counts.rolled_back
FROM audit a
LATERAL VIEW explode(from_json(a.metrics_json,
  'sources MAP<STRING, STRUCT<rows_in: BIGINT, clean: BIGINT, rejected: BIGINT,
                              duplicated: BIGINT, rolled_back: BOOLEAN>>').sources) s AS source, counts
WHERE a.job_name = 'silver'
ORDER BY a.started_at, s.source
""").show(60, truncate=False)
EOF
```

`2>/dev/null` masque les journaux de Spark. Retirez-le pour voir l'erreur d'une requête qui
échoue.

Extrait de la troisième requête sur le cluster, profil `dev` :

```
+--------------+-------------------------+-------+---------+-------+------+--------+----------+-------+-----------+
|ingestion_date|run_id                   |status |source   |rows_in|clean |rejected|duplicated|adds_up|rolled_back|
+--------------+-------------------------+-------+---------+-------+------+--------+----------+-------+-----------+
|2026-01-02    |20261126T222100Z-664166e3|SUCCESS|customers|44     |42    |0       |2         |true   |false      |
|2026-01-02    |20261126T222100Z-664166e3|SUCCESS|orders   |2740   |2631  |68      |41        |true   |false      |
|2026-01-02    |20261126T222100Z-664166e3|SUCCESS|products |17     |12    |4       |1         |true   |false      |
|2026-01-02    |20261126T222100Z-664166e3|SUCCESS|weblogs  |274    |273   |1       |0         |true   |false      |
+--------------+-------------------------+-------+---------+-------+------+--------+----------+-------+-----------+
```

`adds_up` doit valoir `true` sur toute ligne `SUCCESS`. Le détail par code de rejet et par
drapeau est dans `metrics_json.sources.<source>.measures`. La comparaison de ces comptes avec le
manifeste de chaque jour est le rôle du rapport silver
([runbook silver, « Vérifier silver »](silver.md#vérifier-silver)).
