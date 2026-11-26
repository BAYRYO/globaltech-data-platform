# Runbook : MapReduce contre Spark (Lot 6)

Le même calcul tourne sur deux moteurs : le CA facturé par pays et les lignes invalides par motif,
à partir des commandes brutes de la zone landing. La définition du calcul et l'analyse des mesures
sont dans [MapReduce contre Spark](../mapreduce-vs-spark.md), les formats de sortie dans le
[dictionnaire de données](../data-dictionary/mapreduce.md).

| Élément | Hadoop Streaming | Spark |
| --- | --- | --- |
| Code | `mapreduce/mapper.py`, `mapreduce/reducer.py` | `src/gtdp/perf/revenue_by_country.py` |
| Lancement | `scripts/mapreduce/run_mr.sh` | job `revenue-by-country-spark` (`scripts/cluster/submit.sh`) |
| Sortie HDFS | répertoire passé en argument | `<paths.ops>/mr_vs_spark/spark/{revenue,counters}` |
| Compteurs | compteurs Hadoop (groupes `Quality` et `Lines`), JobHistory | fichier `counters/` et colonne `metrics_json` de l'audit |
| Historique | MapReduce JobHistory, http://localhost:19888 | Spark History Server, http://localhost:18080 |

Les deux moteurs lisent toutes les partitions `ingestion_date` de `orders` et `customers` jusqu'à
la date donnée, qui doit exister pour `orders`.

## Lancer

Depuis la racine du dépôt, cluster démarré et initialisé ([guide de la plateforme](../platform.md)).
Le job Spark tourne en `--env cluster`, il exige donc `GTDP_PSEUDO_SALT` comme tous les jobs, même
s'il ne pseudonymise rien.

```bash
export GTDP_PSEUDO_SALT=<valeur hors dépôt>

# Hadoop Streaming : J1 à J3 du landing, résultat sous /datalake/ops/mr_vs_spark/mapreduce
scripts/mapreduce/run_mr.sh /datalake/landing 2026-01-03 /datalake/ops/mr_vs_spark/mapreduce

# Spark : mêmes partitions, résultat sous /datalake/ops/mr_vs_spark/spark
scripts/cluster/submit.sh revenue-by-country-spark --date 2026-01-03

# Les deux, 3 fois chacun, avec vérification et rapport dans build/mr-vs-spark/
scripts/mapreduce/compare.sh --date 2026-01-03
```

`run_mr.sh` affiche le résultat puis `job_id=job_…`. Le job Spark journalise
`revenue by country written` avec `yarn_app_id`, le répertoire de sortie et la liste des fichiers
de commandes lus.

`compare.sh` attend avant chaque exécution que YARN n'ait plus aucune application active, et
recommence (3 fois au plus) une exécution qu'une autre application a chevauchée. `--no-wait`
supprime l'attente pour un simple contrôle d'égalité ; le rapport signale alors les exécutions non
exclusives. Le code de sortie vaut 1 si un contrôle échoue, et `report.md` liste les écarts.

### Profil `full` (10 M de lignes)

```bash
scripts/cluster/client.sh bash -c '
  python3 -m gtdp.datagen generate --profile full --seed 42 --out /tmp/gen &&
  python3 -m gtdp.datagen verify --path /tmp/gen/full &&
  base=/benchmarks/mr-vs-spark/full/landing &&
  for d in 2026-01-01 2026-01-02 2026-01-03; do
    for s in orders customers; do
      hdfs dfs -mkdir -p $base/$s/ingestion_date=$d &&
      hdfs dfs -put -f /tmp/gen/full/landing/$d/$s/${s}_${d//-/}.csv $base/$s/ingestion_date=$d/
    done
    hdfs dfs -mkdir -p $base/_manifests/ingestion_date=$d &&
    hdfs dfs -put -f /tmp/gen/full/landing/$d/_MANIFEST.json $base/_manifests/ingestion_date=$d/
  done'
scripts/mapreduce/compare.sh --date 2026-01-03 \
  --landing hdfs://namenode:8020/benchmarks/mr-vs-spark/full/landing --out build/mr-vs-spark/full
```

Le générateur écrit dans le `/tmp` du conteneur client, pas dans la copie du dépôt : 1 Go de moins
à faire transiter par le montage du poste. Mesuré : 23 s de génération, 26 s de vérification,
24 s de dépôt. Les fichiers occupent 718 036 812 octets logiques, 2 154 110 436 octets physiques en
réplication 3, sous le quota de 20 Go de `/benchmarks`. Supprimer le jeu après usage :

```bash
scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash /benchmarks/mr-vs-spark
```

## Rejouer

Les deux jobs sont idempotents : chacun remplace toute sa sortie (`hdfs dfs -rm -r` avant le job
MapReduce, `mode("overwrite")` en Spark), et le même landing donne le même résultat octet pour
octet. Rejouer après un nouveau `land.sh` d'une date relit simplement le nouveau landing.

## Vérifier

```bash
scripts/cluster/client.sh bash -c 'diff \
  <(hdfs dfs -cat /datalake/ops/mr_vs_spark/mapreduce/part-*) \
  <(hdfs dfs -cat /datalake/ops/mr_vs_spark/spark/revenue/part-*) && echo identiques'
scripts/cluster/client.sh hdfs dfs -cat '/datalake/ops/mr_vs_spark/spark/counters/part-*'
scripts/cluster/client.sh mapred job -status <job_id>      # compteurs Quality et Lines
```

Contrôles attendus, tous faits par `compare.sh` :

- sortie MapReduce et sortie Spark identiques ;
- `Quality.*` égaux, motif par motif, aux anomalies `orders` des manifestes des dates lues
  (`BAD_DISCOUNT` vaut 0, le générateur n'en injecte pas) ;
- `Lines.HEADER + Lines.COMPLETED + Lines.OTHER_STATUS + Σ Quality.*` = `Map input records` ;
- mêmes répertoires lus : `mapreduce.input.fileinputformat.inputdir` et `tmpfiles` du job
  (API REST JobHistory, `/conf`) contre `order_files` et `customer_dirs` du journal Spark.

Tests locaux, sans cluster :

```bash
scripts/test.sh tests/mapreduce
```

## Dépanner

| Symptôme | Cause et correction |
| --- | --- |
| `error: no orders landed in …/orders/ingestion_date=<date>` | La date n'a pas de partition `orders`. Lister `hdfs dfs -ls <landing>/orders`, ou déposer le jour avec `land.sh`. |
| Le job Spark échoue avec `FileNotFoundError: nothing landed in …` | Même cause, côté Spark. |
| Le job Spark sort avec le code 2 sans démarrer | `GTDP_PSEUDO_SALT` manque. |
| `File does not exist: …/customers_….csv` au lancement du job MapReduce | Un chemin `-files` sans schéma désigne un fichier local. `run_mr.sh` préfixe le landing par `fs.defaultFS` ; ne pas retirer ce préfixe. |
| Tâche map en échec, `PipeMapRed.waitOutputThreads(): subprocess failed with code 1` | Le mapper Python a levé une exception. `yarn logs -applicationId <id> -log_files stderr` montre la trace ; reproduire avec `cat <fichier> \| python3 mapreduce/mapper.py <clients> \| sort \| python3 mapreduce/reducer.py`. |
| `compare.sh` reste sur l'attente de YARN | Une autre application tourne (streaming, autre lot). `scripts/cluster/client.sh yarn application -list` ; attendre, ou `--no-wait` pour un contrôle sans mesure. |
| `compare.sh` échoue sur `TimeoutError: http://mr-history:19888/…` | Le JobHistory n'a pas encore déplacé le job dans `done` (toutes les 10 s) ou il est arrêté : `scripts/cluster/status.sh`. |
| Un compteur `Quality` diffère du manifeste | Une règle a changé dans un seul moteur, ou le landing ne correspond plus à son manifeste. Lancer `scripts/test.sh tests/mapreduce` : le test d'égalité des moteurs localise la règle. |
