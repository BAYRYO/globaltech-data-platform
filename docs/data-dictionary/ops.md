# Dictionnaire de données ops

`/datalake/ops` tient ce que les jobs écrivent pour l'exploitation, pas pour l'analyse : la table
d'audit, la table du job `smoke` et l'empreinte du sel de pseudonymisation. L'ACL par défaut donne
la lecture au groupe `ops`. Les sorties du calcul du Lot 6 (`ops/mr_vs_spark/spark/`) sont des
fichiers texte décrits dans le [dictionnaire MapReduce contre Spark](mapreduce.md).

| Table ou fichier | Grain | Clé | Partition | Écriture |
| --- | --- | --- | --- | --- |
| `ops/audit` | une exécution de `run.py` | `run_id` | `run_date`, `run_id` | une partition par exécution, ajoutée à la fin de l'exécution, jamais réécrite |
| `ops/smoke` | un groupe de nombres | `bucket` | aucune | table remplacée par chaque exécution du job `smoke` (`replace_table`) |
| `ops/pseudonymization/salt_fingerprint.json` | le sel de silver | aucune | aucune | écrit par la première exécution silver, jamais réécrit |

## `ops/audit`

Chaque exécution de `run.py` écrit une ligne (`gtdp.audit.audited_run`), aussi quand le job échoue
ou qu'un signal l'arrête. La ligne va dans son propre répertoire
`run_date=<d>/run_id=<id>/` : deux jobs simultanés n'écrivent jamais dans le même répertoire.
C'est la seule table en ajout seul. Les requêtes utiles sont dans
[interroger l'audit](../runbook/README.md#interroger-laudit), les clés de `metrics_json` de chaque
job dans le runbook de sa couche. Les exemples sont ceux de l'exécution silver de J2 que montre
ce runbook. Les valeurs qu'il ne montre pas (microsecondes, durée, application YARN, commit) ont
le format des valeurs écrites.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `job_name` | string | Nom du job, premier argument de `run.py`. Jamais nul. | `silver` |
| `layer` | string | Couche du job : `bronze`, `silver`, `gold`, `streaming`, `ops` ou `perf` (`Layer`). | `silver` |
| `source` | string | Valeur de `--source`, nulle quand le job a traité toutes les sources. | `NULL` |
| `ingestion_date` | date | Valeur de `--date`, nulle pour un job sans date. | `2026-01-02` |
| `env` | string | Environnement de la configuration : `local` ou `cluster`. | `cluster` |
| `status` | string | `SUCCESS`, `FAILED` (exception du job) ou `STOPPED` (SIGTERM ou SIGINT reçu par le driver, [ADR-0002](../adr/0002-parquet-idempotence.md)). | `SUCCESS` |
| `started_at` | timestamp | Début de l'exécution, en UTC. Les lignes bronze de l'exécution ont ce `_ingested_at`. | `2026-11-26 22:21:00.512344` |
| `ended_at` | timestamp | Fin de l'exécution en UTC, quand la ligne est écrite. | `2026-11-26 22:22:01.906812` |
| `duration_s` | double | `ended_at - started_at` en secondes, arrondi à la milliseconde. | `61.394` |
| `rows_in` | bigint | Lignes lues, au sens du job ; nul si le job ne le mesure pas. Silver : lignes bronze du jour. | `3075` |
| `rows_out` | bigint | Lignes produites, au sens du job ; nul si le job ne le mesure pas. Silver : lignes propres. | `2958` |
| `rows_rejected` | bigint | Lignes rejetées ; nul si le job ne rejette rien. | `73` |
| `rows_duplicated` | bigint | Lignes écartées comme doublons ; nul si le job n'en écarte pas. | `44` |
| `yarn_app_id` | string | `applicationId` de la session Spark : `application_…` sur YARN, `local-…` en mode local. | `application_1795715756506_0207` |
| `git_sha` | string | `GTDP_GIT_SHA` à la soumission : `scripts/cluster/client.sh` y met le commit court du dépôt. Nul sans la variable. | `07aeeb3` |
| `error_message` | string | `<type de l'exception>: <message>` pour `FAILED`, `stopped by SIGTERM` ou `stopped by SIGINT` pour `STOPPED`, tronqué à 2 000 caractères. Nul pour `SUCCESS`. | `NULL` |
| `metrics_json` | string | Mesures propres au job, objet JSON à clés triées ; une valeur que JSON ne sait pas écrire devient une chaîne. Vaut `{"audit_error": "…"}` si les mesures ou les compteurs n'ont pas pu être écrits. Jamais nul. | `{"sources": {"customers": {"clean": 42, …}, …}, "unknown_columns": {…}}` |
| `run_date` | date | Partition : date UTC de `started_at`. | `2026-11-26` |
| `run_id` | string | Partition : `<started_at au format yyyyMMddTHHmmssZ>-<8 chiffres hexadécimaux aléatoires>`. Les colonnes `_run_id` de bronze et de silver le reprennent. | `20261126T222100Z-664166e3` |

Contrôles : `audited_run` refuse un compteur qui n'est pas un entier (`TypeError` dans le job).
Un compteur hors de l'intervalle `bigint` vide les quatre compteurs et met l'erreur dans
`metrics_json`. Un échec de l'écriture de la ligne est journalisé et ne remplace jamais
l'erreur du job.

## `ops/smoke`

Le job `smoke` vérifie Spark, le système de fichiers du lake et l'audit de bout en bout : il range
les nombres de 0 à 999 dans 10 groupes selon leur reste modulo 10.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `bucket` | bigint | Reste modulo 10, de 0 à 9. Clé. | `3` |
| `count` | bigint | Nombres du groupe, toujours 100. | `100` |
| `run_id` | string | Exécution qui a écrit la table, clé `run_id` de l'audit. | `20261126T183015Z-0b6f2a91` |

La ligne d'audit du job donne `rows_in` = 1000 et `rows_out` = 10, compté en relisant la table.

## `ops/pseudonymization/salt_fingerprint.json`

La première exécution silver écrit l'empreinte du sel, avant toute écriture de données
(`gtdp.silver.pseudonymize.ensure_fingerprint`). Une exécution dont le sel a une autre empreinte
échoue avant d'écrire ([ADR-0004](../adr/0004-pseudonymisation.md),
[changer le sel](../runbook/silver.md#changer-le-sel)). Les exemples ont le format des valeurs
écrites : l'empreinte du cluster n'est pas recopiée ici.

| Champ | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `fingerprint` | string | `sha256("gtdp-salt-fingerprint" ‖ sel)` en hexadécimal : distingue deux sels sans en révéler aucun. | `5f1c0e9a…41d7b2c3` |
| `recorded_at` | string | Heure de l'écriture, ISO 8601 en UTC, à la seconde. | `2026-11-26T18:58:36+00:00` |
| `run_id` | string | Exécution silver qui a écrit l'empreinte. | `20261126T185836Z-5d1e0c47` |
