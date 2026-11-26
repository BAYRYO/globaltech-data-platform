# Runbook de l'ingestion bronze

Le job `bronze` copie les fichiers déposés en landing pour une date dans
`/datalake/bronze/<source>/ingestion_date=<date>/`, en Parquet, sans perte. Les colonnes sont
décrites dans le [dictionnaire bronze](../data-dictionary/bronze.md). Les commandes se lancent
depuis la racine du dépôt, cluster démarré ([guide de la plateforme](../platform.md)).

## Ce que fait une exécution

1. Elle lit le manifeste `/datalake/landing/_manifests/ingestion_date=<date>/_MANIFEST.json`.
   `land.sh` l'écrit en dernier. Sans manifeste, le jour n'est pas entièrement déposé.
2. Elle vérifie que la partition landing de chaque source contient exactement les fichiers que
   le manifeste liste pour cette source.
3. Pour chaque fichier, elle compte les lignes de données (lignes physiques moins l'en-tête),
   puis les lignes que le lecteur CSV produit. Le lecteur utilise un schéma tout en `string`
   tiré de l'en-tête du fichier lui-même.
4. Si un fichier a des lignes, un manifeste ou une lecture qui ne concordent pas, elle échoue
   avant toute écriture.
5. Elle remplace la partition de la date dans chaque table bronze par `write_partitions` :
   écriture complète dans un répertoire de transit, puis échange
   ([ADR-0002](../adr/0002-parquet-idempotence.md)). Les autres partitions ne changent pas.
6. Elle relit chaque partition écrite et compare les lignes de chaque fichier aux lignes du
   landing. Elle échoue si elles diffèrent.

L'exécution écrit une ligne dans la table d'audit, aussi en cas d'échec. `rows_in` compte les
lignes de données du landing, `rows_out` les lignes relues en bronze. `metrics_json` contient
les comptes de chaque fichier (`files`), de chaque source (`sources`) et les colonnes inconnues
(`unknown_columns`).

## Ingérer un jour

1. Vérifiez que le jour est déposé :

	```bash
	scripts/cluster/client.sh hdfs dfs -ls /datalake/landing/_manifests/ingestion_date=2026-01-01
	```

2. Exportez le sel, exigé par tout job `--env cluster` (valeur fournie hors dépôt) :

	```bash
	export GTDP_PSEUDO_SALT=<valeur>
	```

3. Lancez le job :

	```bash
	scripts/cluster/submit.sh bronze --date 2026-01-01
	```

	Pour une seule source, ajoutez `--source orders`. Si une autre application Spark tourne
	(le streaming, par exemple), limitez le job à un exécuteur :

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh bronze --date 2026-01-01
	```

4. Lisez le code de sortie. `0` signale un succès, `1` un échec du job, dont le motif est dans
   `error_message` de l'audit, et `2` une configuration invalide ou un sel absent.

Sur la machine de référence, profil `dev` et un exécuteur, `duration_s` de l'audit vaut 9,6 s
pour J1 (1 106 403 lignes, une autre application Spark tournait) et 6,4 s pour J2 comme pour J3
(3 075 lignes chacun, aucune autre application). La commande complète de J1 a pris 21 s,
démarrage du conteneur client et de l'application YARN compris.

## Rejouer un jour

Rejouer un jour relance la même commande. L'exécution remplace la partition de ce jour, et
elle seule, par le contenu du landing actuel. Les jours se rejouent dans n'importe quel ordre.
Rejouer J1 après J3 donne le même bronze.

1. Si les fichiers sources ont changé, déposez-les de nouveau :

	```bash
	scripts/cluster/land.sh data/generated/dev/landing/2026-01-02 2026-01-02
	```

2. Relancez le job :

	```bash
	scripts/cluster/submit.sh bronze --date 2026-01-02
	```

3. Vérifiez avec le rapport (section suivante). Pour la date rejouée, `run_id` est celui de la
   nouvelle exécution. Si le landing n'a pas changé, `rows` et `hash_sum` sont identiques à
   ceux d'avant le rejeu. Les autres dates gardent leur `run_id`.

## Vérifier le bronze

```bash
scripts/cluster/submit.sh src/gtdp/bronze/report.py
```

Le rapport affiche quatre tableaux.

| Tableau | Valeur attendue |
| --- | --- |
| Partitions (`source`, `ingestion_date`, `rows`, `corrupt`, `files`, `run_id`, `hash_sum`) | `rows` égal à `rows` du manifeste du fichier. `corrupt` égal à l'anomalie `MALFORMED_ROW` du manifeste. `hash_sum` inchangé après un rejeu du même landing. |
| Écarts landing et bronze (`landing_not_in_bronze`, `bronze_not_in_landing`, `lines_not_in_bronze`) | Pour une source CSV, 1 ligne absente du bronze, l'en-tête. Pour `weblogs`, aucune. `bronze_not_in_landing` à 0 partout. |
| `discount_pct` des commandes lues avec `mergeSchema` | 0 valeur pour J1 et J2, des valeurs pour J3. |
| Exécutions bronze de l'audit, puis comptes par exécution et fichier | `manifest_rows`, `landing_lines`, `rows_read` et `rows_written` égaux pour chaque fichier d'une exécution `SUCCESS`. |

`hash_sum` est la somme des 60 premiers bits de chaque `_record_hash` d'une partition. L'ordre
des lignes ne compte pas. Deux partitions qui ont les mêmes lignes ont le même `hash_sum`.

Taille de la table, réplication comprise en seconde colonne :

```bash
scripts/cluster/client.sh hdfs dfs -du -h /datalake/bronze
```

Sur le profil `dev`, J1 à J3 occupent 96,8 Mo (290,5 Mo répliqués), dont 85,9 Mo pour les
commandes de J1. La colonne `_record_hash` en représente 62,8 Mo, car un hash hexadécimal de
64 caractères aléatoires ne se compresse pas.

## Dépanner

| Message dans `error_message` | Cause | Action |
| --- | --- | --- |
| `FileNotFoundError: .../_MANIFEST.json not found` | Le jour n'est pas déposé, ou `land.sh` est en cours ou a été interrompu. | Relancez `land.sh` pour la date, puis le job. |
| `LandingError: ... does not match the manifest: missing [...], not in the manifest [...]` | La partition landing d'une source n'a pas les fichiers du manifeste. | Redéposez le jour avec `land.sh`. Rien n'a été écrit. |
| `LandingError: the manifest of ... lists no file for [...]` | `--source` désigne une source que le manifeste ne liste pas. | Vérifiez la source ou le dépôt. |
| `LandingError: the manifest of ... lists unknown sources [...]` | Le manifeste liste une source absente de `SOURCES` (`src/gtdp/schemas.py`). | Ajoutez la source à `SOURCES` ou corrigez le dépôt. |
| `ReconciliationError: <fichier>: manifest N, lines M, read M` | Le fichier landing n'a pas le nombre de lignes du manifeste. Le fichier est tronqué ou le manifeste est faux. | Redéposez le jour avec `land.sh`. Rien n'a été écrit. |
| `ReconciliationError: <fichier>: manifest N, lines N, read M` | Le lecteur CSV a sauté des lignes. Il ignore une ligne vide ou faite d'espaces. | Corrigez le fichier en amont, puis redéposez-le. Rien n'a été écrit. |
| `ReconciliationError: <fichier>: lines N, written M` | La partition relue après écriture n'a pas le nombre de lignes du landing. | La partition est publiée mais fausse. Cherchez la cause dans les journaux, puis rejouez le jour. |
| `ValueError: bronze needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| `ValueError: unknown source '...'` | `--source` n'est pas une source de `SOURCES`. | Utilisez `orders`, `customers`, `products` ou `weblogs`. |
| Aucune ligne d'audit, code de sortie 2, journal `GTDP_PSEUDO_SALT is required` | Le sel n'est pas défini. | Exportez `GTDP_PSEUDO_SALT` ou ajoutez-le à `.env`. |

Si l'écriture d'une source échoue (quota, panne d'un nœud), les sources écrites avant elle ont
déjà leur nouvelle partition, les suivantes gardent l'ancienne. Rejouez le jour pour obtenir
le même état qu'une exécution réussie.

Les comptes d'une exécution échouée restent lisibles dans la table d'audit, au dernier tableau
du rapport. Une valeur nulle dans `rows_written` signale un échec avant écriture.
