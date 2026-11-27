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
3. Le driver lit une fois les octets de chaque fichier. Il calcule leur SHA-256, compte les
   lignes de données (lignes physiques moins l'en-tête) et vérifie que le texte est en UTF-8
   strict. Un fichier CSV ne doit contenir aucun octet NUL.
4. Elle vérifie que l'en-tête de chaque fichier CSV nomme toutes les colonnes de sa source dans
   `SOURCES` (`src/gtdp/schemas.py`), sauf `discount_pct`, apparue en J3. Une colonne en plus
   est acceptée.
5. Elle compte les lignes que le lecteur CSV produit. Le lecteur utilise un schéma tout en
   `string` tiré de l'en-tête du fichier lui-même. Les guillemets sont désactivés : un `"`
   reste un caractère du champ.
6. Si un fichier n'a pas le SHA-256 de son manifeste, n'est pas en UTF-8, a un en-tête
   incomplet, ou si ses lignes, son manifeste et sa lecture ne concordent pas, elle échoue
   avant toute écriture.
7. Elle remplace la partition de la date dans chaque table bronze par `write_partitions` :
   verrou de la table, écriture complète dans un répertoire de transit, puis échange
   ([ADR-0002](../adr/0002-parquet-idempotence.md)). Les autres partitions ne changent pas.
8. Elle relit chaque partition écrite et compare les lignes de chaque fichier aux lignes du
   landing. Elle échoue si elles diffèrent.
9. Sans `--source`, elle supprime la partition de la date de chaque source que le manifeste ne
   liste pas. Le bronze du jour reflète alors le landing actuel.

L'exécution écrit une ligne dans la table d'audit, aussi en cas d'échec. `rows_in` compte les
lignes de données du landing, `rows_out` les lignes relues en bronze. `metrics_json` contient
les comptes de chaque fichier (`files`), de chaque source (`sources`), les colonnes inconnues
(`unknown_columns`) et, sans `--source`, les sources dont la partition a été supprimée
(`removed_sources`).

### Verrou et répertoires de transit

`write_partitions` prend le verrou de la table avant d'écrire : le fichier
`/datalake/bronze/_<source>.lock`, créé seulement s'il n'existe pas. Il contient le `run_id`
de l'exécution, l'identifiant de l'application YARN et l'heure de sa création. Une seconde
exécution qui écrit la même table échoue aussitôt avec `TableLockedError`, au lieu d'attendre.
Le verrou disparaît à la fin de l'écriture, aussi en cas d'échec ou d'arrêt par signal.

Le verrou d'une exécution morte se libère de lui-même : l'exécution suivante le reprend dès que
YARN rapporte terminée l'application qu'il nomme (`FINISHED`, `FAILED` ou `KILLED`), quel que
soit son âge, et l'écrit dans un avertissement du journal (`taking over the lock`). Tant que
l'application est dans un autre état (`RUNNING`, `ACCEPTED`…), le verrou est tenu. Si le
ResourceManager ne répond pas après trois essais (`no state from YARN` dans le journal), la règle
d'âge s'applique : le verrou est repris une fois plus vieux que `spark.gtdp.io.staleAfterSeconds`
(6 h par défaut). Le détenteur du verrou supprime aussi les répertoires de transit
`/datalake/bronze/_<source>.staging-<id>` plus vieux que ce délai, et les fichiers
`_<source>.lock.*` : la copie du verrou en cours d'écriture (`.tmp-<id>`) et la réclamation d'une
prise ou d'une libération (`.takeover-<empreinte>`). Ils n'existent que le temps de l'opération,
sauf si son exécution a été tuée. Une libération qui ne peut pas lire son verrou le garde et
l'écrit dans le journal (`could not read the lock`) : suivez alors
[Après une exécution tuée](#après-une-exécution-tuée) une fois l'application terminée.

Un SIGTERM ou un SIGINT reçu pendant l'échange des partitions prend effet à la fin de
l'échange : chaque partition est soit l'ancienne, soit la nouvelle.

## Ingérer un jour

1. Vérifiez que le jour est déposé :

	```bash
	scripts/cluster/client.sh hdfs dfs -ls /datalake/landing/_manifests/ingestion_date=2026-01-01
	```

2. Lancez le job (bronze ne pseudonymise rien : il n'a pas besoin de `GTDP_PSEUDO_SALT`) :

	```bash
	scripts/cluster/submit.sh bronze --date 2026-01-01
	```

	Pour une seule source, ajoutez `--source orders`. Si une autre application Spark tourne
	(le streaming, par exemple), limitez le job à un exécuteur :

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh bronze --date 2026-01-01
	```

3. Lisez le code de sortie. `0` signale un succès, `1` un échec du job, dont le motif est dans
   `error_message` de l'audit, et `2` une configuration ou un usage invalide (`--date` absent
   ou `--source` inconnue compris).

Sur la machine de référence, profil `dev` et un exécuteur, `duration_s` de l'audit vaut 31,2 s
pour J1 (1 106 403 lignes), 12,5 s pour J2 et 14,3 s pour J3 (3 075 lignes chacun). Le
streaming tournait pendant ces exécutions. La lecture des octets par le driver coûte environ
4 s pour J1 : 3,96 s pour les 68 Mo de `orders_20260101.csv` et 0,64 s pour les journaux web.
Sur une copie de J1, avec la même charge, le job prenait 14,6 s et 16,0 s avant cette lecture,
18,9 s et 21,7 s après.

## Rejouer un jour

Rejouer un jour relance la même commande. L'exécution remplace la partition de ce jour, et
elle seule, par le contenu du landing actuel. Si le nouveau dépôt n'a plus une source, la
partition de cette source pour ce jour disparaît. Les jours se rejouent dans n'importe quel
ordre. Rejouer J1 après J3 donne le même bronze. Ne lancez pas deux exécutions bronze en même
temps, même pour deux jours : elles écrivent les mêmes tables, et la seconde échoue sur le
verrou de la première table qu'elle veut écrire.

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
| Écarts landing et bronze, une ligne par source et par jour présent dans l'un ou l'autre (`landing_lines`, `bronze_rows`, `landing_not_in_bronze`, `bronze_not_in_landing` et leurs `_hashes`) | Pour une source CSV, 1 ligne absente du bronze, l'en-tête, dont `landing_not_in_bronze_hashes` donne le SHA-256. Pour `weblogs`, aucune. `bronze_not_in_landing` à 0 partout. |
| `discount_pct` des commandes | 0 valeur pour J1 et J2, des valeurs pour J3. |
| Exécutions bronze de l'audit, puis comptes par exécution et fichier | `manifest_rows`, `landing_lines`, `rows_read` et `rows_written` égaux pour chaque fichier d'une exécution `SUCCESS`. |

Le tableau des écarts ne montre jamais une ligne : il donne le nombre de lignes de chaque écart
et au plus 5 de leurs SHA-256, les plus petits (`--samples N` pour en voir plus). Une partition
bronze sans landing a `landing_lines` à 0 et `bronze_not_in_landing` égal à ses lignes. Pour
retrouver une ligne landing d'après son hash, remplacez `<hash>` :

```bash
scripts/cluster/client.sh bash -c 'hdfs dfs -cat /datalake/landing/orders/ingestion_date=2026-01-02/* | python3 -c "
import hashlib, sys
for line in sys.stdin.buffer:
	if hashlib.sha256(line.rstrip(b\"\n\")).hexdigest() == sys.argv[1]:
		sys.stdout.buffer.write(line)
" <hash>'
```

Une ligne bronze se retrouve par `_record_hash`, dans un job ou une session Spark.

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
| `LandingError: <fichier>: sha256 <landing>, manifest <manifeste>` | Les octets du fichier landing ne sont pas ceux que le générateur a comptés : fichier modifié, tronqué ou remplacé. | Redéposez le jour avec `land.sh`. Rien n'a été écrit. |
| `LandingError: <fichier>: not UTF-8 (line N)` | Le fichier n'est pas en UTF-8. Le lecteur aurait remplacé les octets invalides. | Corrigez l'encodage en amont, puis redéposez le jour. Rien n'a été écrit. |
| `LandingError: <fichier>: NUL byte, which the CSV reader takes for a quote (line N)` | Le fichier CSV contient un octet NUL. Le lecteur, guillemets désactivés, l'utilise comme guillemet. | Corrigez le fichier en amont, puis redéposez le jour. Rien n'a été écrit. |
| `LandingError: <fichier>: header lacks [...]` | L'en-tête ne nomme pas les colonnes listées : autre séparateur que `,`, en-tête absent ou renommé. | Corrigez le fichier en amont, puis redéposez le jour. Rien n'a été écrit. |
| `LandingError: the manifest of ... lists no file for [...]` | `--source` désigne une source que le manifeste du jour ne liste pas. | Vérifiez la source ou le dépôt. Sans `--source`, l'exécution supprime la partition bronze de cette source pour ce jour. |
| `TableLockedError: .../bronze/<source> is being written by another run, which holds .../_<source>.lock: {...}` | Une autre exécution écrit la même table. Le verrou donne son `run_id` et son application YARN. | Attendez la fin de cette exécution (`yarn application -list`), puis relancez. Si aucune ne tourne, suivez [Après une exécution tuée](#après-une-exécution-tuée). |
| `LandingError: the manifest of ... lists unknown sources [...]` | Le manifeste liste une source absente de `SOURCES` (`src/gtdp/schemas.py`). | Ajoutez la source à `SOURCES` ou corrigez le dépôt. |
| `ReconciliationError: <fichier>: manifest N, lines M, read M` | Le fichier landing n'a pas le nombre de lignes du manifeste. Le fichier est tronqué ou le manifeste est faux. | Redéposez le jour avec `land.sh`. Rien n'a été écrit. |
| `ReconciliationError: <fichier>: manifest N, lines N, read M` | Le lecteur CSV a sauté des lignes. Il ignore une ligne vide ou faite d'espaces. | Corrigez le fichier en amont, puis redéposez-le. Rien n'a été écrit. |
| `ReconciliationError: <fichier>: lines N, written M` | La partition relue après écriture n'a pas le nombre de lignes du landing. | La partition est publiée mais fausse. Cherchez la cause dans les journaux, puis rejouez le jour. |
| Code de sortie 2, `ConfigError: bronze needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| Code de sortie 2, `ConfigError: unknown source '...'` | `--source` n'est pas une source de `SOURCES`. | Utilisez `orders`, `customers`, `products` ou `weblogs`. Rien n'a été lu. |

Si l'écriture d'une source échoue (quota, panne d'un nœud), les sources écrites avant elle ont
déjà leur nouvelle partition, les suivantes gardent l'ancienne. Rejouez le jour pour obtenir
le même état qu'une exécution réussie.

Les comptes d'une exécution échouée restent lisibles dans la table d'audit, au dernier tableau
du rapport. Une valeur nulle dans `rows_written` signale un échec avant écriture.

### Après un arrêt par signal

Un SIGINT ou un SIGTERM reçu par le seul processus Python du driver arrête l'exécution avec le
code 130 ou 143 et une ligne d'audit `STOPPED`. Si le signal arrive pendant l'échange d'une table,
l'échange se termine d'abord. Chaque partition est l'ancienne ou la nouvelle, et aucun verrou ne
reste. Rejouez le jour pour que toutes les sources aient la nouvelle partition. Un Ctrl+C dans le
terminal de `submit.sh` atteint aussi la JVM du driver
([codes de sortie](exploitation.md#codes-de-sortie)).

### Après une exécution tuée

Un `kill -9`, ou la perte du conteneur client, tue le driver sans ligne d'audit. S'il était dans
un échange, la partition en cours peut manquer. Le verrou `_<source>.lock` et un répertoire
`_<source>.staging-<id>` restent dans `/datalake/bronze`.

YARN rapporte l'application terminée moins de 2 s après la mort du driver. Rejouez le jour : le
rejeu reprend le verrou de lui-même, avec dans son journal
`taking over the lock .../_orders.lock, left by a run that died (application_… is FINISHED)`.
Vérifiez ensuite le jour avec le rapport.

Si le rejeu échoue avec `TableLockedError` alors qu'aucune exécution ne tourne, le
ResourceManager n'a pas répondu à trois essais (`no state from YARN` dans le journal). Supprimez
alors le verrou à la main :

1. Vérifiez qu'aucune exécution bronze ne tourne :

	```bash
	scripts/cluster/client.sh yarn application -list
	```

2. Lisez le verrou. Son `application_id` ne doit plus être dans la liste :

	```bash
	scripts/cluster/client.sh hdfs dfs -cat /datalake/bronze/_orders.lock
	```

3. Supprimez le verrou :

	```bash
	scripts/cluster/client.sh hdfs dfs -rm /datalake/bronze/_orders.lock
	```

4. Rejouez le jour, puis vérifiez-le avec le rapport.

Sans l'étape 3 et sans réponse de YARN, l'exécution qui suit de plus de 6 heures la création du
verrou le reprend elle-même. La première exécution qui écrit la table plus de 6 heures après le
dernier changement du répertoire de transit le supprime. Pour essayer ces reprises sur une copie,
réduisez le délai :

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.gtdp.io.staleAfterSeconds=60" \
	scripts/cluster/submit.sh bronze --date 2026-01-02 \
	--set paths.bronze=/user/etl/copie/bronze --set paths.ops=/user/etl/copie/ops
```
