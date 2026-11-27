# Runbook de la couche silver

Le job `silver` lit les tables bronze d'une date et publie sous `/datalake/silver` les tables
`orders`, `customers`, `products_scd2`, `weblogs` et les rejets `rejects/<source>`. Les colonnes
et les règles sont décrites dans le [dictionnaire silver](../data-dictionary/silver.md). Les
commandes se lancent depuis la racine du dépôt, cluster démarré et bronze du jour écrit
([runbook bronze](bronze.md)).

## Ce que fait une exécution

1. **Sel.** Sur le cluster, elle refuse un sel vide, de moins de 16 caractères ou égal au sel
   de développement du dépôt. Elle compare ensuite l'empreinte du sel,
   `sha256("gtdp-salt-fingerprint" ‖ sel)`, à celle que la première exécution a écrite dans
   `<ops>/pseudonymization/salt_fingerprint.json`, et refuse un autre sel. Le fichier ne
   contient que l'empreinte, jamais le sel.
2. **Ordre des jours.** Elle refuse la date, sauf `--force`, si un jour antérieur présent en
   bronze n'est pas complet en silver, ou si un jour postérieur est complet
   ([ADR-0005](../adr/0005-silver-incremental.md)). Un jour est complet quand ses quatre
   sources sont publiées (voir « Jours publiés »).
3. **Dépendances.** `orders` contrôle ses `customer_id` et `product_id` contre `customers` et
   `products_scd2` telles que publiées pour la même date. `--source orders` est refusé tant que
   `customers` et `products` ne sont pas publiées pour cette date. Sans `--source`, les sources
   passent dans cet ordre : `customers`, `products`, `orders`, `weblogs`.
4. **Invalidation.** Avant toute écriture, l'exécution retire les marqueurs des jours
   postérieurs (ils deviennent incomplets), le `_SUCCESS` du jour et les marqueurs des sources
   traitées et de celles qui en dépendent : rejouer `customers` rend `orders` à refaire pour le
   jour.
5. **Classement en une passe.** Doublons exacts, codes de rejet et drapeaux en une projection
   à partir du registre de règles, doublons de clé, doublons inter-jours (clés des partitions
   des jours complets antérieurs, pour `orders` et `weblogs`). Le résultat est persisté
   (`MEMORY_AND_DISK`) : les comptes, les contrôles, les rejets et la table le relisent en
   mémoire, comme le montre `--explain` du rapport.
6. **Contrôles avant écriture.** Sur les lignes à publier : `bronze = propres + rejets +
   doublons`, autant de lignes à publier que de propres pour une table partitionnée, clé unique
   sur les jours complets et le jour, invariants SCD2 de `products_scd2`. Un échec arrête la
   source sans rien écrire (`nothing published`).
7. **Écriture.** `write_partitions` remplace la partition du jour de `rejects/<source>`, puis
   celle de `orders` ou `weblogs`. `replace_table` échange `customers` ou `products_scd2` en
   entier ([ADR-0002](../adr/0002-parquet-idempotence.md)), avec le fichier `_built_for.json` qui
   nomme le jour : gold ne lit la table que si ce jour et ceux de ses lignes sont complets.
8. **Vérification après écriture.** Rejets et lignes relus égaux aux comptes, clé unique,
   invariants SCD2. En cas d'échec, l'exécution retire ce qu'elle a publié pour la source et le
   jour (`publication removed`) : les partitions du jour, ou la table reconstruite remise dans
   son état du dernier jour antérieur publié (vide s'il n'y en a pas), avec un `_built_for.json`
   qui nomme ce jour.
9. **Marqueurs.** La source vérifiée reçoit son marqueur ; le jour reçoit `_SUCCESS` quand les
   quatre sources ont le leur.

Chaque exécution écrit une ligne d'audit, même en cas d'échec :

| Champ | Contenu |
| --- | --- |
| `rows_in` | lignes bronze du jour, toutes sources traitées |
| `rows_out`, `rows_rejected`, `rows_duplicated` | lignes propres, rejetées, en doublon ; leur somme vaut `rows_in` |
| `metrics_json.sources.<source>` | `rows_in`, `clean`, `rejected`, `duplicated`, `measures` (par disposition, par code de rejet `rejected.<CODE>`, par drapeau `flagged.<CODE>`), `rows_built` (lignes à publier), `violations` (avant écriture), `rejects_written`, `rows_written`, `written_violations` (après écriture), `rolled_back`, `unknown_columns` |
| `metrics_json.unknown_columns` | colonnes inconnues par source ([ADR-0007](../adr/0007-evolution-schema.md)) |

## Jours publiés

```
/datalake/silver/_days/ingestion_date=2026-01-02/customers.json   {"source", "run_id", "published_at"}
/datalake/silver/_days/ingestion_date=2026-01-02/products.json
/datalake/silver/_days/ingestion_date=2026-01-02/orders.json
/datalake/silver/_days/ingestion_date=2026-01-02/weblogs.json
/datalake/silver/_days/ingestion_date=2026-01-02/_SUCCESS          {"run_id", "sources": {source: run_id}}
/datalake/silver/customers/_built_for.json                          {"day", "run_id"}
/datalake/silver/products_scd2/_built_for.json
```

Un lecteur qui doit voir des jours entiers (gold) ne lit des tables partitionnées que les
partitions des jours complets : `gtdp.silver.days.complete_days(spark, silver)`. Un jour sans
`_SUCCESS` est en cours de traitement, échoué ou rendu incomplet par le rejeu d'un jour
antérieur. Le `_built_for.json` de `customers` ou de `products_scd2` nomme le jour pour lequel
la table a été reconstruite, et entre avec elle à l'échange : gold refuse de s'exécuter tant que
ce jour, ou celui d'une ligne de la table, n'est pas complet. Chaque fichier est écrit sous un nom
temporaire puis renommé à sa place : il n'est jamais tronqué.

## Traiter un jour

1. Exportez le sel, exigé par silver sur le cluster (valeur fournie hors dépôt, jamais
   commitée, au moins 16 caractères, toujours la même) :

	```bash
	export GTDP_PSEUDO_SALT=<valeur>
	```

2. Lancez le job, les jours dans l'ordre :

	```bash
	scripts/cluster/submit.sh silver --date 2026-01-01
	scripts/cluster/submit.sh silver --date 2026-01-02
	scripts/cluster/submit.sh silver --date 2026-01-03
	```

	Pour une seule source, ajoutez `--source <source>`, dans l'ordre `customers`, `products`,
	`orders`, `weblogs`. Si une autre application Spark tourne, limitez le job à un exécuteur :

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh silver --date 2026-01-01
	```

3. Lisez le code de sortie : `0` succès, `1` échec du job (motif dans `error_message` de
   l'audit), `2` usage ou configuration invalide (`--date` absent, `--source` inconnue, sel
   absent).

Mesures sur la machine de référence, profil `dev`, un exécuteur, l'application de streaming
`gtdp-streaming-sensors` tournant à côté (`duration_s` de l'audit) : J1 (1 106 403 lignes
bronze) 71,2 s, J2 53,5 s, rejeu de J2 61,6 s, J3 67,6 s. J2 lance 115 jobs Spark, dont 8,2 s
pour les deux contrôles d'unicité de clé sur les 965 000 commandes de J1, avant et après
l'écriture ; la version précédente en lançait 96 et durait 19,1 s. Après J3, silver occupe 38,8 Mo
(116,5 Mo répliqués), dont 23,3 Mo pour `orders` : la colonne `_record_hash` (64 caractères
hexadécimaux par commande, incompressibles) n'y est plus.

## Rejouer un jour

Le dernier jour complet se rejoue avec la même commande. La partition du jour de `orders`,
`weblogs` et de chaque `rejects/<source>` est remplacée, les autres ne changent pas ;
`customers` et `products_scd2` sont reconstruites à l'identique. Les doublons inter-jours ne
regardent que les jours complets antérieurs : rejouer J2 donne le même contenu que la première
fois. Le rapport le vérifie avec `content_sum`.

Rejouer un jour antérieur au dernier jour complet est refusé :

```
LaterPartitionError: silver already holds the complete day 2026-01-03, later than 2026-01-02;
replay the days in order or force it, then replay the later days
```

Pour corriger un jour passé, forcez-le : les jours suivants deviennent incomplets, le job
refuse tout autre ordre que leur rejeu un par un. Entre les deux, `customers` et
`products_scd2` reflètent l'état du jour forcé et le rapport sort en 1.

```bash
scripts/cluster/submit.sh silver --date 2026-01-02 --force
scripts/cluster/submit.sh silver --date 2026-01-03
```

Il n'y a pas d'option `--cascade` : chaque jour rejoué garde sa ligne d'audit, que le rapport
compare au manifeste du jour.

## Réparer un jour incomplet

Un jour reste incomplet après l'échec d'une source pendant son traitement (contrôle, panne
d'écriture), un arrêt (`SIGTERM`, code 143), un `kill -9`, ou le rejeu forcé d'un jour antérieur :
son `_SUCCESS` manque, le jour suivant est refusé avec
`DayOrderError: <D> is not complete in silver`, le rapport le liste et sort en 1. Si
`customers` ou `products_scd2` a été échangée pour le jour, gold refuse aussi de s'exécuter
(`IncompleteSilverError`) jusqu'à ce que le jour soit complet ; si le jour ne peut pas l'être,
`silver --date <dernier jour complet>` reconstruit les deux tables pour ce jour. Une écriture qui
échoue avant l'échange laisse la table d'avant, que gold lit. Les refus d'avant écriture (sel,
ordre, dépendances, bronze absent) ne changent rien.

1. Repérez les sources publiées et manquantes : tableau des jours du rapport, ou
   `hdfs dfs -ls /datalake/silver/_days/ingestion_date=<D>`.
2. Corrigez la cause (motif dans `error_message`, journaux de l'application). Après un
   `kill -9`, le verrou `_<table>.lock` de la table en cours d'écriture reste. Le rejeu le
   reprend de lui-même dès que YARN rapporte terminée l'application qu'il nomme, moins de 2 s
   après la mort du driver (`taking over the lock` dans le journal). S'il échoue quand même avec
   `TableLockedError`, le ResourceManager n'a pas répondu à trois essais (`no state from YARN`
   dans le journal) et le verrou ne serait repris qu'à 6 h : vérifiez que l'application est terminée
   (`yarn application -status <application_id>`), puis supprimez le verrou :

	```bash
	scripts/cluster/client.sh hdfs dfs -rm /datalake/silver/_orders.lock
	```

3. Rejouez le jour entier, ou seulement les sources manquantes dans l'ordre des dépendances :

	```bash
	scripts/cluster/submit.sh silver --date <D>
	```

4. Relancez le rapport : le jour doit être complet, toutes les mesures `ok`.

Un échec de contrôle après écriture a déjà retiré ce que la source avait publié pour le jour.
Après un `kill -9`, les sources marquées sont publiées et vérifiées, la source interrompue garde
sa partition précédente ou aucune ; le rejeu du jour la remplace. Un répertoire
`_<table>.staging-*` laissé par un `kill -9` est supprimé par l'écriture suivante de la table
une fois plus vieux que `spark.gtdp.io.staleAfterSeconds` (6 h par défaut).

## Changer le sel

Changer le sel change tous les `email_hash`, `ip_hash` et `log_id` : silver se reconstruit en
entier, puis gold. Le job refuse un autre sel tant que l'empreinte de l'ancien est enregistrée :

```
SaltError: GTDP_PSEUDO_SALT is not the salt silver was built with, whose fingerprint run <id>
recorded in .../pseudonymization/salt_fingerprint.json. Export that salt; to change it, rebuild
silver whole (docs/runbook/silver.md, « Changer le sel »)
```

1. Arrêtez les traitements silver et gold.
2. Supprimez les tables silver, les marqueurs et l'empreinte. `silver/rejects` et son ACL
   restent : chaque jour rejoué remplace ses partitions.

	```bash
	scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash \
		/datalake/silver/orders /datalake/silver/customers /datalake/silver/products_scd2 \
		/datalake/silver/weblogs /datalake/silver/_days \
		/datalake/ops/pseudonymization/salt_fingerprint.json
	```

3. Exportez le nouveau sel, rejouez chaque jour dans l'ordre (« Traiter un jour »), puis gold.

## Vérifier silver

```bash
scripts/cluster/submit.sh src/gtdp/silver/report.py [--customer C003921] [--explain orders --date 2026-01-02]
```

Le code de sortie vaut 1 si une mesure diffère, si une mesure n'a pas d'exécution (`no run`) ou
si un jour déposé est incomplet. Le rapport affiche :

| Tableau | Valeur attendue |
| --- | --- |
| Réconciliation (`source`, `ingestion_date`, `measure`, `manifest`, `audit`, `table`, `status`) | `ok` partout. `manifest` : compte prévu par le manifeste selon la table de disposition (`DISPOSITION_TABLE` de `src/gtdp/silver/reconcile.py`, 2 lignes par `CONFLICTING_VERSION`). `audit` : dernière exécution silver réussie du jour. `table` : rejets relus par code, lignes de la partition pour `orders` et `weblogs`. `info` pour un drapeau que le manifeste ne prévoit pas (`flagged.FUTURE_SIGNUP`, `flagged.ANCIENT_SIGNUP`), `no run` pour une source d'un jour déposé que silver n'a pas traitée. Un rejet que le générateur ne produit jamais (`rejected.ANCIENT_TIMESTAMP`, `rejected.FUTURE_TIMESTAMP` des logs) sort en écart, manifeste à 0 : la ligne reçue est dans `silver/rejects/<source>` avec son code, le jour est publié. |
| Jours (`ingestion_date`, `complete`, `published`, `missing`) | `complete` vrai pour chaque jour déposé ; `published` donne l'exécution de chaque source. |
| Contenu (`table`, `ingestion_date`, `rows`, `content_sum`) | `content_sum` inchangé après un rejeu ; `_run_id` des rejets exclu. |
| Exécutions silver de l'audit | `rows_in = rows_out + rows_rejected + rows_duplicated` pour chaque `SUCCESS`. |
| Historique de `P00042` et prix à la date de commande | au moins deux versions ; les commandes avant le changement prennent le prix de la version 1. |
| `--customer` | les lignes bronze du client sans donnée personnelle (longueur du nom, de l'email et du téléphone, `email_hash` de l'email bronze calculé avec le sel), puis sa ligne silver : le même `email_hash`. |
| `--explain` | le plan du classement (lecture des Parquet bronze), puis ceux des rejets et des lignes propres, qui lisent `InMemoryTableScan`. |

Le rapport n'affiche aucun nom, email, téléphone ni adresse IP : en mode cluster, sa sortie
standard est servie par le JobHistory sans authentification.

Pour vérifier qu'aucun plan servi par le History Server ne contient le sel :

```bash
scripts/cluster/client.sh python3 -c '
import os, urllib.request
app = "application_<id>"
base = f"http://spark-history:18080/api/v1/applications/{app}/"
text = "".join(urllib.request.urlopen(base + e).read().decode()
               for e in ("sql?details=true&planDescription=true", "environment", "jobs"))
print(text.count(os.environ["GTDP_PSEUDO_SALT"]), "occurrences du sel")'
```

## Dépanner

| Message dans `error_message` | Cause | Action |
| --- | --- | --- |
| `SaltError: GTDP_PSEUDO_SALT is blank`, `... is the development salt ...`, `... holds N characters, at least 16 needed` | Sel inutilisable sur le cluster. | Exportez le vrai sel. Rien n'a été écrit. |
| `SaltError: GTDP_PSEUDO_SALT is not the salt silver was built with ...` | Autre sel que celui de l'empreinte. | Exportez le sel d'origine, ou « Changer le sel ». Rien n'a été écrit. |
| `DayOrderError: <D-n> is not complete in silver, which <D> deduplicates against ...` | Un jour antérieur est incomplet. | « Réparer un jour incomplet » pour ce jour, puis `<D>`. Rien n'a été écrit. |
| `DayOrderError: orders of <D> are checked against customers as published for <D>, but customers is published for <X> ...` | `--source orders` avant `customers` ou `products` du jour. | Lancez la source nommée pour `<D>`, ou le jour entier. Rien n'a été écrit. |
| `LaterPartitionError: silver already holds the complete day <D+n>, later than <D> ...` | Un jour postérieur est complet. | Rejouez dans l'ordre, ou `--force` puis les jours suivants. Rien n'a été écrit. |
| `FileNotFoundError: .../bronze/<source>/ingestion_date=<D> not found: run bronze for <D> first` | Bronze n'a pas traité ce jour pour cette source. | Lancez `bronze --date <D>`, puis silver. Rien n'a été écrit. |
| `SilverCheckError: <source> <D>, nothing published: ...` | Un contrôle a échoué avant l'écriture (comptes, clé, SCD2). | Bug : gardez les journaux, ouvrez un ticket. Les sources précédentes du jour sont publiées, le jour est incomplet. |
| `SilverCheckError: <source> <D>, publication removed: ...` | La relecture diffère de ce qui a été écrit (écriture concurrente, panne pendant l'échange). | Ce que la source avait publié pour le jour est retiré. Rejouez le jour. |
| `MarkerError: .../_days/ingestion_date=<D>/<source>.json is unreadable (...): run silver --date <D> again ...` | Un marqueur est illisible : abîmé, ou tronqué par une version antérieure du job. Une exécution qui doit le lire s'arrête au lieu de le prendre pour absent. | Rejouez le jour entier, `silver --date <D>` : il retire puis réécrit les marqueurs du jour. Pour `_days/rebuilt/<source>.json`, rejouez le jour que le message nomme. |
| `TableLockedError: ... is being written by another run` | Une autre exécution écrit la table. | Attendez sa fin : le verrou se libère dès que YARN la rapporte terminée. S'il reste alors que l'application est terminée (`no state from YARN` dans le journal), supprimez le verrou nommé. |
| Code de sortie 2, `ConfigError: silver needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| Code de sortie 2, `ConfigError: unknown source ...` | `--source` inconnue. | Relancez avec `--source` parmi `customers`, `products`, `orders`, `weblogs`. |
| Code de sortie 2, journal `GTDP_PSEUDO_SALT is required` | Le sel n'est pas défini. | Exportez `GTDP_PSEUDO_SALT` ou ajoutez-le à `.env`. |

Les comptes d'une exécution échouée restent dans `metrics_json` de sa ligne d'audit.
