# Runbook de la couche silver

Le job `silver` lit les tables bronze d'une date et publie sous `/datalake/silver` les tables
`orders`, `customers`, `products_scd2`, `weblogs` et les rejets `rejects/<source>`. Les colonnes
et les règles sont décrites dans le [dictionnaire silver](../data-dictionary/silver.md). Les
commandes se lancent depuis la racine du dépôt, cluster démarré et bronze du jour écrit
([runbook bronze](bronze.md)).

## Ce que fait une exécution

1. **Garde d'ordre.** Pour chaque source traitée, elle refuse la date si
   `silver/rejects/<source>` contient déjà une partition postérieure, sauf `--force`
   ([ADR-0005](../adr/0005-silver-incremental.md)). Chaque exécution écrit la partition de
   rejets du jour, même vide : c'est la trace des jours traités.
2. **Lecture.** Elle compte les lignes de la partition bronze du jour, à part. Elle lit ensuite
   bronze avec le schéma canonique : la partition du jour pour `orders` et `weblogs`, tous les
   jours jusqu'à la date pour `customers` et `products`, dont les tables sont reconstruites.
   Une colonne inconnue d'un fichier est notée dans l'audit. Une partition bronze absente fait
   échouer l'exécution avant toute écriture.
3. **Classement en une passe.** Doublons exacts, codes de rejet et drapeaux calculés en une
   projection à partir du registre de règles, doublons de clé, doublons inter-jours (clés des
   partitions `orders` antérieures à la date). Le résultat est persisté (`MEMORY_AND_DISK`) :
   les comptes, les rejets et la table le relisent en mémoire au lieu de relire bronze, comme
   le montre `--explain` du rapport.
4. **Écriture.** `write_partitions` remplace la partition du jour de `rejects/<source>`, puis
   celle de `orders` ou `weblogs`. `replace_table` échange `customers` ou `products_scd2` en
   entier ([ADR-0002](../adr/0002-parquet-idempotence.md)). L'email et l'IP sont hachés avec
   le sel, passé comme une donnée.
5. **Vérification.** Elle relit ce qu'elle a écrit et échoue si `bronze ≠ propres + rejets +
   doublons`, si les rejets ou la partition relus diffèrent des comptes, si la clé de la table
   n'est plus unique, ou si `products_scd2` viole un invariant SCD2.

Sans `--source`, les quatre sources sont traitées l'une après l'autre dans la même exécution.
L'exécution écrit une ligne d'audit, même en cas d'échec :

| Champ | Contenu |
| --- | --- |
| `rows_in` | lignes bronze du jour, toutes sources traitées |
| `rows_out`, `rows_rejected`, `rows_duplicated` | lignes propres, rejetées, en doublon ; leur somme vaut `rows_in` |
| `metrics_json.sources.<source>` | `rows_in`, `clean`, `rejected`, `duplicated`, `measures` (par disposition, par code de rejet `rejected.<CODE>`, par drapeau `flagged.<CODE>`), `rejects_written`, `rows_written`, `violations`, `unknown_columns` |
| `metrics_json.unknown_columns` | colonnes inconnues par source ([ADR-0007](../adr/0007-evolution-schema.md)) |

## Traiter un jour

1. Exportez le sel, exigé par tout job `--env cluster` (valeur fournie hors dépôt, jamais
   commitée) :

	```bash
	export GTDP_PSEUDO_SALT=<valeur>
	```

2. Lancez le job, les jours dans l'ordre :

	```bash
	scripts/cluster/submit.sh silver --date 2026-01-01
	scripts/cluster/submit.sh silver --date 2026-01-02
	scripts/cluster/submit.sh silver --date 2026-01-03
	```

	Pour une seule source, ajoutez `--source orders`. Si une autre application Spark tourne,
	limitez le job à un exécuteur :

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh silver --date 2026-01-01
	```

3. Lisez le code de sortie : `0` succès, `1` échec du job (motif dans `error_message` de
   l'audit), `2` configuration invalide ou sel absent.

Mesures sur la machine de référence, profil `dev`, un exécuteur, quatre sources : J1
(1 106 403 lignes bronze) prend 32,5 s (`duration_s`, aucune autre application en cours), 46 s
de commande complète ; J2 et J3 (3 075 lignes chacun) prennent de 18 à 25 s, une autre
application tournant à côté. Après J3, silver occupe 100,3 Mo (301,0 Mo répliqués), dont
84,5 Mo pour `orders`.

## Rejouer un jour

Relancez la même commande. La partition du jour de `orders`, `weblogs` et de chaque
`rejects/<source>` est remplacée, les autres ne changent pas ; `customers` et `products_scd2`
sont reconstruites à l'identique. Les doublons inter-jours ne regardent que les partitions
antérieures : rejouer J2 donne le même contenu que la première fois. Le rapport le vérifie avec
`content_sum`.

Rejouer un jour antérieur au dernier jour traité est refusé :

```
LaterPartitionError: hdfs://namenode:8020/datalake/silver/rejects/orders already holds
ingestion_date=2026-01-03, later than 2026-01-02; replay the days in order or force it
```

Pour corriger un jour passé, forcez-le puis rejouez les jours suivants dans l'ordre. Entre les
deux, `customers` et `products_scd2` reflètent l'état du jour forcé.

```bash
scripts/cluster/submit.sh silver --date 2026-01-02 --force
scripts/cluster/submit.sh silver --date 2026-01-03
```

## Vérifier silver

```bash
scripts/cluster/submit.sh src/gtdp/silver/report.py [--customer C003921] [--explain orders --date 2026-01-02]
```

Le code de sortie vaut 1 si une mesure diffère. Le rapport affiche :

| Tableau | Valeur attendue |
| --- | --- |
| Réconciliation (`source`, `ingestion_date`, `measure`, `manifest`, `audit`, `table`, `status`) | `ok` partout. `manifest` : compte prévu par le manifeste selon la table de disposition (`DISPOSITION_TABLE` de `src/gtdp/silver/reconcile.py`, 2 lignes par `CONFLICTING_VERSION`). `audit` : dernière exécution silver réussie du jour. `table` : rejets relus par code, lignes de la partition pour `orders` et `weblogs`. `info` pour une mesure que le manifeste ne prévoit pas (`flagged.FUTURE_SIGNUP`), `no run` pour un jour déposé que silver n'a pas encore traité. |
| Contenu (`table`, `ingestion_date`, `rows`, `content_sum`) | `content_sum` inchangé après un rejeu ; `_run_id` des rejets exclu. |
| Exécutions silver de l'audit | `rows_in = rows_out + rows_rejected + rows_duplicated` pour chaque `SUCCESS`. |
| Historique de `P00042` et prix à la date de commande | au moins deux versions ; les commandes avant le changement prennent le prix de la version 1. |
| `--customer` | les lignes bronze du client (nom, email, téléphone en clair) puis sa ligne silver (`email_hash`, sans nom ni téléphone). |
| `--explain` | le plan du classement (lecture des Parquet bronze), puis ceux des rejets et des lignes propres, qui lisent `InMemoryTableScan`. |

Résultat après J1, J2 (rejoué), J3 sur le profil `dev` : 108 mesures, 105 `ok`, 3 `info`,
aucun écart.

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
| `LaterPartitionError: ... already holds ingestion_date=<D+n>, later than <D>` | Un jour postérieur est déjà publié. | Rejouez dans l'ordre, ou `--force` puis les jours suivants. Rien n'a été écrit. |
| `FileNotFoundError: .../bronze/<source>/ingestion_date=<D> not found: run bronze for <D> first` | Bronze n'a pas traité ce jour pour cette source. | Lancez `bronze --date <D>`, puis silver. Les sources traitées avant celle-ci sont publiées. |
| `SilverCheckError: <source> <D>: N bronze records, but clean ... = M` | Des lignes ont été perdues ou comptées deux fois. | Bug : gardez les journaux, ouvrez un ticket. Les tables du jour sont publiées mais fausses. |
| `SilverCheckError: ... N rejected, M in silver/rejects` ou `N clean, M in the silver partition` | La relecture ne trouve pas ce qui a été écrit (écriture concurrente, panne pendant l'échange). | Rejouez le jour. |
| `SilverCheckError: ... N rows break duplicate_key` | Clé en double dans la table, par exemple après un jour traité avec `--force` sans rejouer les suivants. | Rejouez les jours suivants dans l'ordre. |
| `SilverCheckError: ... rows break overlap_or_gap` (ou un autre invariant SCD2) | Historique produit incohérent. | Bug : ouvrez un ticket avec le rapport. |
| `ValueError: silver needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| `ValueError: unknown source [...]` | `--source` inconnue. | Utilisez `orders`, `customers`, `products` ou `weblogs`. |
| Code de sortie 2, journal `GTDP_PSEUDO_SALT is required` | Le sel n'est pas défini. | Exportez `GTDP_PSEUDO_SALT` ou ajoutez-le à `.env`. |

Les comptes d'une exécution échouée restent dans `metrics_json` de sa ligne d'audit. Changer
le sel change tous les `email_hash` et `ip_hash` : retraitez alors tous les jours dans l'ordre.
