# Runbook de la couche gold

Le job `gold` lit les tables silver et publie sous `/datalake/gold` le modèle en étoile
(`fact_sales`, `fact_web_events`, `dim_date`, `dim_customer`, `dim_product`) et neuf tables
d'indicateurs `kpi_*`. Les colonnes et les règles sont décrites dans le
[dictionnaire gold](../data-dictionary/gold.md). Les commandes se lancent depuis la racine du
dépôt, cluster démarré et silver du jour publié ([runbook silver](silver.md)).

## Ce que fait une exécution

Gold est une fonction des jours silver terminés : quel que soit l'ordre des exécutions, les
rejeux ou les corrections de silver, les mêmes jours silver terminés donnent le même gold.
`gold --date D` enchaîne, dans une seule application et une seule ligne d'audit :

1. **Garde.** Gold ne lit que les jours que silver a terminés, marqués par
   `silver/_days/ingestion_date=<jour>/_SUCCESS` (toutes les sources publiées et vérifiées,
   `gtdp.silver.days`). `D` doit en faire partie, sinon l'exécution échoue avant toute écriture.
   `D` déclenche l'exécution et sert de clé d'audit ; le contenu dépend de tous les jours
   terminés, pas de `D`. `silver/customers` et `silver/products_scd2`, reconstruites en entier,
   doivent avoir été écrites pour un jour terminé (`silver/_days/rebuilt/<source>.json`), sinon
   l'exécution échoue aussi avant toute écriture (`IncompleteSilverError`, voir « Dépanner »).
2. **Verrou.** L'exécution prend le verrou de toute la couche, `_gold.lock` à côté du répertoire
   gold (`/datalake/_gold.lock`), et le garde jusqu'à la fin. Une seconde exécution échoue
   aussitôt (`TableLockedError`), avant de lire quoi que ce soit.
3. **Calcul complet.** Les dimensions, `fact_sales` (toutes les commandes de tous les jours
   terminés, chacune au prix de la version `dim_product` telle que
   `valid_from <= event_time < valid_to`), `fact_web_events` et les neuf indicateurs sont
   calculés en entier et gardés en cache. Les indicateurs qui regroupent par pays ou par segment
   joignent `dim_customer` à ce moment : ce sont les valeurs actuelles du client.
4. **Contrôles avant toute écriture.** Chaque commande a exactement une version
   (`build_violations`), chaque table a une clé unique et aucune colonne obligatoire nulle, chaque
   fait pointe vers une ligne de dimension (`checks`). Au premier écart, l'exécution échoue sans
   rien écrire, l'état de publication compris.
5. **Plan.** Une empreinte par `order_date` (nombre de lignes et somme, indépendante de l'ordre,
   du SHA-256 de chaque ligne) compare le `fact_sales` calculé à celui publié : les dates qui
   diffèrent sont à réécrire, les dates publiées qui n'ont plus aucune commande sont à supprimer,
   les autres ne bougent pas. Un `fact_sales` écrit avec d'autres colonnes ou d'autres types (une
   version antérieure de gold) diffère sur toutes ses dates.
6. **Publication.** `gold/_state.json` passe à `PUBLISHING`. Les partitions à réécrire sont
   remplacées (`write_partitions`), celles à supprimer effacées (`drop_partitions`), les autres
   tables échangées en entier (`replace_table`,
   [ADR-0002](../adr/0002-parquet-idempotence.md)).
7. **Relecture.** `fact_sales` relu doit avoir, date par date, l'empreinte calculée ; sinon
   l'exécution échoue et l'état reste `PUBLISHING`. Sinon `gold/_state.json` passe à
   `PUBLISHED`.

`--force` n'a pas d'effet sur ce job.

**Coût.** Chaque exécution relit tout l'historique silver terminé et tout `fact_sales` publié :
elle est en O(historique), pas en O(jour). C'est acceptable à cette échelle (environ un million
de lignes de commande par an). L'alternative incrémentale, recalculer seulement les dates
touchées par le jour, n'est pas retenue : une date dépend aussi des versions SCD2 des produits et
des clients SCD1, qu'un jour silver peut changer pour des dates anciennes, et chaque oubli de
dépendance laisse gold en désaccord avec silver sans le signaler (constat des deux revues du lot).

Ligne d'audit :

| Champ | Contenu |
| --- | --- |
| `rows_in` | commandes silver de tous les jours terminés |
| `rows_out` | lignes de `fact_sales` relues après publication, égales à `rows_in` |
| `metrics_json.silver_days` | jours silver terminés que l'exécution a lus |
| `metrics_json.dates_rewritten`, `dates_removed`, `dates_unchanged` | nombre de dates `order_date` réécrites, supprimées, laissées telles quelles |
| `metrics_json.build_violations` | `no_version`, `several_versions` : lignes du calcul qui cassent le grain, toutes à 0 |
| `metrics_json.table_rows` | lignes des cinq tables de l'étoile |
| `metrics_json.checks` | `duplicate_key.<table>`, `null_required.<table>` et `orphan.<table>.<colonne>`, tous à 0 |
| `metrics_json.unmatched` | `unmatched.fact_web_events.customer_id` : client d'un log absent de `dim_customer`, information |
| `metrics_json.kpi_rows` | lignes de chaque indicateur |

## Faire confiance à gold

`gold/_state.json` dit si les tables gold forment un tout publié :

```bash
scripts/cluster/client.sh hdfs dfs -cat /datalake/gold/_state.json
```

```json
{"status": "PUBLISHED", "run_id": "20261127T101500Z-1a2b3c4d", "silver_days": ["2026-01-01", "2026-01-02", "2026-01-03"]}
```

Ne lisez gold (analyses, rapport, exports) que si `status` vaut `PUBLISHED`. `PUBLISHING` veut
dire qu'une exécution écrit en ce moment, ou qu'elle a été interrompue : les tables peuvent venir
de deux exécutions. `silver_days` donne les jours silver dont gold est la fonction. Un fichier
absent veut dire qu'aucune exécution de cette version de gold n'a publié.

## Traiter un jour

1. Lancez silver pour le jour, puis gold, un exécuteur si une autre application tourne. Gold
   ne hache rien : il ne lit pas le sel, `GTDP_PSEUDO_SALT` n'est pas nécessaire.

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh gold --date 2026-01-01
	```

2. Lisez le code de sortie : `0` succès, `1` échec du job (motif dans `error_message` de
   l'audit), `2` usage ou configuration invalide (`--date` absent, section `[gold]` absente,
   réglage hors bornes).

Deux réglages de `[gold]` dans `conf/app.toml` : `top_n` (5, de 1 à 100), le nombre de rangs du
classement des produits par pays, et `moving_average_days` (7, de 1 à 366), la fenêtre de la
moyenne mobile. Pour une exécution : `--set gold.top_n=10` ou `GTDP_GOLD__MOVING_AVERAGE_DAYS=30`.
Une valeur hors bornes arrête le job avec le code 2 avant le démarrage de Spark.

## Rejouer

Relancez la commande de n'importe quel jour terminé : gold est recalculé depuis tous les jours
terminés, une exécution sur un silver inchangé ne réécrit aucune date
(`dates_rewritten` = 0) et donne le même `content_sum` pour chaque table (rapport).

Après un rejeu de silver, relancez gold une fois, pour le dernier jour terminé. Un rejeu
`silver --date J1 --force` retire les marques des jours suivants : gold J1 reflète alors J1 seul,
`PUBLISHED` et cohérent, jusqu'au rejeu de silver J2 et J3 ; relancez gold J3 ensuite. Ne lancez
pas silver et gold en même temps : gold lit les tables silver non partitionnées (clients,
versions produit) qu'une exécution silver remplace.

## Corriger une commande déplacée ou retirée

Un dépôt corrigé qui change la date d'une commande, ou en retire une, se traite comme un rejeu :
rejouez bronze et silver du dépôt corrigé ([runbook silver](silver.md)), puis gold pour le
dernier jour terminé. Gold réécrit les dates dont le contenu change et supprime une date qui n'a
plus de commande (`dates_removed`). Aucune suppression à la main n'est nécessaire. Les tests
`test_a_corrected_drop_replayed_through_silver_and_gold_leaves_no_ghost_order`,
`test_gold_recovers_when_a_replayed_order_changes_its_date` et
`test_the_runbook_repair_of_an_order_moved_to_another_date_works` (`tests/gold/`) jouent cette
procédure.

## Vérifier gold

```bash
scripts/cluster/submit.sh src/gtdp/gold/report.py [--explain]
```

Le code de sortie vaut 1 si gold n'est pas `PUBLISHED`, si une date ou un invariant est en écart.
Le rapport affiche :

| Tableau | Valeur attendue |
| --- | --- |
| `gold/_state.json`, `pending`, `withdrawn` | `PUBLISHED`. `pending` : jours que silver a terminés après la dernière exécution gold (gold est en retard, relancez-le) ; `withdrawn` : jours dont gold est la fonction et que silver ne marque plus terminés (après un `--force`). Ni l'un ni l'autre n'est un écart. |
| Commandes par `order_date` (une colonne par dépôt, `drops`, `silver`, `fact_sales`, `status`) | `ok` partout : `fact_sales` = commandes silver de la date dans les jours de `silver_days`. Affiché pour les 7 dernières dates, les dates reçues dans plusieurs dépôts et toute date en écart, puis un résumé. |
| Invariants (`check`, `rows`, `status`) | `ok` (0) pour chaque clé, chaque colonne obligatoire et chaque référence obligatoire, `info` pour les clients des logs. |
| Gold contre silver (`check`, `silver`, `gold`, `status`) | `ok` partout. Silver est recalculé en une passe sur les jours de `silver_days` : nombre de commandes, `revenue_eur` au prix applicable, `invoiced_amount_eur` (formule appliquée directement aux commandes silver), commandes sans version valide ou avec plusieurs (0), couverture de `dim_date` sans trou, chiffre d'affaires `COMPLETED` égal au total de `kpi_revenue_daily`, entonnoir jamais croissant. |
| Top produits de FR (`plain_rank`, `plain_*`, `kpi_*`, `same`) | `true` partout : les produits dont le chiffre d'affaires est parmi les `top_n` plus élevés de FR, classés en Python simple (égalité = même rang), sont ceux de l'indicateur jusqu'au rang `top_n`. |
| Contenu (`table`, `rows`, `content_sum`) | inchangé après un rejeu. |
| Historique de `P00042` et ses commandes autour du changement de prix | avant le changement, `applicable_unit_price` vaut le prix de la version 1 ; après, celui de la version 2. |
| Échantillons d'indicateurs | top produits par pays, moyenne mobile des 8 premiers et 8 derniers jours de FR, écart de prix des catégories de P00042, puis chaque autre indicateur. |
| Exécutions gold de l'audit | `rows_in = rows_out`, `build_violations` à 0, `rewritten`, `removed`, `unchanged`. |
| Dernière ligne | `gold PUBLISHED, 0 order dates mismatched, 0 checks failed`. |
| `--explain` | le plan du calcul de `fact_sales` sur les jours de `silver_days`, avec la sélection de jointure par défaut puis avec `spark.sql.autoBroadcastJoinThreshold=-1`. |

Lecture du plan : par défaut, `products_scd2` (1 321 versions, 39 Ko) est diffusé
(`BroadcastHashJoin ... LeftOuter, BuildRight`) ; la condition d'intervalle
`valid_from <= event_time < valid_to` est évaluée sur les versions du même `product_id`, sans
échange des commandes. Sans diffusion, Spark trie les deux côtés par `product_id`
(`SortMergeJoin` précédé de `Exchange hashpartitioning(product_id, …)`) : `P00042`, 20 % des
lignes synthétiques, tombe dans une seule tâche. C'est le cas que le lot performance mesure.

## Après un arrêt brutal

Une exécution tuée (`kill -9`, panne du conteneur client, perte du pilote) laisse
`gold/_state.json` à `PUBLISHING`, son verrou `_gold.lock`, souvent le verrou de la table qu'elle
écrivait (`gold/_<table>.lock`) et un répertoire `gold/_<table>.staging-<id>`, et aucune ligne
d'audit. Ces verrous se libèrent d'eux-mêmes : la relance les reprend dès que YARN rapporte
terminée l'application qu'ils nomment, moins de 2 s après la mort du driver (`taking over the
lock` dans le journal). Relancez donc gold directement (étape 4). Si la relance échoue quand même
avec `TableLockedError`, suivez les étapes 1 à 3 avant de la refaire :

1. Lisez le verrou : il nomme l'application YARN qui le tient.

	```bash
	scripts/cluster/client.sh hdfs dfs -cat /datalake/_gold.lock
	```

	```json
	{"run_id": "20261127T101500Z-1a2b3c4d", "application_id": "application_1795715756506_0412", "locked_at": "2026-11-27T10:15:01+00:00", "token": "…"}
	```

2. Vérifiez que cette application est terminée : `State` doit valoir `FINISHED`, `FAILED` ou
   `KILLED`, jamais `RUNNING` ni `ACCEPTED`. Si elle tourne encore, attendez sa fin. Lisez
   `State`, pas `Final-State` : un pilote tué en mode client laisse une application
   `FINISHED` dont `Final-State` vaut `SUCCEEDED` (constaté sur le cluster). Si elle est
   terminée, le journal de la relance dit `no state from YARN` : le ResourceManager n'a pas
   répondu à trois essais, et sans l'étape 3 les verrous ne seraient repris qu'une fois plus
   vieux que `spark.gtdp.io.staleAfterSeconds` (6 h par défaut).

	```bash
	scripts/cluster/client.sh yarn application -status application_1795715756506_0412
	```

3. Supprimez les verrous de gold, et eux seuls :

	```bash
	scripts/cluster/client.sh hdfs dfs -rm -f /datalake/_gold.lock '/datalake/gold/_*.lock'
	```

4. Relancez gold pour le dernier jour terminé. Il reconstruit tout, réécrit ce que l'exécution
   tuée a laissé à moitié et repasse l'état à `PUBLISHED`. Les répertoires `_<table>.staging-*`
   de l'exécution tuée sont effacés par une exécution qui prend le verrou de la table plus de 6 h
   plus tard ; ils ne sont jamais lus.

Une exécution arrêtée par `SIGTERM` ou `SIGINT` (code 143 ou 130) libère ses verrous et écrit une
ligne d'audit `STOPPED` ; l'état reste `PUBLISHING` si elle avait commencé à écrire : relancez-la.

## Dépanner

| Message dans `error_message` | Cause | Action |
| --- | --- | --- |
| `FileNotFoundError: silver has not completed <D>: run silver for <D> first` | Silver n'a pas terminé ce jour (pas de `_SUCCESS` sous `silver/_days/ingestion_date=<D>`). | Lancez ou rejouez `silver --date <D>`, puis gold. Rien n'a été écrit. |
| `IncompleteSilverError: silver/<table> holds the rows of <D>, a day silver has not completed (published: ...; missing: ...). Complete the day with scripts/cluster/submit.sh silver --date <D>, then run gold again` | `customers` ou `products_scd2` tient les lignes de `<D>`, que silver n'a pas terminé : jour lancé source par source, exécution silver échouée ou arrêtée après l'échange de la table, rejeu d'un jour antérieur qui a retiré les marqueurs de `<D>`. Gold lirait ces lignes avec celles des jours terminés. | Terminez `<D>` : la commande du message, ou les sources de `missing` dans l'ordre `customers`, `products`, `orders`, `weblogs` ([runbook silver](silver.md#réparer-un-jour-incomplet)). Puis relancez gold. Rien n'a été écrit. |
| `MarkerError: .../silver/_days/... is unreadable (...): run silver --date <D> again ...` | Un fichier de `silver/_days` est illisible : abîmé, ou tronqué par une version antérieure de silver. | Lancez la commande silver du message, le jour entier : elle réécrit les marqueurs du jour. Puis relancez gold. Rien n'a été écrit. |
| `GoldCheckError: the gold built from silver breaks its invariants: {'no_version': N}` | Une commande tombe avant la première version de son produit ou dans un trou de l'historique. Silver vérifie seulement que le produit existe. | Vérifiez `products_scd2` avec le rapport silver (invariants SCD2). Rien n'a été écrit. |
| `GoldCheckError: ... {'several_versions': N, 'duplicate_key.fact_sales': N}` | Deux versions d'un produit se chevauchent. | Idem : historique SCD2 incohérent en silver. |
| `GoldCheckError: ... {'duplicate_key.fact_sales': N}` seul | Une transaction figure dans deux jours silver terminés avec deux dates. | Examinez la déduplication silver de ces jours ; rejouez silver puis gold. Rien n'a été écrit. |
| `GoldCheckError: ... {'orphan.fact_sales.customer_id': N}` | Une commande silver n'a pas de client dans silver `customers`. | Rejouez silver du jour, puis gold. Rien n'a été écrit. |
| `GoldCheckError: ... {'null_required.<table>': N}` | Une colonne obligatoire sort nulle du calcul (valeur nulle laissée par silver). | Examinez les lignes silver concernées. Rien n'a été écrit. |
| `GoldCheckError: fact_sales differs from the rows built from silver on [...]` | La relecture ne trouve pas ce qui a été écrit (écriture concurrente hors gold, panne pendant l'échange). | L'état reste `PUBLISHING` : relancez gold. |
| `TableLockedError: .../gold is being written by another run, which holds .../_gold.lock` | Une autre exécution gold tourne, ou une exécution tuée a laissé son verrou. | Si l'application du verrou tourne, attendez sa fin ; sinon suivez « Après un arrêt brutal ». Rien n'a été écrit. |
| `TableLockedError: .../gold/<table> is being written by another run, which holds .../gold/_<table>.lock` | Le verrou d'une table laissé par une exécution tuée. | « Après un arrêt brutal ». Rien n'a été écrit. |
| Code de sortie 2, journal `invalid configuration: gold needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| Code de sortie 2, journal `invalid configuration: the configuration has no [gold] section` | `conf/app.toml` sans section `[gold]`. | Restaurez la section (`top_n`, `moving_average_days`). |
| Code de sortie 2, journal `invalid configuration: [gold] top_n must be in 1..100, got N` | Réglage hors bornes (`--set`, variable `GTDP_GOLD__*` ou fichier). | Corrigez la valeur. |
