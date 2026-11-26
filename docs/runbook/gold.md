# Runbook de la couche gold

Le job `gold` lit les tables silver et publie sous `/datalake/gold` le modèle en étoile
(`fact_sales`, `fact_web_events`, `dim_date`, `dim_customer`, `dim_product`) et neuf tables
d'indicateurs `kpi_*`. Les colonnes et les règles sont décrites dans le
[dictionnaire gold](../data-dictionary/gold.md). Les commandes se lancent depuis la racine du
dépôt, cluster démarré et silver du jour publié ([runbook silver](silver.md)).

## Ce que fait une exécution

`gold --date D` enchaîne, dans une seule application et une seule ligne d'audit :

1. **Garde.** Gold ne lit que les jours que silver a terminés, marqués par
   `silver/_days/ingestion_date=<jour>/_SUCCESS` (toutes les sources publiées et vérifiées,
   `gtdp.silver.days`). `D` doit en faire partie, sinon l'exécution échoue avant toute écriture.
   Les partitions `ingestion_date` d'un jour silver incomplet (en cours ou échoué) sont ignorées
   de `orders`, `weblogs` et des rejets.
2. **Dimensions.** `dim_customer` (silver `customers`), `dim_product` (silver `products_scd2`) et
   `dim_date` (tous les jours entre la première et la dernière date des commandes et des logs
   silver) sont reconstruites et échangées (`replace_table`), puis relues.
3. **Fait des ventes.** Les dates de commande (`order_date`, jour UTC de `event_time`) présentes
   dans `silver/orders` de `D` sont recalculées à partir de **toutes** les commandes silver de ces
   dates, quel que soit leur jour silver terminé, puis seules ces partitions sont remplacées
   (`write_partitions`, [ADR-0002](../adr/0002-parquet-idempotence.md)). Une commande en retard
   de J2 datée du 2025-12-30 reconstruit le 2025-12-30 avec les commandes de J1 et de J2 ; les
   autres dates ne bougent pas. Chaque commande prend le prix catalogue de la version
   `dim_product` telle que `valid_from <= event_time < valid_to`.
4. **Contrôles avant écriture.** L'exécution échoue sans rien écrire dans `fact_sales` si une
   commande ne trouve aucune version, en trouve plusieurs, ou n'a pas de client dans
   `dim_customer` (`build_violations` de l'audit).
5. **Contrôle après écriture.** Les partitions relues doivent contenir exactement les commandes
   silver de leurs dates (`rows_out = rows_in`).
6. **Logs web.** `fact_web_events` est reconstruit à partir de tout `silver/weblogs`.
7. **Intégrité de l'étoile.** Clés uniques des cinq tables et aucun fait orphelin
   (`checks`, tous à 0). En cas d'écart, l'exécution échoue avant de toucher aux indicateurs.
8. **Indicateurs.** Les neuf tables `kpi_*` sont recalculées en entier et échangées.

Le job est un seul job plutôt qu'un job par table : les indicateurs lisent le fait et les
dimensions de la même exécution, et une seule ligne d'audit dit si toute la couche est cohérente.
L'ordre des jours n'a pas d'importance : comme chaque date est recalculée depuis tout silver,
rejouer J1 après J3 ne perd aucune commande. `--force` n'a pas d'effet sur ce job.

Ligne d'audit :

| Champ | Contenu |
| --- | --- |
| `rows_in` | commandes silver des dates recalculées |
| `metrics_json.silver_days` | jours silver terminés que l'exécution a lus |
| `rows_out` | lignes de `fact_sales` relues sur ces dates, égales à `rows_in` |
| `metrics_json.partitions_refreshed`, `order_dates` | nombre et liste des dates `order_date` remplacées |
| `metrics_json.build_violations` | `no_version`, `several_versions`, `unknown_customer` : lignes du calcul qui cassent le grain, toutes à 0 |
| `metrics_json.table_rows` | lignes des cinq tables de l'étoile |
| `metrics_json.checks` | `duplicate_key.<table>` et `orphan.<table>.<colonne>`, tous à 0 |
| `metrics_json.unmatched` | `unmatched.fact_web_events.customer_id` : client d'un log absent de `dim_customer`, information |
| `metrics_json.kpi_rows` | lignes de chaque indicateur |

## Traiter un jour

1. Exportez le sel : la configuration `cluster` l'exige, même si gold ne hache rien
   (valeur fournie hors dépôt, jamais commitée).

	```bash
	export GTDP_PSEUDO_SALT=<valeur>
	```

2. Lancez le job, idéalement dans l'ordre des jours :

	```bash
	scripts/cluster/submit.sh gold --date 2026-01-01
	scripts/cluster/submit.sh gold --date 2026-01-02
	scripts/cluster/submit.sh gold --date 2026-01-03
	```

	Si une autre application Spark tourne (le streaming, par exemple), limitez le job à un
	exécuteur :

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh gold --date 2026-01-01
	```

3. Lisez le code de sortie : `0` succès, `1` échec du job (motif dans `error_message` de
   l'audit), `2` configuration invalide ou sel absent.

Deux réglages de `[gold]` dans `conf/app.toml` : `top_n` (5), le nombre de rangs du classement
des produits par pays, et `moving_average_days` (7), la fenêtre de la moyenne mobile. Pour une
exécution : `--set gold.top_n=10` ou `GTDP_GOLD__MOVING_AVERAGE_DAYS=30`.

Deux exécutions gold ne doivent pas écrire les mêmes tables en même temps : la seconde échoue
sur le verrou de table (`TableLockedError`) sans rien modifier ; relancez-la après la première.

Durées relevées sur la machine de référence, profil `dev`, un exécuteur, le streaming et d'autres
applications tournant à côté (indicatives, pas des mesures de performance) : J1 (365 dates,
965 128 commandes) de 55 à 77 s (`duration_s`), J2 et J3 (4 dates, environ 10 400 commandes) de
32 à 69 s. Après J3, `/datalake/gold` occupe 51,6 Mo hors capteurs (155 Mo répliqués), dont
43,4 Mo pour `fact_sales` (une partition et un fichier par date).

## Rejouer un jour

Relancez la même commande. Les partitions `fact_sales` des dates du jour sont recalculées à
l'identique, les dimensions, `fact_web_events` et les indicateurs aussi. Le rapport le vérifie
avec `content_sum`. Après un rejeu de silver (même `--force`), relancez gold pour chaque jour
rejoué : une date de commande n'est recalculée que lorsqu'un jour qui la contient passe en gold.

Le prix appliqué à une commande est fixé quand sa partition est recalculée. Si silver reçoit plus
tard une version catalogue antidatée qui couvre cette commande (le contrat l'exclut : une version
est datée du jour qui précède son dépôt), rejouez gold pour le jour de la commande.

## Vérifier gold

```bash
scripts/cluster/submit.sh src/gtdp/gold/report.py [--explain 2026-01-01]
```

Le code de sortie vaut 1 si une date ou un invariant est en écart. Le rapport affiche :

| Tableau | Valeur attendue |
| --- | --- |
| Commandes par `order_date` (une colonne par dépôt, `drops`, `silver`, `fact_sales`, `status`) | `ok` partout : `fact_sales` = commandes silver de la date. `not built` pour une date qu'aucune exécution gold n'a encore couverte. Affiché pour les 7 dernières dates, les dates reçues dans plusieurs dépôts et toute date en écart, puis un résumé. |
| Invariants (`check`, `rows`, `status`) | `ok` (0) pour chaque clé et chaque référence obligatoire, `info` pour les clients des logs. |
| Gold contre silver (`check`, `silver`, `gold`, `status`) | `ok` partout. Silver est recalculé en une passe (commandes des jours terminés × versions × clients) : nombre de commandes, `revenue_eur` au prix applicable, `invoiced_amount_eur` (formule appliquée directement aux commandes silver), commandes sans version valide, avec plusieurs versions ou sans client (0), couverture de `dim_date` sans trou, chiffre d'affaires `COMPLETED` égal au total de `kpi_revenue_daily`, entonnoir jamais croissant. |
| Top produits de FR (`position`, `plain_*`, `kpi_*`, `same`) | `true` partout : le top N recalculé par un `groupBy` et un `orderBy` simples sur `fact_sales` donne les mêmes produits et montants que l'indicateur. |
| Contenu (`table`, `rows`, `content_sum`) | inchangé après un rejeu. |
| Historique de `P00042` et ses commandes autour du changement de prix | avant le changement, `applicable_unit_price` vaut le prix de la version 1 ; après, celui de la version 2. |
| Échantillons d'indicateurs | top produits par pays, moyenne mobile des 8 premiers et 8 derniers jours de FR, écart de prix des catégories de P00042, puis chaque autre indicateur. |
| Exécutions gold de l'audit | `rows_in = rows_out`, `build_violations` à 0. |
| Dernière ligne | `0 order dates mismatched, 0 checks failed`. |
| `--explain D` | le plan du calcul de `fact_sales` pour les dates de `D`, avec la sélection de jointure par défaut puis avec `spark.sql.autoBroadcastJoinThreshold=-1`. |

Lecture du plan : par défaut, `products_scd2` (1 321 versions, 39 Ko) et `dim_customer` sont
diffusés (`BroadcastHashJoin ... LeftOuter, BuildRight`) ; la condition d'intervalle
`valid_from <= event_time < valid_to` est évaluée sur les versions du même `product_id`, sans
échange des commandes. Sans diffusion, Spark trie les deux côtés par `product_id`
(`SortMergeJoin` précédé de `Exchange hashpartitioning(product_id, 16)`) : `P00042`, 20 % des
lignes synthétiques, tombe dans une seule tâche. C'est le cas que le lot performance mesure.

## Dépanner

| Message dans `error_message` | Cause | Action |
| --- | --- | --- |
| `FileNotFoundError: silver has not completed <D>: run silver for <D> first` | Silver n'a pas terminé ce jour (pas de `_SUCCESS` sous `silver/_days/ingestion_date=<D>`). | Lancez ou rejouez `silver --date <D>`, puis gold. Rien n'a été écrit. |
| `GoldCheckError: orders of <D> break the grain of fact_sales: {'no_version': N}` | Une commande tombe avant la première version de son produit ou dans un trou de l'historique. | Vérifiez `products_scd2` avec le rapport silver (invariants SCD2). Rien n'a été écrit dans `fact_sales` ; les dimensions sont déjà échangées. |
| `GoldCheckError: ... {'several_versions': N}` | Deux versions d'un produit se chevauchent. | Idem : historique SCD2 incohérent en silver. |
| `GoldCheckError: ... {'unknown_customer': N}` | Une commande silver n'a pas de client dans silver `customers`. | Rejouez silver du jour, puis gold. |
| `GoldCheckError: N silver orders on the refreshed dates, M rows in their fact_sales partitions` | La relecture ne trouve pas ce qui a été écrit (écriture concurrente, panne pendant l'échange). | Rejouez le jour. |
| `GoldCheckError: the gold star breaks its invariants: {'duplicate_key.fact_sales': N}` | Une commande figure sous deux dates : silver a changé une commande déjà publiée en gold. | Rejouez gold pour les jours concernés ; si l'écart reste, examinez silver. Les indicateurs n'ont pas été recalculés. |
| `GoldCheckError: ... {'orphan.fact_sales.customer_id': N}` | Une partition ancienne référence un client ou une version disparus de silver. | Rejouez gold pour les jours des dates concernées. |
| `TableLockedError` | Une autre exécution écrit la même table gold. | Attendez sa fin puis relancez. |
| `ValueError: gold needs --date` | `--date` manque. | Relancez avec `--date AAAA-MM-JJ`. |
| `ValueError: the configuration has no [gold] section` | `conf/app.toml` sans section `[gold]`. | Restaurez la section (`top_n`, `moving_average_days`). |
| Code de sortie 2, journal `GTDP_PSEUDO_SALT is required` | Le sel n'est pas défini. | Exportez `GTDP_PSEUDO_SALT` ou ajoutez-le à `.env`. |
