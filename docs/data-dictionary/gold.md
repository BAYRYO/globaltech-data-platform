# Dictionnaire de données gold

Le job `gold` publie sous `/datalake/gold` le modèle en étoile des ventes et des logs web, puis
neuf tables d'indicateurs, en Parquet (snappy). L'ACL par défaut de `/datalake/gold` donne la
lecture au groupe `analysts` sur chaque table. L'exploitation (exécution, rejeu, vérification)
est décrite dans le [runbook gold](../runbook/gold.md), les choix de modèle dans le
[modèle de données](../data-model.md#gold--modèle-en-étoile).

Chaque exécution recalcule toutes les tables, contrôle tout, puis publie. Elle lit les commandes,
les logs et les rejets des jours silver terminés, et les tables `silver/customers` et
`silver/products_scd2` telles que silver les a publiées en dernier, que leur jour soit terminé ou
non. `_state.json`, à la racine de `/datalake/gold`, dit si les tables forment un tout publié
(voir [`gold/_state.json`](#gold_statejson)) : ne lisez gold que s'il vaut `PUBLISHED`.

Limite connue (KAN-58) : un jour silver en cours qui a déjà republié `customers` ou
`products_scd2` change `dim_customer`, `dim_product` et les indicateurs qui les joignent, sans
changer `silver_days`. Deux exécutions gold sur les mêmes jours terminés peuvent donc publier des
tables différentes.

Les schémas de l'étoile (`StructType`) sont dans `src/gtdp/gold/tables.py`, les indicateurs dans
`src/gtdp/gold/kpis.py` (une fonction pure par table). Les exemples sont des lignes du profil
`dev` (graine 42) publiées sur le cluster après J1, J2 et J3.

| Table | Grain | Clé (vérifiée à chaque exécution) | Partition | Écriture |
| --- | --- | --- | --- | --- |
| `fact_sales` | une ligne de commande silver | `transaction_id` | `order_date` | recalculée depuis toutes les commandes des jours silver terminés ; seules les dates dont le contenu change sont réécrites (`write_partitions`), les dates qui n'ont plus de commande sont supprimées (`drop_partitions`) |
| `fact_web_events` | une requête valide des logs web | `log_id` | aucune | reconstruite depuis les logs des jours silver terminés (`replace_table`) |
| `dim_date` | un jour | `date_key` | aucune | reconstruite |
| `dim_customer` | un client (SCD 1) | `customer_id` | aucune | reconstruite depuis `silver/customers` tel que silver l'a publié en dernier (KAN-58) |
| `dim_product` | une version catalogue (SCD 2) | `product_sk` | aucune | reconstruite depuis `silver/products_scd2` tel que silver l'a publié en dernier (KAN-58) |
| `kpi_*` | ligne « Grain » de la formule de chaque indicateur | les colonnes du grain, uniques par construction (`groupBy`, ou calendrier croisé avec les pays pour `kpi_revenue_moving_avg`), non vérifiées | aucune | reconstruite |
| `sensor_aggregates`, `sensor_alerts` | écrites par le job `streaming-sensors`, pas par `gold` | voir le [dictionnaire des capteurs](gold-sensors.md) | `window_date`, `batch_id` | partitions du micro-batch remplacées |

## Références et contrôles

| Référence | Règle |
| --- | --- |
| `fact_sales.date_key` → `dim_date.date_key` | obligatoire, aucun orphelin |
| `fact_sales.customer_id` → `dim_customer.customer_id` | obligatoire, aucun orphelin |
| `fact_sales.product_sk` → `dim_product.product_sk` | obligatoire, aucun orphelin |
| `fact_web_events.date_key` → `dim_date.date_key` | obligatoire, aucun orphelin |
| `fact_web_events.customer_id` → `dim_customer.customer_id` | facultative (0 ou 1 client) : une valeur sans client est comptée (`unmatched`), pas refusée |

Chaque exécution calcule toutes les tables, puis vérifie, avant d'écrire quoi que ce soit :

1. chaque commande trouve exactement une version `dim_product` valide à `event_time`
   (`metrics_json.build_violations` : `no_version`, `several_versions`) ;
2. chaque table de l'étoile a une clé unique (`duplicate_key.<table>`) et aucune valeur nulle
   dans une colonne obligatoire (`null_required.<table>`) ;
3. aucune référence obligatoire n'a d'orphelin (`orphan.<table>.<colonne>`) : une commande sans
   client dans `dim_customer` est un orphelin de `fact_sales.customer_id`.

Une colonne est obligatoire quand le schéma de `src/gtdp/gold/tables.py` la déclare
`nullable=False`. Toutes les colonnes de l'étoile le sont, sauf celles que leur règle dit nulles :

| Table | Colonnes facultatives |
| --- | --- |
| `dim_customer` | `region`, `segment`, `signup_date`, `email_hash` |
| `dim_product` | `product_name` |
| `fact_web_events` | `customer_id`, `product_id`, `device` |

Au premier écart, l'exécution s'arrête sans rien écrire et la ligne d'audit, écrite aussi en cas
d'échec, donne les comptes. Garder une commande sans prix applicable fausserait le chiffre
d'affaires sans le signaler, et l'écarter ferait perdre une commande : gold préfère s'arrêter et
nommer l'écart. Après l'écriture, `fact_sales` relu doit avoir, date par date, le même nombre de
lignes et la même empreinte de contenu que le calcul ; sinon l'exécution échoue et
`_state.json` reste à `PUBLISHING`.

Aucun membre `UNKNOWN` n'est nécessaire. Silver ne publie une commande que si son client a une
ligne silver et si son produit existe dans `products_scd2`. Silver ne vérifie pas qu'une version
couvre la date de la commande, mais la version 1 vaut depuis le `1900-01-01` et silver ne sait
pas encore écrire un horodatage antérieur : une telle commande fait échouer l'exécution silver
(limite connue, KAN-59, voir le [dictionnaire silver](silver.md#disposition-dune-ligne-bronze)).
Le contrôle 1 reste la défense de gold et échoue fermé (`no_version`). Le pays `UNKNOWN` de
`dim_customer` est celui d'un client sans région (drapeau `MISSING_REGION`), pas un membre
technique.

Le rapport (`src/gtdp/gold/report.py`) recalcule ces comptes depuis silver (lignes
`orders with ...`), montre que le total de `revenue_eur` et de `invoiced_amount_eur` de
`fact_sales` est égal au total recalculé depuis les jours silver terminés que gold a lus, et
que `dim_date` couvre toutes les dates de commande sans trou.

**Attributs du client.** `dim_customer` est de type SCD 1 : il ne garde que la dernière fiche
de chaque client. `fact_sales` ne copie donc aucun attribut du client (pays, région, segment) :
chaque indicateur qui regroupe par pays ou par segment joint `dim_customer` quand il est
calculé, à chaque exécution. Le pays et le segment des indicateurs sont les valeurs actuelles du
client, pas celles du jour de la commande : un client qui change de pays déplace tout son
historique dans les indicateurs, sans réécrire `fact_sales`. Les attributs du produit, eux, sont
versionnés : `product_sk` désigne la version appliquée à la commande, son prix et sa catégorie.

## `gold/fact_sales`

Une ligne par commande silver. Le prix catalogue est celui de la version `dim_product` telle que
`valid_from <= event_time < valid_to` (jointure SCD2 par intervalle sur `product_id`). Les
montants sont arrondis au centime, demi vers le haut. Ils sont en `decimal(20,2)` : une quantité
`int` (10 chiffres) fois un prix `decimal(10,2)` (8 chiffres avant la virgule) tient en 18
chiffres avant la virgule, aucune ligne acceptée par silver (9 et 8 chiffres au plus) ne
déborde. Le pays et le segment du client sont dans `dim_customer`.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `transaction_id` | string | Clé : unique sur toute la table. | `T000771975` |
| `event_time` | timestamp | Horodatage UTC de la commande silver. | `2025-09-25 08:19:09` |
| `date_key` | int | `yyyyMMdd` de `order_date`. | `20250925` |
| `customer_id` | string | Client de la commande, présent dans `dim_customer`. | `C003039` |
| `product_sk` | string | Version catalogue valide à `event_time`, présente dans `dim_product`. | `P00042-20250925081903` |
| `product_id` | string | Produit de la commande. | `P00042` |
| `channel` | string | `web`, `store` ou `partner`. | `partner` |
| `status` | string | `COMPLETED`, `CANCELLED` ou `RETURNED`. | `CANCELLED` |
| `quantity` | int | Quantité commandée. | `12` |
| `unit_price_invoiced` | decimal(10,2) | Prix unitaire facturé sur la ligne (`unit_price_eur` silver). | `505.26` |
| `applicable_unit_price` | decimal(10,2) | Prix catalogue de la version `product_sk`. | `530.97` |
| `discount_pct` | decimal(4,3) | Remise de la ligne, `0.000` avant J3. | `0.000` |
| `revenue_eur` | decimal(20,2) | `quantity × applicable_unit_price × (1 − discount_pct)`. | `6371.64` |
| `invoiced_amount_eur` | decimal(20,2) | `quantity × unit_price_invoiced × (1 − discount_pct)`. | `6063.12` |
| `order_date` | date | Partition : jour UTC de `event_time`. | `2025-09-25` |

L'exemple est la première commande de `P00042` après son changement de prix du
`2025-09-25 08:19:03` : elle prend le prix de la version 2 (530,97), la commande de 08:11:51 celui
de la version 1 (504,06), et le prix facturé reste celui de la ligne.

## `gold/fact_web_events`

Une ligne par requête valide de `silver/weblogs` ; silver a déjà déduit le type d'événement, le
produit et l'appareil.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `log_id` | string | Clé : identifiant silver de la ligne de log. | `0011a152…4b758cfb` |
| `event_ts` | timestamp | Horodatage UTC de la requête. | `2025-08-21 08:46:44` |
| `date_key` | int | `yyyyMMdd` du jour de `event_ts`. | `20250821` |
| `customer_id` | string | Champ utilisateur du log, nul pour `-`. | `C004653` |
| `event_type` | string | `view` (`GET /products/…`), `search` (`GET /search`), `cart` (`POST /cart/add`), `purchase` (`POST /checkout`), sinon `other`. | `cart` |
| `product_id` | string | Produit du chemin (`/products/<id>` ou `product=<id>`), nul sinon. | `P00216` |
| `http_status` | int | Code de réponse HTTP. | `200` |
| `device` | string | Texte entre parenthèses de l'agent utilisateur. | `tablet` |

## `gold/dim_date`

Tous les jours entre la première et la dernière date des commandes et des logs silver, sans trou.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `date_key` | int | Clé, `yyyyMMdd`. | `20251230` |
| `date` | date | Le jour. | `2025-12-30` |
| `year`, `quarter`, `month` | int | Année, trimestre (1 à 4), mois (1 à 12). | `2025`, `4`, `12` |
| `iso_week` | int | Semaine ISO 8601 : la semaine 1 de 2026 commence le lundi 2025-12-29. | `1` |
| `day_of_week` | int | Jour ISO : lundi 1 à dimanche 7. | `2` |
| `is_weekend` | boolean | Samedi ou dimanche. | `false` |

## `gold/dim_customer`

Silver `customers` (SCD 1, déjà pseudonymisé) : la dernière fiche de chaque client. Ses
attributs sont joints par les indicateurs à chaque exécution : valeurs actuelles.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `customer_id` | string | Clé. | `C000001` |
| `country` | string | Préfixe de la région, `UNKNOWN` sans région. | `DE` |
| `region` | string | Région, nulle si absente. | `DE-BY` |
| `segment` | string | Segment commercial. | `SMB` |
| `signup_date` | date | Date d'inscription, nulle si illisible. | `2026-02-27` |
| `email_hash` | string | Email haché avec le sel, nul si l'email est absent ou invalide. | `e77d644e…a446a451` |
| `quality_flags` | array<string> | Drapeaux silver de la fiche. | `[FUTURE_SIGNUP]` |

## `gold/dim_product`

Silver `products_scd2` : une ligne par version.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `product_sk` | string | Clé, `<product_id>-<valid_from yyyyMMddHHmmss>`. | `P00042-19000101000000` |
| `product_id` | string | Clé métier. | `P00042` |
| `product_name` | string | Nom de la version : chaque version garde le sien (attribut historisé). Nul si le champ du fichier est vide. | `Security P00042` |
| `category` | string | Catégorie de la version. | `Security` |
| `unit_price_eur` | decimal(10,2) | Prix catalogue de la version. | `504.06` |
| `valid_from` | timestamp | Début inclus ; `1900-01-01` pour la version 1. | `1900-01-01 00:00:00` |
| `valid_to` | timestamp | Fin exclue ; `9999-12-31` pour la version courante. | `2025-09-25 08:19:03` |
| `is_current` | boolean | Version courante. | `false` |
| `version` | int | Rang de la version. | `1` |

## Indicateurs

Chaque indicateur est une fonction pure de l'étoile, recalculée en entier à chaque exécution et
écrite par `replace_table` (registre `KPIS` de `src/gtdp/gold/kpis.py`). Le tableau « Formule »
de chaque indicateur donne les lignes lues, le grain et le calcul de chaque mesure. Règles
communes :

- Le chiffre d'affaires ne compte que les lignes `COMPLETED`, au prix catalogue applicable
  (`revenue_eur` de `fact_sales`).
- Le pays et le segment du client sont ceux de `dim_customer` au moment du calcul (valeurs
  actuelles, voir « Attributs du client »), joints sur `customer_id`. Le contrôle des orphelins
  garantit que chaque ligne de `fact_sales` trouve son client : le pays n'est jamais nul, le
  segment l'est quand la fiche n'en a pas.
- La catégorie est celle de la version appliquée à la ligne (`product_sk`).
- Une somme de `revenue_eur` (`decimal(20,2)`) est un `decimal(30,2)` exact, sans arrondi. Une
  moyenne de montants est arrondie au centime, demi vers le haut (`round(…, 2)`).
- Un taux est un `double` non arrondi, nul quand son dénominateur vaut 0.
- Une valeur nulle d'une colonne du grain forme son propre groupe.

Les indicateurs n'ont pas de contrôle bloquant propre : ils sont calculés depuis l'étoile déjà
contrôlée. Le [rapport gold](../runbook/gold.md) recalcule trois d'entre eux et signale un écart :
la somme de `revenue_eur` de `kpi_revenue_daily` égale le chiffre d'affaires `COMPLETED` de
`fact_sales`, les comptes de `kpi_conversion_funnel` ne croissent jamais d'une étape à la
suivante, et `kpi_top_products_country` de FR égale un classement fait en Python simple.

### `gold/kpi_revenue_daily`

Une ligne par date de commande et pays ayant au moins une commande `COMPLETED`.

| Formule | Règle |
| --- | --- |
| Lignes lues | `fact_sales` où `status = 'COMPLETED'` |
| Grain | `order_date`, `country` |
| `orders` | `count(*)` : une ligne de `fact_sales` est une transaction |
| `quantity` | `sum(quantity)` |
| `revenue_eur` | `sum(revenue_eur)` |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Date de commande. | `2025-07-26` |
| `country` | string | Pays actuel du client. | `DE` |
| `orders` | bigint | Commandes `COMPLETED`. | `413` |
| `quantity` | bigint | Somme des quantités. | `2684` |
| `revenue_eur` | decimal(30,2) | Somme de `revenue_eur`. | `1988414.29` |

### `gold/kpi_top_products_country`

Les produits de rang 1 à `top_n` (`[gold] top_n`, 5 par défaut) par chiffre d'affaires dans
chaque pays, sur tout l'historique. Des produits à égalité partagent un rang et sont tous gardés :
un pays peut donc lister plus de `top_n` produits, et les rangs se suivent sans trou. Le résultat
ne dépend que des montants (décimaux exacts), jamais de l'ordre de lecture. Un test vérifie la
règle d'égalité pour `top_n` = 1, 2 et 3. Le rapport remet à côté de l'indicateur, jusqu'au rang
`top_n`, les produits de FR dont le chiffre d'affaires est parmi les `top_n` plus élevés, classés
en Python simple : des produits à égalité y partagent aussi un rang.

| Formule | Règle |
| --- | --- |
| Lignes lues | `fact_sales` où `status = 'COMPLETED'`, toutes les dates |
| Grain | `country`, `product_id` |
| `revenue_eur`, `quantity`, `orders` | `sum(revenue_eur)`, `sum(quantity)`, `count(*)` |
| `rank` | `dense_rank() over (partition by country order by revenue_eur desc)` ; seules les lignes où `rank <= top_n` sont gardées |
| `product_name`, `category` | version de `dim_product` où `is_current`, jointe sur `product_id` (jointure externe gauche) |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `country` | string | Pays actuel du client. | `FR` |
| `rank` | int | Rang dense, 1 = plus gros chiffre d'affaires. | `1` |
| `product_id` | string | Produit. | `P00042` |
| `product_name`, `category` | string | Nom et catégorie de la version courante. | `Security P00042`, `Lighting` |
| `revenue_eur` | decimal(30,2) | Chiffre d'affaires du produit dans le pays. | `203874707.20` |
| `quantity`, `orders` | bigint | Quantités et commandes `COMPLETED`. | `398728`, `61252` |

### `gold/kpi_revenue_moving_avg`

Chiffre d'affaires quotidien par pays et sa moyenne sur les `N` derniers jours, le jour compris,
avec `N = [gold] moving_average_days` (7 par défaut), sans boucle sur les pays. Le calendrier est
complet : un jour sans vente vaut 0. Sans cette ligne, les `N` lignes d'une fenêtre couvriraient
plus de `N` jours et la moyenne serait surestimée. Les `N - 1` premiers jours du calendrier ont
moins de `N` jours derrière eux : leur moyenne porte sur `window_days` jours (1 à 6 pour
`N = 7`). Un test compare le résultat à un calcul en Python simple, pour `N` = 1, 3 et 7.

| Formule | Règle |
| --- | --- |
| Calendrier | chaque jour de `min(order_date)` à `max(order_date)` des lignes de `fact_sales` de tout statut, croisé avec chaque pays ayant au moins une ligne de tout statut ; un pays sans commande `COMPLETED` a donc des lignes à 0 |
| Grain | `order_date`, `country` : un jour du calendrier pour un pays |
| `revenue_eur` | `revenue_eur` de `kpi_revenue_daily` pour le jour et le pays, `0` quand il n'y en a pas |
| Fenêtre | `partition by country order by order_date rows between N - 1 preceding and current row` |
| `revenue_moving_avg_eur` | `round(avg(revenue_eur), 2)` sur la fenêtre |
| `window_days` | `count(*)` sur la fenêtre |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Jour du calendrier. | `2025-01-03` |
| `country` | string | Pays actuel du client. | `FR` |
| `revenue_eur` | decimal(30,2) | Chiffre d'affaires `COMPLETED` du jour, 0 sans commande. | `5155128.61` |
| `revenue_moving_avg_eur` | decimal(31,2) | Moyenne de `revenue_eur` sur la fenêtre, arrondie au centime. | `5055677.62` |
| `window_days` | bigint | Jours dans la fenêtre : `N`, sauf les `N - 1` premiers jours. | `3` |

### `gold/kpi_price_deviation`

Médiane exacte du rapport prix facturé / prix catalogue applicable, arrondi à 13 décimales, par
mois et par catégorie de la version appliquée, sur toutes les lignes quel que soit leur statut. Les groupes comptent au
plus quelques dizaines de milliers de lignes : le calcul exact reste léger. La médiane résiste
aux lignes du kit dont le prix facturé ne dépend pas du catalogue ; sur les lignes synthétiques,
la remise commerciale tirée dans `[0 ; 0,10]` donne une médiane proche de 0,95.

| Formule | Règle |
| --- | --- |
| Lignes lues | toutes les lignes de `fact_sales`, tout statut |
| Grain | `month = trunc(order_date, 'month')`, `category` |
| `lines` | `count(*)` |
| `median_price_ratio` | `median(unit_price_invoiced / applicable_unit_price)`, soit `percentile(…, 0.5)` : sur un nombre pair de lignes, la moyenne des deux rapports du milieu. Le rapport de deux `decimal(10,2)` est un `decimal(23,13)` : Spark l'arrondit à 13 décimales (demi vers le haut), puis le convertit en `double` avant la médiane |
| Valeurs nulles | aucune : silver refuse un prix nul ou négatif, le rapport est toujours défini |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `month` | date | Premier jour du mois de commande. | `2025-07-01` |
| `category` | string | Catégorie de la version appliquée. | `Electrical` |
| `lines` | bigint | Lignes de commande. | `13044` |
| `median_price_ratio` | double | Médiane de `unit_price_invoiced / applicable_unit_price`, chaque rapport arrondi à 13 décimales. | `0.9494675748273` |

### `gold/kpi_cancel_return_rate`

Part des lignes annulées et des lignes retournées, par date de commande, pays, canal et catégorie
de la version appliquée, toutes lignes comptées quel que soit leur statut. Les comptes
s'additionnent vers un grain plus large : un taux par pays et par jour vaut
`sum(cancelled) / sum(lines)` sur les canaux et catégories du jour.

| Formule | Règle |
| --- | --- |
| Lignes lues | toutes les lignes de `fact_sales`, tout statut |
| Grain | `order_date`, `country`, `channel`, `category` |
| `lines` | `count(*)` |
| `cancelled`, `returned` | `sum(status = 'CANCELLED')`, `sum(status = 'RETURNED')` |
| `cancel_rate`, `return_rate` | `cancelled / lines`, `returned / lines` ; un groupe compte au moins une ligne, le taux n'est jamais nul |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Date de commande. | `2026-01-02` |
| `country` | string | Pays actuel du client. | `FR` |
| `channel` | string | Canal. | `partner` |
| `category` | string | Catégorie de la version appliquée. | `Lighting` |
| `lines` | bigint | Lignes de commande, tous statuts. | `135` |
| `cancelled`, `returned` | bigint | Lignes `CANCELLED`, `RETURNED`. | `4`, `3` |
| `cancel_rate`, `return_rate` | double | `cancelled / lines`, `returned / lines`. | `0.02962962962962963`, `0.022222222222222223` |

### `gold/kpi_avg_basket`

Panier moyen des commandes `COMPLETED`, par date de commande, pays et segment du client. Un panier
est une transaction : le contrat n'a pas d'identifiant de panier qui regrouperait plusieurs lignes.
Pour un grain plus large (un pays par jour, par exemple), le panier moyen vaut
`sum(revenue_eur) / sum(orders)`.

| Formule | Règle |
| --- | --- |
| Lignes lues | `fact_sales` où `status = 'COMPLETED'` |
| Grain | `order_date`, `country`, `segment` ; un client sans segment tombe dans le groupe où `segment` est nul |
| `orders` | `count(*)` |
| `revenue_eur` | `sum(revenue_eur)` |
| `avg_basket_eur` | `round(avg(revenue_eur), 2)` |
| `avg_invoiced_eur` | `round(avg(invoiced_amount_eur), 2)` |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Date de commande. | `2026-01-02` |
| `country` | string | Pays actuel du client. | `FR` |
| `segment` | string | Segment actuel du client dans `dim_customer`, nul si la fiche n'en a pas. | `SMB` |
| `orders` | bigint | Commandes `COMPLETED`. | `528` |
| `revenue_eur` | decimal(30,2) | Somme de `revenue_eur`. | `2390096.37` |
| `avg_basket_eur` | decimal(21,2) | Moyenne de `revenue_eur` (prix catalogue applicable). | `4526.70` |
| `avg_invoiced_eur` | decimal(21,2) | Moyenne de `invoiced_amount_eur` (prix facturé). | `4309.14` |

### `gold/kpi_http_errors_hourly`

Taux d'erreurs HTTP par heure des logs web : erreurs client (4xx) et erreurs serveur (5xx)
rapportées à toutes les requêtes valides de l'heure. Le contrat n'injecte que des 404 et des 500.

| Formule | Règle |
| --- | --- |
| Lignes lues | toutes les lignes de `fact_web_events` (lignes de log valides, tous codes HTTP) |
| Grain | `event_hour = date_trunc('hour', event_ts)` |
| `requests` | `count(*)` |
| `http_4xx`, `http_5xx` | `sum(http_status between 400 and 499)`, `sum(http_status between 500 and 599)` |
| `rate_4xx`, `rate_5xx` | `http_4xx / requests`, `http_5xx / requests` ; une heure compte au moins une requête, le taux n'est jamais nul |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `event_hour` | timestamp | `event_ts` tronqué à l'heure. | `2025-12-06 23:00:00` |
| `requests` | bigint | Requêtes de l'heure. | `6` |
| `http_4xx`, `http_5xx` | bigint | Réponses de 400 à 499, de 500 à 599. | `2`, `0` |
| `rate_4xx`, `rate_5xx` | double | `http_4xx / requests`, `http_5xx / requests`. | `0.3333333333333333`, `0.0` |

### `gold/kpi_conversion_funnel`

Entonnoir visite → produit vu → panier → commande, par appareil. Une visite est un client sur un
appareil un jour donné : le contrat n'a pas d'identifiant de session. Seules les requêtes servies
d'un client identifié comptent : une page produit en 404 ou un paiement en erreur 500 n'est pas
une étape, et une requête anonyme ne se rattache à aucune visite. Une visite atteint une étape
quand elle contient l'événement de cette étape et ceux de toutes les étapes précédentes : chaque
étape ne garde que des visites de l'étape d'avant, les comptes ne croissent donc jamais le long
de l'entonnoir (vérifié par un test et par le rapport). Constat sur les données : le kit tire ses
événements indépendamment les uns des autres, une visite en contient rarement plus d'un, d'où des
taux de panier et de commande très faibles, que l'indicateur montre tels quels.

| Formule | Règle |
| --- | --- |
| Lignes lues | `fact_web_events` où `http_status < 400` et `customer_id` non nul |
| Visite | regroupement par `customer_id`, `to_date(event_ts)` et `device`, avec un drapeau par étape : `max(event_type = 'view')`, `max(event_type = 'cart')`, `max(event_type = 'purchase')` |
| Grain | `device` ; un agent sans appareil tombe dans le groupe où `device` est nul |
| `visits` | `count(*)` des visites |
| `product_views` | visites avec `view` |
| `carts` | visites avec `view` et `cart` |
| `orders` | visites avec `view`, `cart` et `purchase` |
| Taux | `view_rate = product_views / visits`, `cart_rate = carts / product_views`, `order_rate = orders / carts`, `conversion_rate = orders / visits` ; `cart_rate` est nul sans produit vu, `order_rate` sans panier |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `device` | string | Appareil. | `tablet` |
| `visits` | bigint | Visites (client, appareil, jour) avec au moins une requête servie. | `32055` |
| `product_views` | bigint | Visites avec un `view`. | `8136` |
| `carts` | bigint | Visites avec un `view` et un `cart`. | `40` |
| `orders` | bigint | Visites avec un `view`, un `cart` et un `purchase`. | `1` |
| `view_rate` | double | `product_views / visits`. | `0.2538137576041179` |
| `cart_rate` | double | `carts / product_views`. | `0.004916420845624385` |
| `order_rate` | double | `orders / carts`. | `0.025` |
| `conversion_rate` | double | `orders / visits`. | `3.119638121977851E-5` |

### `gold/kpi_data_quality`

Par dépôt, source et code : les rejets silver par motif, et les commandes conservées bien que
datées d'avant l'inscription de leur client (incohérence connue du kit, voir le
[contrat](../data-contract.md#incohérences-connues-du-kit)). Une ligne rejetée pour deux motifs
compte une fois sous chacun.

| Formule | Règle |
| --- | --- |
| Lignes rejetées | `silver/rejects/<source>` des jours silver terminés, une ligne par motif de `reject_reasons` (`explode`), avec `disposition = 'rejected'` |
| Lignes gardées | `silver/orders` des jours silver terminés, jointes à `dim_customer` sur `customer_id`, où `to_date(event_time) < signup_date`, avec `code = 'ORDER_BEFORE_SIGNUP'` et `disposition = 'kept'` ; une `signup_date` nulle n'en donne aucune |
| Grain | `ingestion_date`, `source`, `code`, `disposition` |
| `rows` | `count(*)` |

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `ingestion_date` | date | Dépôt (J1, J2, J3). | `2026-01-01` |
| `source` | string | `orders`, `customers`, `products`, `weblogs`. | `weblogs` |
| `code` | string | Motif de rejet silver, ou `ORDER_BEFORE_SIGNUP`. | `MALFORMED_LINE` |
| `disposition` | string | `rejected` (dans `silver/rejects`) ou `kept` (en silver). | `rejected` |
| `rows` | bigint | Lignes. | `500` |

## `gold/_state.json`

Un fichier JSON à la racine de `/datalake/gold`, écrit par le job `gold` (`gtdp.gold.state`).
Il couvre les tables du job `gold`, pas `sensor_aggregates` ni `sensor_alerts` du streaming.

| Champ | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `status` | string | `PUBLISHING` de la première écriture d'une exécution à la relecture de `fact_sales`, puis `PUBLISHED`. Une exécution échouée ou tuée en cours d'écriture laisse `PUBLISHING`. Une exécution qui échoue à un contrôle n'écrit rien et laisse l'état précédent. | `PUBLISHED` |
| `run_id` | string | Exécution auditée qui a écrit l'état (`run_id` de l'audit). | `20261127T101500Z-1a2b3c4d` |
| `silver_days` | array<string> | Jours silver terminés dont cette exécution a lu les commandes, les logs et les rejets. `customers` et `products_scd2` sont lus tels que silver les a publiés en dernier (limite connue, KAN-58). | `["2026-01-01", "2026-01-02", "2026-01-03"]` |
