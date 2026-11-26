# Dictionnaire de données gold

Le job `gold` publie sous `/datalake/gold` le modèle en étoile des ventes et des logs web, puis
neuf tables d'indicateurs, en Parquet (snappy). L'ACL par défaut de `/datalake/gold` donne la
lecture au groupe `analysts` sur chaque table. L'exploitation (exécution, rejeu, vérification)
est décrite dans le [runbook gold](../runbook/gold.md), les choix de modèle dans le
[modèle de données](../data-model.md#gold--modèle-en-étoile).

Les schémas de l'étoile (`StructType`) sont dans `src/gtdp/gold/tables.py`, les indicateurs dans
`src/gtdp/gold/kpis.py` (une fonction pure par table). Les exemples sont des lignes du profil
`dev` (graine 42) publiées sur le cluster après J1, J2 et J3.

| Table | Grain | Clé (vérifiée à chaque exécution) | Partition | Écriture |
| --- | --- | --- | --- | --- |
| `fact_sales` | une ligne de commande silver | `transaction_id` | `order_date` | les dates de commande du jour traité, recalculées depuis les commandes de ces dates de tous les jours silver terminés (`write_partitions`) |
| `fact_web_events` | une requête valide des logs web | `log_id` | aucune | reconstruite depuis tout silver (`replace_table`) |
| `dim_date` | un jour | `date_key` | aucune | reconstruite |
| `dim_customer` | un client (SCD 1) | `customer_id` | aucune | reconstruite |
| `dim_product` | une version catalogue (SCD 2) | `product_sk` | aucune | reconstruite |
| `kpi_*` | voir chaque table | — | aucune | reconstruite |
| `sensor_aggregates`, `sensor_alerts` | écrites par le job `streaming-sensors`, pas par `gold` | voir le dictionnaire des capteurs (`gold-sensors.md`, lot 5) | `batch_id` | micro-batch du streaming |

## Références et contrôles

| Référence | Règle |
| --- | --- |
| `fact_sales.date_key` → `dim_date.date_key` | obligatoire, aucun orphelin |
| `fact_sales.customer_id` → `dim_customer.customer_id` | obligatoire, aucun orphelin |
| `fact_sales.product_sk` → `dim_product.product_sk` | obligatoire, aucun orphelin |
| `fact_web_events.date_key` → `dim_date.date_key` | obligatoire, aucun orphelin |
| `fact_web_events.customer_id` → `dim_customer.customer_id` | facultative (0 ou 1 client) : une valeur sans client est comptée (`unmatched`), pas refusée |

Chaque exécution vérifie, dans cet ordre :

1. avant d'écrire `fact_sales` : chaque commande des dates recalculées trouve exactement une
   version `dim_product` valide à `event_time` et un client `dim_customer` ;
2. après l'écriture : les partitions relues contiennent autant de lignes que de commandes silver
   sur ces dates (aucune commande perdue) ;
3. avant les indicateurs : clé unique de chaque table de l'étoile et aucun orphelin sur les
   références obligatoires, sur toutes les partitions.

Aucun membre `UNKNOWN` n'est nécessaire : silver ne publie une commande que si son client a une
ligne silver et son produit une version SCD2 valide à sa date, et le contrôle 1 le confirme. Le pays `UNKNOWN` de `dim_customer` est celui
d'un client sans région (drapeau `MISSING_REGION`), pas un membre technique.

## `fact_sales`

Une ligne par commande silver. Le prix catalogue est celui de la version `dim_product` telle que
`valid_from <= event_time < valid_to` (jointure SCD2 par intervalle sur `product_id`). Les
montants sont arrondis au centime, demi vers le haut.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `transaction_id` | string | Clé : unique sur toute la table. | `T000771975` |
| `event_time` | timestamp | Horodatage UTC de la commande silver. | `2025-09-25 08:19:09` |
| `date_key` | int | `yyyyMMdd` de `order_date`. | `20250925` |
| `customer_id` | string | Client de la commande, présent dans `dim_customer`. | `C003039` |
| `product_sk` | string | Version catalogue valide à `event_time`, présente dans `dim_product`. | `P00042-20250925081903` |
| `product_id` | string | Produit de la commande. | `P00042` |
| `country` | string | `country` du client dans `dim_customer` (SCD 1 : pays actuel). | `FR` |
| `channel` | string | `web`, `store` ou `partner`. | `partner` |
| `status` | string | `COMPLETED`, `CANCELLED` ou `RETURNED`. | `CANCELLED` |
| `quantity` | int | Quantité commandée. | `12` |
| `unit_price_invoiced` | decimal(10,2) | Prix unitaire facturé sur la ligne (`unit_price_eur` silver). | `505.26` |
| `applicable_unit_price` | decimal(10,2) | Prix catalogue de la version `product_sk`. | `530.97` |
| `discount_pct` | decimal(4,3) | Remise de la ligne, `0.000` avant J3. | `0.000` |
| `revenue_eur` | decimal(12,2) | `quantity × applicable_unit_price × (1 − discount_pct)`. | `6371.64` |
| `invoiced_amount_eur` | decimal(12,2) | `quantity × unit_price_invoiced × (1 − discount_pct)`. | `6063.12` |
| `order_date` | date | Partition : jour UTC de `event_time`. | `2025-09-25` |

L'exemple est la première commande de `P00042` après son changement de prix du
`2025-09-25 08:19:03` : elle prend le prix de la version 2 (530,97), la commande de 08:11:51 celui
de la version 1 (504,06), et le prix facturé reste celui de la ligne.

## `fact_web_events`

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

## `dim_date`

Tous les jours entre la première et la dernière date des commandes et des logs silver, sans trou.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `date_key` | int | Clé, `yyyyMMdd`. | `20251230` |
| `date` | date | Le jour. | `2025-12-30` |
| `year`, `quarter`, `month` | int | Année, trimestre (1 à 4), mois (1 à 12). | `2025`, `4`, `12` |
| `iso_week` | int | Semaine ISO 8601 : la semaine 1 de 2026 commence le lundi 2025-12-29. | `1` |
| `day_of_week` | int | Jour ISO : lundi 1 à dimanche 7. | `2` |
| `is_weekend` | boolean | Samedi ou dimanche. | `false` |

## `dim_customer`

Silver `customers` (SCD 1, déjà pseudonymisé) : la dernière fiche de chaque client.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `customer_id` | string | Clé. | `C000001` |
| `country` | string | Préfixe de la région, `UNKNOWN` sans région. | `DE` |
| `region` | string | Région, nulle si absente. | `DE-BY` |
| `segment` | string | Segment commercial. | `SMB` |
| `signup_date` | date | Date d'inscription, nulle si illisible. | `2026-02-27` |
| `email_hash` | string | Email haché avec le sel, nul si l'email est absent ou invalide. | `e77d644e…a446a451` |
| `quality_flags` | array<string> | Drapeaux silver de la fiche. | `[FUTURE_SIGNUP]` |

## `dim_product`

Silver `products_scd2` : une ligne par version.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `product_sk` | string | Clé, `<product_id>-<valid_from yyyyMMddHHmmss>`. | `P00042-19000101000000` |
| `product_id` | string | Clé métier. | `P00042` |
| `product_name` | string | Nom de la version initiale. | `Security P00042` |
| `category` | string | Catégorie de la version. | `Security` |
| `unit_price_eur` | decimal(10,2) | Prix catalogue de la version. | `504.06` |
| `valid_from` | timestamp | Début inclus ; `1900-01-01` pour la version 1. | `1900-01-01 00:00:00` |
| `valid_to` | timestamp | Fin exclue ; `9999-12-31` pour la version courante. | `2025-09-25 08:19:03` |
| `is_current` | boolean | Version courante. | `false` |
| `version` | int | Rang de la version. | `1` |

## Indicateurs

Le chiffre d'affaires d'un indicateur ne compte que les lignes `COMPLETED`, au prix catalogue
applicable (`revenue_eur`). Les taux sont des `double`, nuls quand leur dénominateur vaut 0.

### `kpi_revenue_daily`

Une ligne par date de commande et pays ayant au moins une commande `COMPLETED`.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Date de commande. | `2025-07-26` |
| `country` | string | Pays du client. | `DE` |
| `orders` | bigint | Commandes `COMPLETED`. | `413` |
| `quantity` | bigint | Somme des quantités. | `2684` |
| `revenue_eur` | decimal(22,2) | Somme de `revenue_eur`. | `1988414.29` |

### `kpi_top_products_country`

Les produits de rang 1 à `top_n` (`[gold] top_n`, 5 par défaut) par chiffre d'affaires dans
chaque pays, sur tout l'historique. `rank` est un `dense_rank` sur une fenêtre partitionnée par
pays et ordonnée par chiffre d'affaires décroissant : des produits à égalité partagent un rang et
sont tous gardés, un pays peut donc lister plus de `top_n` produits, et les rangs se suivent sans
trou. Le résultat ne dépend que des montants (décimaux exacts), jamais de l'ordre de lecture.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `country` | string | Pays du client. | `FR` |
| `rank` | int | Rang dense, 1 = plus gros chiffre d'affaires. | `1` |
| `product_id` | string | Produit. | `P00042` |
| `product_name`, `category` | string | Nom et catégorie de la version courante. | `Security P00042`, `Lighting` |
| `revenue_eur` | decimal(22,2) | Chiffre d'affaires du produit dans le pays. | `203874707.20` |
| `quantity`, `orders` | bigint | Quantités et commandes `COMPLETED`. | `398728`, `61252` |

### `kpi_revenue_moving_avg`

Chiffre d'affaires quotidien par pays et sa moyenne sur 7 jours, le jour compris (fenêtre
`rowsBetween(-6, 0)` partitionnée par pays, ordonnée par date). Le calendrier est complet : tous
les jours de la première à la dernière date de commande, pour chaque pays ayant des commandes. Un
jour sans vente vaut 0 : sans cette ligne, les 7 lignes d'une fenêtre couvriraient plus de
7 jours et la moyenne serait surestimée. Les 6 premiers jours du calendrier ont moins de 7 jours
derrière eux : leur moyenne porte sur `window_days` jours (1 à 6).

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `order_date` | date | Jour du calendrier. | `2025-01-03` |
| `country` | string | Pays. | `FR` |
| `revenue_eur` | decimal(22,2) | Chiffre d'affaires du jour, 0 sans commande. | `5155128.61` |
| `revenue_ma7_eur` | decimal(23,2) | Moyenne de `revenue_eur` sur la fenêtre. | `5055677.62` |
| `window_days` | bigint | Jours dans la fenêtre : 7, sauf les 6 premiers jours. | `3` |

### `kpi_price_deviation`

Médiane du rapport prix facturé / prix catalogue applicable (`percentile_approx`, précision
10 000), par mois et par catégorie de la version appliquée, sur toutes les lignes quel que soit
leur statut. La médiane résiste aux lignes du kit dont le prix facturé ne dépend pas du
catalogue ; sur les lignes synthétiques, la remise commerciale tirée dans `[0 ; 0,10]` donne une
médiane proche de 0,95.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `month` | date | Premier jour du mois de commande. | `2025-07-01` |
| `category` | string | Catégorie de la version appliquée. | `Electrical` |
| `lines` | bigint | Lignes de commande. | `13044` |
| `median_price_ratio` | double | Médiane de `unit_price_invoiced / applicable_unit_price`. | `0.9494675748273` |

### `kpi_cancel_return_rate`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `channel` | string | Canal. | `web` |
| `category` | string | Catégorie de la version appliquée. | `Security` |
| `lines` | bigint | Lignes de commande, tous statuts. | `96199` |
| `cancelled`, `returned` | bigint | Lignes `CANCELLED`, `RETURNED`. | `2839`, `3006` |
| `cancel_rate`, `return_rate` | double | `cancelled / lines`, `returned / lines`. | `0.0295`, `0.0312` |

### `kpi_avg_basket`

Un panier est une transaction : le contrat n'a pas d'identifiant de panier qui regrouperait
plusieurs lignes.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `country` | string | Pays du client. | `UNKNOWN` |
| `segment` | string | Segment du client dans `dim_customer`. | `ENTERPRISE` |
| `orders` | bigint | Commandes `COMPLETED`. | `2016` |
| `avg_basket_eur` | decimal(13,2) | Moyenne de `revenue_eur`. | `4847.27` |
| `avg_invoiced_eur` | decimal(13,2) | Moyenne de `invoiced_amount_eur`. | `4556.03` |
| `avg_quantity` | double | Moyenne des quantités, arrondie à 2 décimales. | `6.47` |

### `kpi_http_errors_hourly`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `event_hour` | timestamp | `event_ts` tronqué à l'heure. | `2025-07-22 18:00:00` |
| `requests` | bigint | Requêtes de l'heure. | `9` |
| `http_404`, `http_500` | bigint | Réponses 404, 500. | `0`, `0` |
| `rate_404`, `rate_500` | double | Part des 404, des 500. | `0.0`, `0.0` |

### `kpi_conversion_funnel`

Vues de produit, ajouts au panier et achats par appareil. Seules les requêtes servies comptent
(`http_status` < 400) : une page produit en 404 ou un paiement en erreur 500 n'est pas une étape.
Constat sur les données : le kit tire ses événements de façon uniforme entre les types, l'entonnoir
est donc plat (rapports proches de 1), ce que l'indicateur montre tel quel.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `device` | string | Appareil. | `mobile` |
| `views`, `carts`, `purchases` | bigint | Événements `view`, `cart`, `purchase` servis. | `8179`, `8179`, `7974` |
| `view_to_cart` | double | `carts / views`. | `1.0` |
| `cart_to_purchase` | double | `purchases / carts`. | `0.9749` |
| `view_to_purchase` | double | `purchases / views`. | `0.9749` |

### `kpi_data_quality`

Par dépôt, source et code : les rejets silver par motif, et les commandes conservées bien que
datées d'avant l'inscription de leur client (incohérence connue du kit, voir le
[contrat](../data-contract.md#incohérences-connues-du-kit)). Une ligne rejetée pour deux motifs
compte une fois sous chacun.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `ingestion_date` | date | Dépôt (J1, J2, J3). | `2026-01-01` |
| `source` | string | `orders`, `customers`, `products`, `weblogs`. | `weblogs` |
| `code` | string | Motif de rejet silver, ou `ORDER_BEFORE_SIGNUP`. | `MALFORMED_LINE` |
| `disposition` | string | `rejected` (dans `silver/rejects`) ou `kept` (en silver). | `rejected` |
| `rows` | bigint | Lignes. | `500` |
