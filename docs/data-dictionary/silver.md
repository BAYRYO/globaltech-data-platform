# Dictionnaire de données silver

Le job `silver` lit les tables bronze d'un jour et publie, sous `/datalake/silver`, des tables
Parquet (snappy) typées, contrôlées, dédupliquées, historisées et pseudonymisées, ainsi qu'une
table de rejets par source. L'exploitation (exécution, rejeu, vérification) est décrite dans le
[runbook silver](../runbook/silver.md). Les règles viennent du
[modèle de données](../data-model.md#règles-de-qualité-et-disposition).

| Table | Chemin | Clé (unicité vérifiée avant et après écriture) | Partition | Écriture |
| --- | --- | --- | --- | --- |
| `orders` | `silver/orders` | `transaction_id` | `ingestion_date` | partition du jour remplacée (`write_partitions`) |
| `customers` | `silver/customers` | `customer_id` | aucune | table reconstruite depuis bronze jusqu'au jour, échangée (`replace_table`) |
| `products_scd2` | `silver/products_scd2` | `product_id`, `valid_from` | aucune | table reconstruite depuis bronze jusqu'au jour, échangée |
| `weblogs` | `silver/weblogs` | `log_id` | `ingestion_date` | partition du jour remplacée |
| rejets | `silver/rejects/<source>` | aucune | `ingestion_date` | partition du jour remplacée, même vide |

Les schémas explicites (`StructType`) sont `SILVER_ORDERS`, `SILVER_CUSTOMERS`,
`SILVER_PRODUCTS_SCD2`, `SILVER_WEBLOGS` et `SILVER_REJECTS` dans `src/gtdp/schemas.py`. Les
exemples sont des lignes du profil `dev` (graine 42) publiées sur le cluster.

## Jours complets

`silver/_days/ingestion_date=D/` tient un marqueur JSON par source publiée et vérifiée pour `D`,
puis `_SUCCESS` quand les quatre sources le sont (`gtdp.silver.days`). Un jour sans `_SUCCESS`
est en cours, échoué, ou rendu incomplet par le rejeu d'un jour antérieur ou d'une source dont il
dépend. Les doublons inter-jours ne regardent que les jours complets ; un lecteur qui doit voir
des jours entiers ne lit des tables partitionnées que les partitions de
`complete_days(spark, silver)`. `customers` et `products_scd2`, reconstruites en entier, n'ont
pas de partition à écarter : elles tiennent les lignes du jour pour lequel silver les a écrites en
dernier, que `silver/_days/rebuilt/<source>.json` nomme dès avant l'écriture. Un tel lecteur ne
les lit que si ce jour est complet : gold refuse sinon ([dictionnaire gold](gold.md)). Rejouer un
jour réécrit ses marqueurs et retire ceux des jours suivants
([ADR-0005](../adr/0005-silver-incremental.md)).

Chaque fichier de `_days` est écrit en entier sous un nom temporaire, `<fichier>.tmp-<id>`, puis
renommé à sa place en remplaçant l'ancien, dans le même appel au NameNode
(`gtdp.io.write_file`) : un lecteur trouve l'ancien contenu ou le nouveau, jamais un fichier
tronqué. Un fichier illisible (abîmé, ou tronqué par une version antérieure du job) arrête le job
qui le lit avec `MarkerError: <chemin> is unreadable (...)` et la commande silver qui le
réécrit, au lieu d'être pris pour absent. Dans les exemples des marqueurs, `run_id` est
l'exécution J3 des rejets plus bas ; les heures ont le format écrit, pas la valeur du cluster.

### `silver/_days/ingestion_date=<d>/<source>.json`

Écrit par `mark_source` une fois la source publiée et relue sans écart.

| Champ | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `source` | string | Source publiée, nom du fichier sans `.json`. | `orders` |
| `run_id` | string | Exécution silver qui a publié la source, clé `run_id` de l'audit. | `20261126T195836Z-d3d2ae34` |
| `published_at` | string | Heure d'écriture du marqueur, après la publication et sa relecture, ISO 8601 en UTC à la seconde. | `2026-11-26T19:59:41+00:00` |

### `silver/_days/ingestion_date=<d>/_SUCCESS`

Écrit par `mark_complete` quand chacune des quatre sources a son marqueur pour le jour. Gold ne
lit des commandes, des logs et des rejets que les jours qui ont ce fichier, et ne s'exécute que si
le jour de `rebuilt/customers.json` et celui de `rebuilt/products.json` l'ont aussi.

| Champ | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `run_id` | string | Exécution silver qui a complété le jour. | `20261126T195836Z-d3d2ae34` |
| `sources` | map<string,string> | Chaque source publiée pour le jour, avec le `run_id` de l'exécution qui l'a publiée. | `{"customers": "20261126T195836Z-d3d2ae34", "orders": "20261126T195836Z-d3d2ae34", …}` |

### `silver/_days/rebuilt/<source>.json`

Écrit par `mark_rebuilt` pour `customers` et `products`, juste avant chaque échange de leur table,
puis après la remise en état d'une publication retirée. La table ne tient donc jamais les lignes
d'un jour que le fichier ne nomme pas, même si l'exécution s'arrête ou échoue après l'échange.

| Champ | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `source` | string | Source de la table reconstruite. | `customers` |
| `day` | string | Jour dont la table tient les lignes, reconstruites depuis bronze jusqu'à lui ; nul quand la remise en état a vidé la table. | `2026-01-03` |
| `run_id` | string | Exécution silver qui écrit la table. | `20261126T195836Z-d3d2ae34` |
| `written_at` | string | Heure d'écriture du fichier, avant l'échange, ISO 8601 en UTC à la seconde. | `2026-11-26T19:58:51+00:00` |

## Disposition d'une ligne bronze

Chaque ligne bronze d'un jour reçoit exactement une disposition, évaluée dans cet ordre
(`gtdp.silver.quality.classify`) :

| Disposition | Condition | Destination |
| --- | --- | --- |
| `duplicated.exact` | même `_record_hash` qu'une autre ligne du jour, sauf la première | écartée, comptée |
| `rejected` | au moins un code dans `reject_reasons` | `silver/rejects/<source>` |
| `duplicated.key` | même clé qu'une autre ligne retenue du jour ; la survivante est celle du plus petit `_record_hash`. Une ligne dont une colonne de la clé est nulle n'est jamais un doublon de clé | écartée, comptée |
| `duplicated.cross_day` | clé déjà publiée par un jour complet antérieur (`transaction_id` des commandes, `log_id` des logs, [ADR-0005](../adr/0005-silver-incremental.md)) | écartée, comptée |
| `clean` | aucune des précédentes, drapeaux compris | table silver |

Identité vérifiée à chaque exécution : `lignes bronze du jour = clean + rejected + duplicated`.

Bornes de plausibilité. Le `1900-01-01`, début de la version 1 de tout produit, est le premier
jour que le modèle connaît. Une date ou un horodatage UTC antérieur reçoit la disposition de son
registre : rejet `ANCIENT_TIMESTAMP` pour l'heure d'une commande, d'un log ou d'une version
produit, drapeau `ANCIENT_SIGNUP` ou `ANCIENT_UPDATED_AT` pour une fiche client, gardée comme pour
`BAD_DATE`. Côté futur, l'heure d'une commande ou d'un log postérieure à `ingestion_date + 1 jour`
est rejetée (`FUTURE_TIMESTAMP`) et une `signup_date` postérieure au dépôt est signalée
(`FUTURE_SIGNUP`). Le générateur ne produit aucune de ces dates anciennes ni aucun log futur : ces
codes n'ont pas de code du manifeste. Limite connue : `updated_at` n'a pas de borne future, ni pour
un client (une fiche datée du futur l'emporte dans le choix SCD 1) ni pour un produit (sa version
ne vaut qu'à partir de cette date).

Parquet écrit et relit les dates telles que Spark les calcule, calendrier grégorien proleptique
(modes `CORRECTED` de `gtdp.spark.build_spark`, [ADR-0002](../adr/0002-parquet-idempotence.md)) :
une date antérieure à 1582 ou un horodatage antérieur à 1900, comme la `signup_date`
`1500-01-01` d'une fiche signalée, s'écrit comme une autre valeur.

Les codes de rejet et les drapeaux sont calculés en **une seule projection** à partir du
registre de règles de la source (`ENTITY.rules` de `src/gtdp/silver/<source>.py`). Une ligne
dont `_corrupt_record` est non nul reçoit le seul code `MALFORMED_ROW` : ses colonnes sont
décalées, les autres règles ne sont pas évaluées.

## Formats acceptés

Une valeur hors format devient nulle, puis la règle correspondante la signale. Le texte est
contrôlé avant la conversion : une conversion seule accepterait `1.5` comme entier 1 ou
`2025-03-04 10:00` comme horodatage (`src/gtdp/silver/parse.py`).

| Type | Format accepté | Refusé (exemples) |
| --- | --- | --- |
| horodatage | `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$`, date réelle, fuseau UTC | `2025/03/04 10:00`, `N/A`, `2025-02-30T10:00:00` |
| date | `^\d{4}-\d{2}-\d{2}$`, puis `dd/MM/yyyy` pour la relecture de `BAD_DATE` | `31/02/2024` |
| entier | `^-?\d{1,9}$` | `abc`, `1.5` |
| montant `decimal(10,2)` | `^-?\d{1,8}(\.\d{1,2})?$` | `abc`, `12.345` |
| remise `decimal(4,3)` | `^\d(\.\d{1,3})?$`, vide = `0.000` | `-0.100`, `abc` |

## `silver/orders`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `transaction_id` | string | Texte bronze. Clé : unique sur toute la table, une commande renvoyée un autre jour reste dans la partition de sa première arrivée. | `T000992703` |
| `event_time` | timestamp | Horodatage ISO du fichier, UTC. | `2026-01-02 23:46:37` |
| `customer_id` | string | Identifiant d'une ligne de `silver/customers` publiée pour le jour. | `C003781` |
| `product_id` | string | Identifiant d'au moins une version de `silver/products_scd2` publiée pour le jour : toute commande trouve exactement un prix. | `P00113` |
| `quantity` | int | Entier strictement positif. | `3` |
| `unit_price_eur` | decimal(10,2) | Prix facturé sur la ligne, strictement positif. | `1061.33` |
| `channel` | string | `web`, `store` ou `partner`. | `web` |
| `status` | string | `COMPLETED`, `CANCELLED` ou `RETURNED`. | `COMPLETED` |
| `discount_pct` | decimal(4,3) | `0.000` quand la colonne est absente (J1, J2) ou vide (doublon renvoyé dans J3), sinon la valeur du fichier dans `[0 ; 0,30]` ([ADR-0007](../adr/0007-evolution-schema.md)). Jamais nulle. | `0.000` |
| `ingestion_date` | date | Partition : jour de première arrivée de la commande. | `2026-01-03` |

Règles de rejet (`src/gtdp/silver/orders.py`), une ligne pouvant en cumuler plusieurs :

| Code | Condition | Code du manifeste |
| --- | --- | --- |
| `MISSING_KEY` | `transaction_id` vide (règle défensive, rien d'injecté) | — |
| `NULL_CUSTOMER` | `customer_id` vide ou fait d'espaces | `NULL_CUSTOMER` |
| `UNKNOWN_CUSTOMER` | `customer_id` non vide sans ligne dans `silver/customers` publiée pour le jour, client vu seulement dans une fiche rejetée compris | `UNKNOWN_CUSTOMER` |
| `UNKNOWN_PRODUCT` | `product_id` sans version dans `silver/products_scd2` publiée pour le jour, produit vu seulement dans des lignes rejetées compris | `UNKNOWN_PRODUCT` |
| `BAD_QUANTITY` | quantité non entière, nulle ou négative | `BAD_QUANTITY` |
| `BAD_PRICE` | prix non décimal, nul ou négatif | `BAD_PRICE` |
| `BAD_TIMESTAMP` | `event_time` hors format | `BAD_TIMESTAMP` |
| `FUTURE_TIMESTAMP` | `event_time > ingestion_date + 1 jour` (le lendemain à minuit est accepté) | `FUTURE_TIMESTAMP` |
| `ANCIENT_TIMESTAMP` | `event_time` antérieur au `1900-01-01 00:00:00`, qu'aucune version produit ne couvre (règle défensive, rien d'injecté) | — |
| `BAD_ENUM` | `channel` ou `status` hors liste, casse comprise | `BAD_ENUM` |
| `BAD_DISCOUNT` | remise hors format ou hors `[0 ; 0,30]` | — |
| `MALFORMED_ROW` | `_corrupt_record` non nul, code exclusif | `MALFORMED_ROW` |

Doublons : `DUPLICATE_EXACT` du manifeste → `duplicated.exact`, `DUPLICATE_CROSS_DAY` →
`duplicated.cross_day`. Le contrat n'injecte pas de doublon de clé dans un même dépôt.

Les commandes de `D` sont contrôlées contre les dimensions publiées pour `D` : le job traite
`customers` et `products` avant `orders`, refuse `--source orders` tant qu'elles ne sont pas
publiées pour `D`, et rend `orders` à refaire quand l'une d'elles est rejouée. Sur les données du
contrat, les comptes `UNKNOWN_CUSTOMER` et `UNKNOWN_PRODUCT` restent ceux du manifeste : aucun
client ni produit n'y existe seulement dans des lignes rejetées.

## `silver/customers`

Type SCD 1 : par `customer_id`, la ligne propre de plus grand `updated_at` parmi tous les jours
jusqu'à `D`. Une ligne sans `updated_at` lisible ne l'emporte que si le client n'en a pas
d'autre ; à égalité, le jour le plus récent, puis le plus petit `_record_hash`, départagent.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `customer_id` | string | Clé, identifiant interne conservé. | `C000002` |
| `email_hash` | string | `sha2(sel ‖ lower(trim(email)), 256)` en hexadécimal ; nul si l'email porte `MISSING_EMAIL` ou `BAD_EMAIL`. | `ee04c102…eea549dad` |
| `region` | string | Région du fichier, nulle si vide. | `FR-IDF` |
| `country` | string | Préfixe de la région avant `-`, `UNKNOWN` si la région est vide. | `FR` |
| `segment` | string | Texte du fichier. | `MID` |
| `signup_date` | date | Date ISO, sinon relue au format `dd/MM/yyyy`, sinon nulle. | `2022-10-13` |
| `updated_at` | timestamp | Horodatage ISO de la fiche, nul s'il est illisible (drapeau `BAD_UPDATED_AT`). | `2026-01-02 03:01:42` |
| `quality_flags` | array<string> | Drapeaux de la fiche retenue, vide si aucun. | `[FUTURE_SIGNUP]` |
| `ingestion_date` | date | Jour où la version retenue est arrivée (colonne simple). | `2026-01-03` |

`full_name`, `phone`, l'email en clair et le `_record_hash` de la ligne bronze ne sont pas
écrits : un hachage non salé de la ligne confirmerait un nom, un email et un téléphone devinés.

| Code | Action | Condition | Code du manifeste |
| --- | --- | --- | --- |
| `MISSING_KEY` | rejet | `customer_id` vide (règle défensive) | — |
| `MALFORMED_ROW` | rejet | `_corrupt_record` non nul (règle défensive) | — |
| `MISSING_EMAIL` | drapeau | email vide ou fait d'espaces ; `email_hash` nul | `MISSING_EMAIL` |
| `BAD_EMAIL` | drapeau | `lower(trim(email))` hors `^[^@\s]+@[^@\s]+\.[a-z]{2,}$` ; `email_hash` nul | `BAD_EMAIL` |
| `MISSING_REGION` | drapeau | région vide ; `country = UNKNOWN` | `MISSING_REGION` |
| `BAD_DATE` | drapeau | `signup_date` hors format ISO | `BAD_DATE` |
| `FUTURE_SIGNUP` | drapeau | `signup_date > ingestion_date` (incohérence connue du kit, 688 clients en J1) | — |
| `ANCIENT_SIGNUP` | drapeau | `signup_date` antérieure au `1900-01-01`, gardée telle quelle (règle défensive, rien d'injecté) | — |
| `BAD_UPDATED_AT` | drapeau | `updated_at` vide ou hors format ISO ; la fiche est gardée, dernière dans le choix SCD 1 | — |
| `ANCIENT_UPDATED_AT` | drapeau | `updated_at` antérieur au `1900-01-01 00:00:00`, gardé tel quel : la fiche passe après toute fiche datée plus tard dans le choix SCD 1 (règle défensive, rien d'injecté) | — |

Doublons : `DUPLICATE_EXACT` → `duplicated.exact` ; `DUPLICATE_KEY` → `duplicated.key`, clé
(`customer_id`, `updated_at`) : la fiche est répétée avec son email en majuscules ou entouré
d'espaces, donc identique une fois l'email normalisé. Deux fiches sans `updated_at` lisible ne
sont jamais doublons de clé l'une de l'autre.

## `silver/products_scd2`

Type SCD 2, reconstruit à chaque exécution à partir de toutes les lignes produits propres de
bronze jusqu'à `D`. Une ligne ne crée une version que si `product_name`, `category` ou
`unit_price_eur` change par rapport à la version précédente. Une même (`product_id`,
`updated_at`) reçue deux jours différents garde la ligne du jour le plus récent.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `product_sk` | string | `<product_id>-<valid_from au format yyyyMMddHHmmss>`, clé de version. | `P00038-20260102134039` |
| `product_id` | string | Clé métier. | `P00038` |
| `product_name` | string | Attribut historisé. | `Automation P00038` |
| `category` | string | Attribut historisé, jamais vide. | `Automation` |
| `unit_price_eur` | decimal(10,2) | Prix catalogue de la version, attribut historisé. | `1445.83` |
| `valid_from` | timestamp | Début inclus : `1900-01-01 00:00:00` pour la version 1, `updated_at` de la version sinon. | `2026-01-02 13:40:39` |
| `valid_to` | timestamp | Fin exclue : `valid_from` de la version suivante, `9999-12-31 00:00:00` pour la version courante. | `9999-12-31 00:00:00` |
| `is_current` | boolean | Vrai pour la seule dernière version du produit. | `true` |
| `version` | int | Rang de la version, à partir de 1. | `2` |
| `_record_hash` | string | Ligne bronze qui a ouvert la version. | `3601c2a6…8cfb8d277` |
| `ingestion_date` | date | Jour où cette ligne est arrivée (colonne simple). | `2026-01-03` |

Le prix applicable à une commande est celui de la version telle que
`valid_from <= event_time < valid_to` (`gtdp.silver.products.with_applicable_price`). La
version 1 valant depuis 1900 et une commande antérieure étant rejetée (`ANCIENT_TIMESTAMP`),
toute commande trouve exactement une version. Contrôles avant et après
écriture, chacun à zéro ligne : clé (`product_id`, `valid_from`) unique, intervalle non vide,
ni recouvrement ni trou entre deux versions, `is_current` vrai pour la seule version ouverte,
version 1 depuis 1900.

| Code | Condition | Code du manifeste |
| --- | --- | --- |
| `MISSING_KEY` | `product_id` vide (règle défensive) | — |
| `BAD_PRICE` | prix non décimal, nul ou négatif | `BAD_PRICE` |
| `NULL_CATEGORY` | catégorie vide | `NULL_CATEGORY` |
| `BAD_TIMESTAMP` | `updated_at` hors format (règle défensive) | — |
| `ANCIENT_TIMESTAMP` | `updated_at` antérieur au `1900-01-01 00:00:00` : la version finirait avant le début de la version 1 (règle défensive, rien d'injecté) | — |
| `CONFLICTING_VERSION` | au moins deux lignes distinctes du jour pour (`product_id`, `updated_at`) : tout le groupe est rejeté, soit 2 lignes par anomalie du manifeste | `CONFLICTING_VERSION` |
| `MALFORMED_ROW` | `_corrupt_record` non nul | `MALFORMED_ROW` |

Exemple, historique de `P00042` après J3 : version 1 à 504,06 € (`Security`) du
`1900-01-01` au `2025-09-25 08:19:03`, version 2 à 530,97 € (`Lighting`) depuis. La commande
`T000593481` du `2025-09-25 08:11:51` se valorise à 504,06 € (facturée 463,83 €), la commande
`T000771975` du `2025-09-25 08:19:09` à 530,97 € (facturée 505,26 €).

## `silver/weblogs`

Lignes au *Combined Log Format* découpées par `regexp_extract` (aucune UDF) avec
`COMBINED_LOG` de `src/gtdp/silver/weblogs.py`. Sont acceptés un client IPv4 ou IPv6, un
champ entre guillemets contenant un guillemet ou une barre oblique inverse échappés (gardés
tels quels) et la requête `"-"` d'une connexion fermée avant toute requête.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `log_id` | string | `sha2(sel ‖ ligne brute, 256)` en hexadécimal, clé. Salé : un hachage non salé de la ligne, recalculé depuis les autres colonnes et les 256 adresses du `/24`, redonnait l'adresse. | `24eff5ba…7e51ab063` |
| `event_ts` | timestamp | `[dd/MMM/yyyy:HH:mm:ss Z]` converti en UTC : jour, mois (`Jan` à `Dec`), heure et décalage contrôlés, puis conversion d'un texte ISO-8601. | `2026-01-02 00:09:30` |
| `ip_hash` | string | `sha2(sel ‖ lower(adresse IP), 256)` en hexadécimal. | `0b645cb3…d025c738` |
| `ip_prefix` | string | Réseau `/24` d'une adresse IPv4, `/48` d'une IPv6 (trois premiers groupes, `::` développé), pour la géographie grossière. | `198.18.139.0/24`, `2001:db8:0::/48` |
| `customer_id` | string | Champ utilisateur, nul pour `-`. | `C000096` |
| `method` | string | Méthode HTTP, nulle pour la requête `"-"`. | `POST` |
| `path` | string | Chemin demandé, requête comprise, nul pour `"-"`. | `/cart/add?product=P00968` |
| `protocol` | string | `HTTP/x.y`, nul pour `"-"`. | `HTTP/1.1` |
| `http_status` | int | Code de réponse. Les 404 et 500 sont des lignes valides, conservées. | `200` |
| `bytes_sent` | bigint | Taille de la réponse, nulle pour `-`. | `8497` |
| `referer` | string | Nul pour `-`. | `NULL` |
| `user_agent` | string | Agent utilisateur. | `Mozilla/5.0 (tablet)` |
| `device` | string | Texte entre parenthèses de l'agent. | `tablet` |
| `event_type` | string | `view` (`GET /products/`), `search` (`GET /search`), `cart` (`POST /cart/add`), `purchase` (`POST /checkout`), sinon `other` ; table `EVENT_TYPES`. | `cart` |
| `product_id` | string | Produit du chemin (`/products/<id>` ou `product=<id>`), nul sinon. | `P00968` |
| `ingestion_date` | date | Partition : jour du dépôt. | `2026-01-03` |

| Code | Condition | Code du manifeste |
| --- | --- | --- |
| `MALFORMED_LINE` | ligne hors de l'expression du Combined Log Format, ou horodatage hors format ou impossible (`1/Jan/2026:…`, `31/Feb/2026`, décalage `+2500`) : la ligne est rejetée, le jour publié | `MALFORMED_LINE` |
| `FUTURE_TIMESTAMP` | `event_ts > ingestion_date + 1 jour`, en UTC, le lendemain à minuit accepté (règle défensive, rien d'injecté) | — |
| `ANCIENT_TIMESTAMP` | `event_ts` antérieur au `1900-01-01 00:00:00` UTC (règle défensive, rien d'injecté) | — |

`HTTP_404` et `HTTP_500` du manifeste sont conservés. Doublons : même ligne dans le dépôt
(`duplicated.exact`), ligne déjà publiée par un jour complet antérieur
(`duplicated.cross_day`) ; rien d'injecté.

## Rejets (`silver/rejects/<source>`)

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `source` | string | Source de la ligne. | `orders` |
| `reject_reasons` | array<string> | Codes vérifiés, dans l'ordre du registre ; au moins un. | `[BAD_QUANTITY]` |
| `raw_record` | string | Ligne du fichier : `_corrupt_record`, ou les colonnes du fichier jointes par `,`. Son SHA-256 est `_record_hash` (une colonne inconnue n'y figure pas). | `T000992796,2026-01-02T08:57:28,C000292,P00042,-3,486.32,partner,COMPLETED,0.290` |
| `_record_hash` | string | Ligne bronze rejetée. | `3480b46b…5d3303d3db63` |
| `_source_file` | string | Fichier landing d'origine. | `hdfs://namenode:8020/datalake/landing/orders/ingestion_date=2026-01-03/orders_20260103.csv` |
| `_run_id` | string | Exécution silver qui a écrit la partition, clé `run_id` de l'audit. | `20261126T195836Z-d3d2ae34` |
| `ingestion_date` | date | Partition : jour du dépôt. | `2026-01-03` |

Les rejets contiennent des données personnelles en clair (emails, IP). Le répertoire porte
l'ACL `group:analysts:---`, héritée par chaque partition. Un job ne supprime jamais
`/datalake/silver/rejects` lui-même.

## Pseudonymisation

| Donnée | Traitement silver | Justification |
| --- | --- | --- |
| `email` | `email_hash = sha2(sel ‖ lower(trim(email)), 256)` | compte les clients distincts et permet les jointures sans exposer l'adresse ; la normalisation rend le même hash aux variantes de casse et d'espaces |
| `full_name`, `phone` | supprimés | aucun usage analytique |
| `customer_id` | conservé | identifiant interne, non identifiant hors du SI |
| adresse IP | `ip_hash` salé, `ip_prefix` `/24` (IPv4) ou `/48` (IPv6) | l'IP est une donnée personnelle (RGPD) ; le préfixe suffit à la géographie grossière (troisième octet par pays) |
| ligne de log | `log_id` salé | sans sel, le hachage de la ligne redonnait l'adresse |
| ligne bronze d'un client ou d'une commande | `_record_hash` non publié | sans sel, il confirmait une identité devinée |

Le sel vient du seul `GTDP_PSEUDO_SALT` ([ADR-0004](../adr/0004-pseudonymisation.md)). Il
entre dans le calcul comme une donnée : un DataFrame d'une ligne, diffusé et joint
(`gtdp.silver.pseudonymize`). Les plans ne montrent que le nom de colonne `_salt`. Les tests
vérifient son absence des plans `explain` (modes `extended`, `formatted`, `cost`, `codegen`) et
de l'event log d'une exécution réelle. Sur le cluster, ce que sert le History Server pour les
exécutions J1 à J3 (plans, environnement, jobs, étapes) nomme `_salt` 205 fois par exécution et
ne contient pas sa valeur. Sans le
sel, retrouver un email demande une attaque par dictionnaire sur un espace inconnu. Avec le
sel, la correspondance reste calculable : la donnée est pseudonymisée, non anonymisée.

Règle : aucune colonne silver lisible par les analystes n'est une fonction non salée d'une
valeur personnelle ou d'une ligne qui en contient. Les tests le vérifient : aucune valeur de
`orders`, `customers` ou `weblogs` n'est le SHA-256 d'une ligne bronze, et l'attaque par les 256
adresses du `/24` ne retrouve aucune IP. `products_scd2` garde `_record_hash` : une ligne
produit ne contient aucune donnée personnelle.

Sel : sur le cluster, le job refuse un sel vide, de moins de 16 caractères ou égal au sel de
développement du dépôt. La première exécution écrit l'empreinte
`sha256("gtdp-salt-fingerprint" ‖ sel)` dans `<ops>/pseudonymization/salt_fingerprint.json` ;
une exécution avec un autre sel est refusée avant toute écriture. Changer de sel impose de
reconstruire silver ([runbook](../runbook/silver.md#changer-le-sel)).

Risque résiduel, accepté par l'[ADR-0004](../adr/0004-pseudonymisation.md) :

| Colonnes | Risque | Pourquoi il est accepté |
| --- | --- | --- |
| `ip_hash`, `email_hash` | relier les lignes d'une même adresse ou d'un même client entre tables et jours | c'est l'usage prévu (visiteurs et clients distincts) ; inverser un hachage demande le sel |
| `ip_prefix` | localiser un visiteur à 256 adresses près (IPv4) ou à un `/48` (IPv6) | géographie grossière ; sans le sel, aucune colonne ne permet de tester les 256 candidates |
| `customer_id` des logs et des commandes | relier navigation et achats d'un client | identifiant interne, conservé par le modèle ; il ne nomme personne hors du SI |
| `user_agent`, `event_ts`, `path` | empreinte de navigateur, horaires, termes de recherche (`/search?q=…`) | les agents du jeu de données se réduisent à trois appareils ; sur des données réelles, tronquer l'agent et la requête de `path` serait la mesure suivante |
| `region`, `segment`, `signup_date`, `updated_at` des clients | quasi-identifiants combinés | nécessaires aux indicateurs ; ni nom, ni email, ni téléphone |

Les rejets gardent les lignes brutes, données personnelles comprises : l'ACL
`group:analysts:---` les ferme aux analystes.

## Changement de schéma (J3) et colonnes inconnues

Silver lit bronze avec `bronze_schema(source)` : `discount_pct`, absente des partitions J1 et
J2, y vaut nul, puis `0.000` en silver. Une colonne d'en-tête absente de `SOURCES` n'est pas
lue. Elle est listée par source dans `metrics_json.unknown_columns` de la ligne d'audit, par
exemple `{"orders": ["coupon_code"]}`.
