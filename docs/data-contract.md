# Contrat de données

Le kit fourni (`data/kit/`, 5 000 clients, 1 000 produits, 200 000 transactions 2025,
100 000 événements web) est **propre** : aucune anomalie, aucun déséquilibre de clé, une seule
version par produit, pas de données personnelles, pas de logs texte ni de capteurs. Le sujet
décrit pourtant un générateur à graine fixe qui produit trois jours J1, J2, J3 avec anomalies
volontaires. Ce générateur n'est pas dans le kit : le module `gtdp.datagen` le réalise en
prenant le kit comme référentiel. Ce document fixe ce qu'il produit. Tout le pipeline en dépend.

## Principes

- **Déterministe** : même graine et même profil donnent des fichiers identiques octet pour octet.
  Chaque fichier a son propre `random.Random` dérivé de `(seed, nom du fichier)`.
- **Vérité terrain** : chaque dépôt contient un `_MANIFEST.json` qui compte les anomalies
  injectées par type. Une ligne porte **au plus une** anomalie, donc les comptes de rejets
  attendus sont exacts et testables.
- **Bibliothèque standard uniquement** (Python ≥ 3.11), pour tourner partout, y compris dans le
  conteneur client du cluster.

## Calendrier

| Dépôt | `ingestion_date` | Contenu des commandes |
| --- | --- | --- |
| J1 | 2026-01-01 | Chargement initial : historique 2025 (les 200 000 transactions du kit + lignes synthétiques jusqu'au volume du profil) |
| J2 | 2026-01-02 | Commandes du 2026-01-01, retardataires de fin décembre, renvois de lignes de J1 |
| J3 | 2026-01-03 | Commandes du 2026-01-02, **changement de schéma** (colonne `discount_pct`) |

Volume journalier de J2 et J3 = volume annuel de J1 / 365, pour une série quotidienne homogène
(utile à la moyenne mobile).

## Profils de volume

| Profil | Commandes J1 | Commandes J2, J3 (chacun) | Logs web J1 | Usage |
| --- | --- | --- | --- | --- |
| `ci` | 20 000 (échantillon du kit) | ~55 | 5 000 | tests, CI |
| `dev` (défaut) | 1 000 000 | ~2 740 | 100 000 | démonstration |
| `full` | 10 000 000 | ~27 400 | 1 000 000 | benchmarks (10 M+ lignes) |

## Arborescence produite

```
data/generated/<profil>/
├── landing/
│   ├── 2026-01-01/
│   │   ├── orders/orders_20260101.csv
│   │   ├── customers/customers_20260101.csv
│   │   ├── products/products_20260101.csv
│   │   ├── weblogs/weblogs_20260101.log
│   │   └── _MANIFEST.json
│   ├── 2026-01-02/ …
│   └── 2026-01-03/ …
└── sensors/
    └── 2026-01-01/sensors_20260101T000000.jsonl, sensors_20260101T000005.jsonl, …
```

CLI :

```bash
python -m gtdp.datagen generate --profile dev --seed 42 --out data/generated
python -m gtdp.datagen replay-sensors --src data/generated/dev/sensors/2026-01-01 \
       --dst hdfs:///datalake/landing/sensors/incoming --interval 5
```

## Commandes (`orders`)

CSV UTF-8, séparateur `,`, en-tête, fin de ligne `\n`. Colonnes du kit conservées :

| Colonne | Type cible | Règle |
| --- | --- | --- |
| `transaction_id` | string | `T` + 9 chiffres, unique par commande |
| `event_time` | timestamp | ISO `yyyy-MM-ddTHH:mm:ss`, heure locale UTC |
| `customer_id` | string | doit exister dans les clients |
| `product_id` | string | doit exister dans les produits |
| `quantity` | int | > 0 |
| `unit_price_eur` | decimal(10,2) | > 0, prix facturé sur la ligne |
| `channel` | string | `web`, `store`, `partner` |
| `status` | string | `COMPLETED`, `CANCELLED`, `RETURNED` |
| `discount_pct` | decimal(4,3) | **J3 seulement**, 0 à 0.30, ajoutée en dernière colonne |

Déséquilibre de clé : 20 % des lignes synthétiques portent le produit `P00042`. En profil
`full`, le ratio max/médiane du nombre de lignes par produit dépasse 200.

Anomalies injectées (taux sur les lignes du dépôt, codes repris dans le manifeste) :

| Code | Taux | Exemple |
| --- | --- | --- |
| `DUPLICATE_EXACT` | 1 % | ligne répétée à l'identique dans le même fichier |
| `DUPLICATE_CROSS_DAY` | 0,5 % (J2, J3) | ligne d'un dépôt antérieur renvoyée telle quelle |
| `NULL_CUSTOMER` | 0,5 % | `customer_id` vide |
| `UNKNOWN_CUSTOMER` | 0,3 % | `C999…` absent du référentiel |
| `UNKNOWN_PRODUCT` | 0,3 % | `P9…` absent du référentiel |
| `BAD_QUANTITY` | 0,5 % | `0`, `-3`, `abc`, vide |
| `BAD_PRICE` | 0,2 % | `-12.50`, `0`, vide |
| `BAD_TIMESTAMP` | 0,3 % | `2025/03/04 10:00`, `N/A`, vide |
| `FUTURE_TIMESTAMP` | 0,1 % | `event_time` postérieur à `ingestion_date` + 1 jour |
| `BAD_ENUM` | 0,2 % | `channel` = `fax` ou `status` = `UNKNOWN` |
| `MALFORMED_ROW` | 0,1 % | nombre de champs incorrect |

Les 200 000 lignes du kit restent intactes dans J1 (seuls des doublons exacts d'entre elles sont
ajoutés).

## Produits (`products`)

En-tête : `product_id,product_name,category,unit_price_eur,updated_at`

- J1 : les 1 000 produits du kit en version initiale (`updated_at` = `2024-12-31T00:00:00`),
  plus des versions supplémentaires datées de 2025 pour 15 % des produits (variation de prix de
  ±5 à 20 %, changement de `category` pour 3 %). Un même produit apparaît donc plusieurs fois.
- J2, J3 : delta du jour (produits modifiés ce jour-là, `updated_at` dans la journée
  précédant l'ingestion) et quelques nouveaux produits `P01001+`.
- `P00042` change de prix au moins une fois en 2025, pour que le prix applicable à la date de
  commande diffère du prix courant.

Anomalies : `DUPLICATE_EXACT` 1 %, `BAD_PRICE` 0,5 %, `NULL_CATEGORY` 0,5 %,
`CONFLICTING_VERSION` (même `product_id` et `updated_at`, prix différent) 0,2 %.

## Clients (`customers`)

En-tête : `customer_id,full_name,email,phone,region,segment,signup_date,updated_at`

- J1 : les 5 000 clients du kit, enrichis de données personnelles synthétiques déterministes
  (`full_name`, `email`, `phone`). `region` garde le format du kit `PP-RRR` ; le pays est le
  préfixe (`FR`, `DE`, `ES`, `IT`, `UK`).
- J2, J3 : nouveaux clients `C005001+` et mises à jour de clients existants (région, segment),
  `updated_at` du jour.

Anomalies : `DUPLICATE_EXACT` 1 %, `DUPLICATE_KEY` 0,5 % (même identifiant, email en casse ou
espaces différents), `MISSING_EMAIL` 2 %, `BAD_EMAIL` 0,5 %, `MISSING_REGION` 1 %,
`BAD_DATE` 0,5 % (`27/02/2024`).

## Logs web (`weblogs`)

Format *Combined Log Format*, une requête par ligne :

```
198.51.100.23 - C001315 [01/Jan/2026:20:21:48 +0000] "GET /products/P00042 HTTP/1.1" 200 5123 "-" "Mozilla/5.0 (tablet)"
```

Les lignes dérivent des événements du kit (`events.jsonl`) : `view` → `GET /products/<pid>`,
`search` → `GET /search?q=<terme>`, `cart` → `POST /cart/add?product=<pid>`,
`purchase` → `POST /checkout`. Le champ utilisateur porte `customer_id`, l'agent utilisateur le
`device`. J1 rejoue l'historique 2025, J2 et J3 une journée.

Anomalies : `MALFORMED_LINE` 0,5 % (ligne tronquée, guillemets manquants, texte libre),
`HTTP_404` 2 % (`/products/P99999`, `/old-catalog`), `HTTP_500` 0,5 %. Les 404 et 500 sont des
lignes valides : elles alimentent l'indicateur d'erreurs, elles ne sont pas rejetées.

## Capteurs (`sensors`)

JSON Lines, un fichier par intervalle de 5 secondes (`sensors_<yyyyMMddTHHmmss>.jsonl`),
120 fichiers par jour en profil `dev` (10 minutes de flux).

```json
{"event_id":"S20260101-000123","sensor_id":"FR-LYON-M03-TEMP","site":"FR-LYON","machine_id":"FR-LYON-M03","metric":"temperature","value":71.4,"unit":"C","event_time":"2026-01-01T00:00:03.120Z","emitted_at":"2026-01-01T00:00:04.900Z"}
```

| Métrique | Unité | Régime normal | Seuil d'alerte |
| --- | --- | --- | --- |
| `temperature` | °C | 60 ± 8 | > 85 |
| `vibration` | mm/s | 3 ± 1 | > 7,1 (ISO 10816, zone D) |
| `pressure` | bar | 6 ± 0,5 | < 4 ou > 8 |

5 sites (un par pays), 4 machines par site, 3 capteurs par machine.

Anomalies : `LATE_EVENT` 5 % (`event_time` de 30 s à 5 min avant l'intervalle du fichier),
`VERY_LATE_EVENT` 1 % (plus de 15 min, au-delà du watermark), `DUPLICATE_EVENT` 3 % (même
`event_id` réémis dans un fichier ultérieur), `MALFORMED_JSON` 0,5 %, `NULL_VALUE` 0,5 %,
`SPIKE` (rafale au-delà du seuil sur une machine) pour déclencher des alertes.

## Manifeste (`_MANIFEST.json`)

```json
{
  "generator_version": "1.0.0",
  "seed": 42,
  "profile": "dev",
  "ingestion_date": "2026-01-02",
  "files": [
    {"source": "orders", "path": "orders/orders_20260102.csv", "rows": 2803, "sha256": "…",
     "anomalies": {"DUPLICATE_EXACT": 28, "DUPLICATE_CROSS_DAY": 14, "NULL_CUSTOMER": 14}}
  ]
}
```

`rows` compte les lignes de données (hors en-tête). Les tests silver comparent leurs compteurs
de rejets et de doublons à ces valeurs.
