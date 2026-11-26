# Contrat de données

Le kit fourni (`data/kit/`, 5 000 clients, 1 000 produits, 200 000 transactions 2025,
100 000 événements web) est **techniquement propre** : aucun doublon, aucune valeur manquante,
aucune référence orpheline, aucun déséquilibre de clé, une seule version par produit, pas de
données personnelles, pas de logs texte ni de capteurs. Le sujet décrit pourtant un générateur à
graine fixe qui produit trois jours J1, J2, J3 avec anomalies volontaires. Ce générateur n'est pas
dans le kit : le module `gtdp.datagen` le réalise en prenant le kit comme référentiel. Ce document
fixe ce qu'il produit. Tout le pipeline en dépend. Le traitement attendu de chaque anomalie en
silver est dans le [modèle de données](data-model.md#règles-de-qualité-et-disposition).

## Incohérences connues du kit

Mesurées sur le kit (profilage du 2026-11-26, revue adverse du Lot 0) et **conservées** : ce sont
des données valides, que le générateur ne corrige pas et que le manifeste ne compte pas comme
anomalies.

| Constat | Mesure | Traitement |
| --- | --- | --- |
| Commandes antérieures à la date d'inscription du client | 49 580 transactions sur 200 000 | conservées ; indicateur de qualité en gold |
| Clients inscrits après le 2026-01-01 (après J1) | 688 clients | conservés ; drapeau `FUTURE_SIGNUP` en silver |
| Format de région | `^[A-Z]{2}-[A-Z]{2,4}$` (`DE-BY`, `FR-IDF`, `FR-PACA`) | pays = préfixe avant `-` |
| Prix facturé indépendant du prix catalogue | 1 ligne sur 200 000 égale au prix catalogue, corrélation −0,0003, ratio médian 0,97 | conservé ; voir l'indicateur d'écart de prix |

## Principes

- **Déterministe** : même graine et même profil donnent des fichiers identiques octet pour octet.
  Chaque fichier a son propre `random.Random` dérivé de `(seed, nom du fichier)`.
- **Vérité terrain** : chaque dépôt contient un `_MANIFEST.json` qui compte les anomalies
  injectées par type. Une ligne porte **au plus une** anomalie, donc les comptes attendus sont
  exacts et testables. Chaque type d'anomalie applicable à un fichier batch y apparaît **au moins
  une fois**, même quand le taux appliqué au volume donne moins d'une ligne (profil `ci`, dépôts
  J2 et J3). Pour les capteurs, cette règle s'applique par journée de flux.
- **Doublons tirés de lignes saines** : `DUPLICATE_EXACT`, `DUPLICATE_CROSS_DAY` et
  `DUPLICATE_KEY` copient uniquement des lignes sans anomalie, pour que chaque doublon soit compté
  comme doublon et non comme rejet.
- **Vérificateur indépendant** : `python -m gtdp.datagen verify` relit les fichiers, recompte les
  anomalies avec ses propres règles et compare au manifeste.
- **Bibliothèque standard uniquement** (Python ≥ 3.11), pour tourner partout, y compris dans le
  conteneur client du cluster.

## Calendrier

| Dépôt | `ingestion_date` | Contenu des commandes |
| --- | --- | --- |
| J1 | 2026-01-01 | Chargement initial : historique 2025 (transactions du kit + lignes synthétiques jusqu'au volume du profil) |
| J2 | 2026-01-02 | Commandes du 2026-01-01, retardataires de fin décembre, renvois de lignes de J1 |
| J3 | 2026-01-03 | Commandes du 2026-01-02, **changement de schéma** (colonne `discount_pct`) |

Volume journalier de J2 et J3 = volume annuel de J1 / 365, pour une série quotidienne homogène
(utile à la moyenne mobile).

## Profils de volume

| Profil | Commandes J1 | Commandes J2, J3 (chacun) | Logs web J1 | Flux capteurs par jour | Usage |
| --- | --- | --- | --- | --- | --- |
| `ci` | 20 000 (les 10 000 premières lignes du kit + 10 000 synthétiques) | ~55 | 5 000 | 48 fichiers (4 min) | tests, CI |
| `dev` (défaut) | 1 000 000 (les 200 000 lignes du kit + 800 000 synthétiques) | ~2 740 | 100 000 | 180 fichiers (15 min) | démonstration |
| `full` | 10 000 000 (les 200 000 lignes du kit + 9 800 000 synthétiques) | ~27 400 | 1 000 000 | 180 fichiers | benchmarks (10 M+ lignes) |

Les lignes du kit retenues par le profil restent **intactes**, octet pour octet, dans J1 : seuls
des doublons exacts de certaines d'entre elles sont ajoutés.

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
python -m gtdp.datagen verify --path data/generated/dev
python -m gtdp.datagen replay-sensors --src data/generated/dev/sensors/2026-01-01 \
       --dst hdfs:///datalake/landing/sensors/incoming --interval 5
```

## Commandes (`orders`)

CSV UTF-8, séparateur `,`, en-tête, fin de ligne `\n`. Colonnes du kit conservées :

| Colonne | Type cible | Règle |
| --- | --- | --- |
| `transaction_id` | string | `T` + 9 chiffres, unique par commande |
| `event_time` | timestamp | ISO `yyyy-MM-ddTHH:mm:ss`, UTC |
| `customer_id` | string | doit exister dans le référentiel clients |
| `product_id` | string | doit exister dans le référentiel produits |
| `quantity` | int | > 0 |
| `unit_price_eur` | decimal(10,2) | > 0, prix facturé sur la ligne |
| `channel` | string | `web`, `store`, `partner` |
| `status` | string | `COMPLETED`, `CANCELLED`, `RETURNED` |
| `discount_pct` | decimal(4,3) | **J3 seulement**, 0 à 0.30, ajoutée en dernière colonne |

Prix des lignes synthétiques : prix catalogue de la version du produit valide à `event_time`,
multiplié par `(1 − remise commerciale)` tirée uniformément dans `[0 ; 0,10]`, arrondi au centime.
Les lignes du kit gardent leur prix (indépendant du catalogue, voir plus haut).

Déséquilibre de clé : 20 % des lignes synthétiques portent le produit `P00042`. En profil
`full`, le ratio max/médiane du nombre de lignes par produit **du catalogue** vaut 244,6 (190,8 en
`dev`) ; les identifiants inconnus `P9…` de l'anomalie `UNKNOWN_PRODUCT` sont exclus du calcul, comme
ils le sont de la jointure avec les produits.

Un doublon inter-jours renvoyé dans J3 prend le format de J3 : les 8 valeurs d'origine à
l'identique et un 9ᵉ champ `discount_pct` vide.

Anomalies injectées (taux sur les lignes du dépôt, codes repris dans le manifeste) :

| Code | Taux | Exemple |
| --- | --- | --- |
| `DUPLICATE_EXACT` | 1 % | ligne saine répétée à l'identique dans le même fichier |
| `DUPLICATE_CROSS_DAY` | 0,5 % (J2, J3) | ligne saine d'un dépôt antérieur renvoyée |
| `NULL_CUSTOMER` | 0,5 % | `customer_id` vide |
| `UNKNOWN_CUSTOMER` | 0,3 % | `C999…` absent du référentiel |
| `UNKNOWN_PRODUCT` | 0,3 % | `P9…` absent du référentiel |
| `BAD_QUANTITY` | 0,5 % | `0`, `-3`, `abc`, vide |
| `BAD_PRICE` | 0,2 % | `-12.50`, `0`, vide |
| `BAD_TIMESTAMP` | 0,3 % | `2025/03/04 10:00`, `N/A`, vide |
| `FUTURE_TIMESTAMP` | 0,1 % | `event_time` postérieur à `ingestion_date` + 1 jour |
| `BAD_ENUM` | 0,2 % | `channel` = `fax` ou `status` = `UNKNOWN` |
| `MALFORMED_ROW` | 0,1 % | nombre de champs différent de celui de l'en-tête du fichier, et jamais 8 ni 9 (deux champs de moins, deux de plus, ou `;` comme séparateur) |

## Produits (`products`)

En-tête : `product_id,product_name,category,unit_price_eur,updated_at`

- J1 : les 1 000 produits du kit en version initiale (`updated_at` = `2024-12-31T00:00:00`),
  plus des versions supplémentaires datées de 2025 pour 15 % des produits (variation de prix de
  ±5 à 20 %, changement de `category` pour 3 %). Un même produit apparaît donc plusieurs fois.
- J2, J3 : delta du jour (produits modifiés, `updated_at` dans la journée précédant l'ingestion)
  et quelques nouveaux produits `P01001+`.
- `P00042` change de prix au moins une fois en 2025, pour que le prix applicable à la date de
  commande diffère du prix courant.
- `product_name` vaut `<category> <product_id>` de la version initiale (le kit n'a pas de nom) ; il
  ne change pas quand la catégorie change.

Anomalies, injectées **uniquement sur des versions non initiales** pour que chaque produit garde
toujours une version valide : `DUPLICATE_EXACT` 1 %, `BAD_PRICE` 0,5 %, `NULL_CATEGORY` 0,5 %,
`CONFLICTING_VERSION` 0,2 % (une ligne ajoutée avec le même `product_id` et le même `updated_at`
qu'une version saine, mais un prix différent ; le manifeste compte la ligne ajoutée).

## Clients (`customers`)

En-tête : `customer_id,full_name,email,phone,region,segment,signup_date,updated_at`

- J1 : les 5 000 clients du kit, enrichis de données personnelles synthétiques déterministes
  (`full_name`, `email`, `phone`). `region` garde la valeur du kit ; le pays est le préfixe
  (`FR`, `DE`, `ES`, `IT`, `UK`).
- J2, J3 : nouveaux clients `C005001+` et mises à jour de clients existants (région, segment),
  `updated_at` du jour.

Anomalies : `DUPLICATE_EXACT` 1 %, `DUPLICATE_KEY` 0,5 % (même identifiant, email en casse ou
espaces différents, même `updated_at`), `MISSING_EMAIL` 2 %, `BAD_EMAIL` 0,5 %,
`MISSING_REGION` 1 %, `BAD_DATE` 0,5 % (`27/02/2024`).

## Logs web (`weblogs`)

Format *Combined Log Format*, une requête par ligne :

```
198.51.100.23 - C001315 [01/Jan/2026:20:21:48 +0000] "GET /products/P00042 HTTP/1.1" 200 5123 "-" "Mozilla/5.0 (tablet)"
```

Les lignes dérivent des événements du kit (`events.jsonl`) : `view` → `GET /products/<pid>`,
`search` → `GET /search?q=<terme>`, `cart` → `POST /cart/add?product=<pid>`,
`purchase` → `POST /checkout`. Le kit n'ayant ni produit ni terme dans ses événements, `<pid>` est
tiré parmi les produits du kit et `<terme>` dans une liste fixe, de façon déterministe. Le champ
utilisateur porte `customer_id`, l'agent utilisateur le `device`. J1 rejoue l'historique 2025,
J2 et J3 une journée.

Anomalies : `MALFORMED_LINE` 0,5 % (ligne tronquée, guillemets manquants, texte libre),
`HTTP_404` 2 % (`/products/P99999`, `/old-catalog`), `HTTP_500` 0,5 %. Les 404 et 500 sont des
lignes valides : elles alimentent l'indicateur d'erreurs, elles ne sont pas rejetées.

## Capteurs (`sensors`)

JSON Lines, un fichier par intervalle de 5 secondes (`sensors_<yyyyMMddTHHmmss>.jsonl`).
60 capteurs (5 sites `FR-LYON`, `DE-HAMBURG`, `ES-BILBAO`, `IT-TORINO`, `UK-LEEDS` × 4 machines ×
3 métriques), une lecture de base par capteur
et par fichier, soit 60 événements de base par fichier avant anomalies.

```json
{"event_id":"S20260101-000123","sensor_id":"FR-LYON-M03-TEMP","site":"FR-LYON","machine_id":"FR-LYON-M03","metric":"temperature","value":71.4,"unit":"C","event_time":"2026-01-01T00:00:03.120Z","emitted_at":"2026-01-01T00:00:04.900Z"}
```

| Métrique | Unité | Régime normal | Seuil d'alerte |
| --- | --- | --- | --- |
| `temperature` | °C | 60 ± 8 | > 85 |
| `vibration` | mm/s | 3 ± 1 | > 7,1 (ISO 10816, zone D) |
| `pressure` | bar | 6 ± 0,5 | < 4 ou > 8 |

Anomalies (taux sur les événements de base de la journée) :

| Code | Taux | Définition |
| --- | --- | --- |
| `LATE_EVENT` | 5 % | `event_time` de 10 à 90 s avant l'intervalle du fichier (accepté par le watermark de 2 min) |
| `VERY_LATE_EVENT` | 1 % | `event_time` de 5 à 15 min avant l'intervalle, jamais dans les 36 premiers fichiers du jour (le watermark doit être établi) ; écarté par le watermark |
| `DUPLICATE_EVENT` | 3 % | même `event_id` réémis dans un fichier ultérieur |
| `MALFORMED_JSON` | 0,5 % | ligne JSON illisible |
| `NULL_VALUE` | 0,5 % | `value` à `null` |
| `SPIKE` | 1 machine par site | une métrique au-delà du seuil pendant au moins 2 minutes consécutives (24 fichiers et plus), à partir de la 3ᵉ minute (2ᵉ en `ci`) ; une pression en pic reste du côté haut (> 8 bar) et aucun autre type d'anomalie ne touche le capteur pendant son pic |

Le flux étant fini, les fenêtres des dernières `watermark + fenêtre` minutes (3 min) ne sont
finalisées que si des données plus récentes arrivent ; la démonstration en tient compte.

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
de rejets et de doublons à ces valeurs, selon la table de disposition du modèle de données.

## Précisions d'implémentation (générateur 1.0.0)

Fixées par l'implémentation et vérifiées en revue ; utiles aux auteurs de bronze, silver et
MapReduce.

| Sujet | Valeur |
| --- | --- |
| Identifiants synthétiques | `T000200001` et suivants dans J1 pour tous les profils, puis J2, J3 à la suite ; nouveaux clients `C005001+`, nouveaux produits `P01001+` |
| Deltas quotidiens J2, J3 | 30 mises à jour et 12 nouveaux clients, 10 produits modifiés et 3 nouveaux ; logs web = volume de J1 / 365 |
| Retardataires | environ 3 % des commandes de J2 et J3, datées de 1 à 3 jours avant le jour métier |
| `FUTURE_TIMESTAMP` | `ingestion_date` + 2 à 30 jours |
| `updated_at` des clients de J1 | `2025-12-31T00:00:00` |
| Encodage | UTF-8 sans BOM, fins de ligne LF (les CRLF du kit deviennent LF), aucun champ entre guillemets ; noms avec accents et apostrophes |
| Doublons de clé clients | email avec espaces ou majuscules : lire sans `ignoreLeading/TrailingWhiteSpace` |
| Prix du kit | parfois une seule décimale (`26.9`) |
| Adresses IP des logs | `198.18.0.0/15`, troisième octet par pays : FR 0-49, DE 50-99, ES 100-149, IT 150-199, UK 200-249 |
| Capteurs | lire avec le motif `sensors_*.jsonl` (le `_MANIFEST.json` est dans le même répertoire) ; un doublon recopie un événement sain du fichier précédent (retard < 5 s) |
