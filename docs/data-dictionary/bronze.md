# Dictionnaire de données bronze

Le job `bronze` écrit une table Parquet (compression snappy) par source batch :
`/datalake/bronze/<source>/ingestion_date=AAAA-MM-JJ/`, avec `<source>` parmi `orders`,
`customers`, `products` et `weblogs`. Chaque ligne d'un fichier landing donne exactement une
ligne bronze, sans typage ni filtrage. Un fichier n'est ingéré que s'il a le SHA-256 de son
manifeste et qu'il est en UTF-8. L'exploitation (ingestion, rejeu, vérification) est décrite
dans le [runbook bronze](../runbook/bronze.md).

Les colonnes de données sont celles de l'en-tête du fichier lu, en `string`. La liste des
colonnes attendues par source est `SOURCES` dans `src/gtdp/schemas.py`. L'en-tête d'un fichier
CSV doit toutes les nommer, sauf `discount_pct`, apparue en J3. Le schéma de lecture complet est
`bronze_schema(source)` du même module. `read_table(spark, chemin, bronze_schema(source))` rend
toutes ses colonnes : une colonne absente d'une partition y est nulle, une colonne inconnue est
écartée.

## Colonnes techniques, communes aux quatre tables

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `ingestion_date` | date | Colonne de partition. Date du dépôt, valeur de `--date` du job. | `2026-01-03` |
| `_corrupt_record` | string | Ligne brute, sans fin de ligne, quand son nombre de champs diffère de celui de l'en-tête de son fichier. Nulle sinon. Toujours nulle pour `weblogs`, qui n'est pas découpé en champs. | `T000991575;2026-01-01T03:49:03;C001082;P00105;1;960.26;partner;COMPLETED` |
| `_source_file` | string | URI du fichier landing lu, telle que la configuration la forme. | `hdfs://namenode:8020/datalake/landing/orders/ingestion_date=2026-01-03/orders_20260103.csv` |
| `_ingested_at` | timestamp | Début de l'exécution en UTC, égal à `started_at` de sa ligne d'audit. Le même pour toutes les lignes d'une exécution. | `2026-11-26 18:32:15.431725` |
| `_run_id` | string | Identifiant de l'exécution qui a écrit la partition, clé `run_id` de la table d'audit. | `20261126T183215Z-cb55873f` |
| `_record_hash` | string | SHA-256 hexadécimal de la ligne brute sans fin de ligne. Pour une ligne corrompue, la ligne est `_corrupt_record`. Pour une autre ligne CSV, ce sont les champs joints par `,`, un champ nul redevenant vide. Le lecteur CSV lit sans guillemets, donc ces champs joints redonnent la ligne du fichier octet pour octet. Il ne dépend que de la ligne. Un rejeu donne le même hash, deux lignes identiques aussi, deux lignes différentes jamais. | `ca42e3cbaba45f49118b75799bbe546b0e519dc3426589d5e02239f0ca569553` |

L'exemple de `_record_hash` est le hash de la première ligne de données de
`orders_20260103.csv` :

```bash
printf 'T000992700,2026-01-02T00:11:16,C003408,P00390,10,902.08,partner,COMPLETED,0.218' | sha256sum
```

## Valeurs vides et lignes corrompues

| Cas dans le fichier | En bronze |
| --- | --- |
| Champ vide (`a,,c`) | `null` dans la colonne, `_corrupt_record` nul |
| Champ entouré d'espaces (` x@example.com`) | valeur gardée avec ses espaces |
| Champ entre guillemets (`"Jean"`, `""`, `"a""b"`) | valeur gardée avec ses guillemets, `_corrupt_record` nul |
| Virgule entre guillemets (`"Dupont, Jean"`) | un champ de plus que l'en-tête, donc ligne corrompue |
| Champs en moins que l'en-tête | colonnes manquantes à `null`, ligne entière dans `_corrupt_record` |
| Champs en plus | colonnes de l'en-tête remplies dans l'ordre, champs en trop perdus des colonnes mais présents dans `_corrupt_record` |
| Autre séparateur (`;`) sur une ligne | un seul champ, donc ligne corrompue |
| Autre séparateur dans tout le fichier, en-tête compris | l'en-tête ne nomme pas les colonnes de la source : le job échoue sans rien écrire |
| Octets hors UTF-8, ou octet NUL dans un fichier CSV | le job échoue sans rien écrire |

## `orders`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `transaction_id` | string | Texte brut. Le contrat prévoit `T` suivi de 9 chiffres. | `T000992700` |
| `event_time` | string | Texte brut. Le contrat prévoit `yyyy-MM-ddTHH:mm:ss` en UTC. | `2026-01-02T00:11:16` |
| `customer_id` | string | Texte brut, nul si vide. | `C003408` |
| `product_id` | string | Texte brut. | `P00390` |
| `quantity` | string | Texte brut, non converti (`abc` et `-3` restent tels quels). | `10` |
| `unit_price_eur` | string | Texte brut, non converti. | `902.08` |
| `channel` | string | Texte brut. | `partner` |
| `status` | string | Texte brut. | `COMPLETED` |
| `discount_pct` | string | Présente dans les seules partitions dont le fichier a cette colonne (J3). Nulle pour un doublon inter-jours renvoyé dans J3 avec un champ vide. Lue avec `bronze_schema` ou `mergeSchema`, nulle pour J1 et J2. Seule colonne de `SOURCES` qu'un en-tête peut omettre. | `0.218` |

## `customers`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `customer_id` | string | Texte brut. | `C005004` |
| `full_name` | string | Texte brut, donnée personnelle en clair (la pseudonymisation a lieu en silver). | `Alejandro González` |
| `email` | string | Texte brut, espaces et casse gardés, donnée personnelle en clair. | `alejandro.gonzalez5004@example.com` |
| `phone` | string | Texte brut, donnée personnelle en clair. | `+34696478353` |
| `region` | string | Texte brut. | `ES-CAT` |
| `segment` | string | Texte brut. | `SMB` |
| `signup_date` | string | Texte brut, non converti (`27/02/2024` reste tel quel). | `2026-01-01` |
| `updated_at` | string | Texte brut. | `2026-01-01T00:39:42` |

## `products`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `product_id` | string | Texte brut. Un produit peut avoir plusieurs lignes (versions). | `P00032` |
| `product_name` | string | Texte brut. | `Lighting P00032` |
| `category` | string | Texte brut, nul si vide. | `Lighting` |
| `unit_price_eur` | string | Texte brut, non converti. | `434.52` |
| `updated_at` | string | Texte brut. | `2026-01-01T09:24:20` |

## `weblogs`

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `value` | string | Ligne entière du fichier, sans fin de ligne. Le découpage du Combined Log Format a lieu en silver. | `198.18.79.174 - C004523 [01/Jan/2026:00:10:24 +0000] "POST /checkout HTTP/1.1" 200 2842 "-" "Mozilla/5.0 (desktop)"` |

## Colonnes inconnues

Une colonne d'en-tête absente de `SOURCES` est gardée telle quelle dans la partition de son
fichier et listée dans `metrics_json.unknown_columns` de la ligne d'audit
([ADR-0007](../adr/0007-evolution-schema.md)). Silver ne la lit pas.
