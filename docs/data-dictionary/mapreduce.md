# Dictionnaire de données : MapReduce contre Spark (Lot 6)

Sorties du calcul « CA facturé par pays et lignes invalides » des deux moteurs. Ce ne sont pas des
tables du lake : des fichiers texte, séparateur tabulation, sans en-tête, un fichier par sortie,
identiques d'un moteur à l'autre pour faciliter la comparaison. Définition du calcul :
[MapReduce contre Spark](../mapreduce-vs-spark.md#le-calcul).

## Chiffre d'affaires par pays

Hadoop Streaming : `<répertoire de sortie>/part-00000`. Spark :
`<paths.ops>/mr_vs_spark/spark/revenue/part-*.txt`. Une ligne par pays, triée par pays.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `country` | string | préfixe de la région du client avant `-` (ligne client de plus grand `updated_at`, puis de plus grande région) ; `UNKNOWN` si la région est vide | `FR` |
| `revenue_eur` | decimal, 2 décimales | somme exacte de `quantity × unit_price_eur × (1 − discount_pct)` sur les lignes valides `COMPLETED` du pays, arrondie au centime (demi supérieur) une fois, sur le total | `1767230531.51` |
| `completed_lines` | long | nombre de lignes valides `COMPLETED` du pays | `389235` |

## Compteurs

Hadoop Streaming : compteurs du job (`reporter:counter:<groupe>,<nom>,<n>`), lus dans JobHistory.
Spark : `<paths.ops>/mr_vs_spark/spark/counters/part-*.txt`, et `metrics_json.counters` de la
ligne d'audit. Un compteur nul n'apparaît pas.

| Colonne | Type | Règle | Exemple |
| --- | --- | --- | --- |
| `group` | string | `Quality` : ligne invalide ; `Lines` : en-tête ou ligne valide | `Quality` |
| `name` | string | `Quality` : motif de la première règle en échec ; `Lines` : `HEADER`, `COMPLETED`, `OTHER_STATUS` | `BAD_PRICE` |
| `value` | long | nombre de lignes | `2010` |

Motifs `Quality`, dans l'ordre d'évaluation : `MALFORMED_ROW`, `BAD_QUANTITY`, `BAD_PRICE`,
`BAD_TIMESTAMP`, `NULL_CUSTOMER`, `UNKNOWN_CUSTOMER`, `BAD_ENUM`, `BAD_DISCOUNT` (conditions dans
[MapReduce contre Spark](../mapreduce-vs-spark.md#règles-des-lignes-invalides)). Chaque ligne des
fichiers lus compte dans exactement un compteur.

## Audit (job Spark)

| Colonne de l'audit | Valeur |
| --- | --- |
| `rows_in` | lignes de commandes lues, en-têtes exclus |
| `rows_rejected` | somme des compteurs `Quality` |
| `rows_out` | nombre de pays écrits |
| `metrics_json` | `{"counters": {"Quality.BAD_PRICE": 2010, "Lines.HEADER": 3, …}}` |
