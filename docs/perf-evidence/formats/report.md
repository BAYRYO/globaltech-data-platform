`application_1795715756506_0407`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `csv` | 5 | 5.08 | 4.88 à 5.41 | 870 831 538 | 197 789 | 0 | 10 | 4.91 | 4.91 | 1.06 | 1.00 | [131](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=131&attempt=0) |
| `parquet` | 5 | 0.61 | 0.58 à 0.79 | 93 169 973 | 139 541 | 0 | 8 | 0.42 | 0.42 | 1.05 | 1.34 | [136](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=136&attempt=0) |
| `parquet_part` | 5 | 1.10 | 0.95 à 1.27 | 7 759 742 | 37 814 | 0 | 403 | 0.26 | 0.26 | 1.01 | 1.01 | [78](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=78&attempt=0) |
| `parquet_part_zstd` | 5 | 1.04 | 0.93 à 1.24 | 5 872 951 | 37 898 | 0 | 403 | 0.24 | 0.24 | 1.01 | 1.01 | [116](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=116&attempt=0) |
| `parquet_part+driver-listing` | 5 | 0.62 | 0.57 à 0.65 | 7 759 742 | 37 814 | 0 | 7 | 0.21 | 0.21 | 1.02 | 1.01 | [89](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=89&attempt=0) |
| `parquet_part+listed` | 5 | 0.35 | 0.34 à 0.40 | 7 759 742 | 37 814 | 0 | 7 | 0.23 | 0.23 | 1.01 | 1.01 | [158](http://localhost:18080/history/application_1795715756506_0407/stages/stage/?id=158&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

| Disposition | Octets logiques | Octets physiques | Écart sur la première | Écart sur la précédente |
| --- | ---: | ---: | ---: | ---: |
| `csv` | 870 503 858 | 2 611 511 574 | +0.0 % | +0.0 % |
| `parquet` | 232 815 264 | 698 445 792 | -73.3 % | -73.3 % |
| `parquet_part` | 231 537 550 | 694 612 650 | -73.4 % | -0.5 % |
| `parquet_part_zstd` | 174 337 367 | 523 012 101 | -80.0 % | -24.7 % |

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `csv` | `parquet` | 5 contre 5 | 5.08 → 0.61 | -87.9 % | -89.3 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |
| `parquet` | `parquet_part` | 5 contre 5 | 0.61 → 1.10 | +79.4 % | -91.7 % | 25 / 25 | 0.008 | oui | 0.008 | 0.008 |
| `parquet_part` | `parquet_part_zstd` | 5 contre 5 | 1.10 → 1.04 | -5.6 % | -24.3 % | 11 / 25 | 0.841 | non | 0.690 | 0.690 |
| `csv` | `parquet_part_zstd` | 5 contre 5 | 5.08 → 1.04 | -79.5 % | -99.3 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |
| `parquet` | `parquet_part+driver-listing` | 5 contre 5 | 0.61 → 0.62 | +1.4 % | -91.7 % | 10 / 25 | 0.690 | non | 0.008 | 0.008 |
| `parquet` | `parquet_part+listed` | 5 contre 5 | 0.61 → 0.35 | -43.1 % | -91.7 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `csv` | `parquet` | 4 contre 4 | 5.02 → 0.61 | -87.8 % | -89.3 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `parquet` | `parquet_part` | 4 contre 4 | 0.61 → 1.07 | +74.5 % | -91.7 % | 16 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `parquet_part` | `parquet_part_zstd` | 4 contre 4 | 1.07 → 1.02 | -4.1 % | -24.3 % | 7 / 16 | 0.886 | non | 0.886 | 0.886 | non | inchangée |
| `csv` | `parquet_part_zstd` | 4 contre 4 | 5.02 → 1.02 | -79.7 % | -99.3 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `parquet` | `parquet_part+driver-listing` | 4 contre 4 | 0.61 → 0.61 | -0.4 % | -91.7 % | 7 / 16 | 0.886 | non | 0.029 | 0.029 | non | inchangée |
| `parquet` | `parquet_part+listed` | 4 contre 4 | 0.61 → 0.35 | -43.0 % | -91.7 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
