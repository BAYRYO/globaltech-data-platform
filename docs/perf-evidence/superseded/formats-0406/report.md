`application_1795715756506_0406`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `csv` | 5 | 5.09 | 4.75 à 5.37 | 870 831 538 | 197 789 | 0 | 10 | 4.91 | 4.91 | 1.03 | 1.00 | [112](http://localhost:18080/history/application_1795715756506_0406/stages/stage/?id=112&attempt=0) |
| `parquet` | 5 | 0.69 | 0.62 à 0.90 | 93 169 973 | 139 541 | 0 | 8 | 0.47 | 0.47 | 1.05 | 1.34 | [73](http://localhost:18080/history/application_1795715756506_0406/stages/stage/?id=73&attempt=0) |
| `parquet_part` | 5 | 1.23 | 1.01 à 1.39 | 7 759 742 | 37 814 | 0 | 403 | 0.25 | 0.25 | 1.00 | 1.01 | [79](http://localhost:18080/history/application_1795715756506_0406/stages/stage/?id=79&attempt=0) |
| `parquet_part_zstd` | 5 | 1.06 | 0.97 à 1.20 | 5 872 951 | 37 898 | 0 | 403 | 0.23 | 0.23 | 1.00 | 1.01 | [85](http://localhost:18080/history/application_1795715756506_0406/stages/stage/?id=85&attempt=0) |

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
| `csv` | `parquet` | 5 contre 5 | 5.09 → 0.69 | -86.5 % | -89.3 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |
| `parquet` | `parquet_part` | 5 contre 5 | 0.69 → 1.23 | +79.3 % | -91.7 % | 25 / 25 | 0.008 | oui | 0.008 | 0.008 |
| `parquet_part` | `parquet_part_zstd` | 5 contre 5 | 1.23 → 1.06 | -13.9 % | -24.3 % | 8 / 25 | 0.421 | non | 0.222 | 0.222 |
| `csv` | `parquet_part_zstd` | 5 contre 5 | 5.09 → 1.06 | -79.1 % | -99.3 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `csv` | `parquet` | 4 contre 4 | 5.18 → 0.67 | -87.0 % | -89.3 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `parquet` | `parquet_part` | 4 contre 4 | 0.67 → 1.14 | +68.8 % | -91.7 % | 16 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `parquet_part` | `parquet_part_zstd` | 4 contre 4 | 1.14 → 1.06 | -6.9 % | -24.3 % | 6 / 16 | 0.686 | non | 0.057 | 0.057 | non | inchangée |
| `csv` | `parquet_part_zstd` | 4 contre 4 | 5.18 → 1.06 | -79.6 % | -99.3 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
