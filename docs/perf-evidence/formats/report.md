`application_1795715756506_0407`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}

| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente | Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches | Étape clé : max/médiane durée | max/médiane octets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `csv` | 5 | 5.08 | 4.88 à 5.41 | 0.0 % | 0.0 % | 870 831 538 | 0.0 % | 197 789 | 0 | 10 | 1.06 | 1.00 |
| `parquet` | 5 | 0.61 | 0.58 à 0.79 | 87.9 % | 87.9 % | 93 169 973 | 89.3 % | 139 541 | 0 | 8 | 1.05 | 1.34 |
| `parquet_part` | 5 | 1.10 | 0.95 à 1.27 | 78.3 % | -79.4 % | 7 759 742 | 99.1 % | 37 814 | 0 | 403 | 1.01 | 1.01 |
| `parquet_part_zstd` | 5 | 1.04 | 0.93 à 1.24 | 79.5 % | 5.6 % | 5 872 951 | 99.3 % | 37 898 | 0 | 403 | 1.01 | 1.01 |
| `parquet_part+driver-listing` | 5 | 0.62 | 0.57 à 0.65 | 87.7 % | 40.1 % | 7 759 742 | 99.1 % | 37 814 | 0 | 7 | 1.02 | 1.01 |
| `parquet_part+listed` | 5 | 0.35 | 0.34 à 0.40 | 93.1 % | 43.9 % | 7 759 742 | 99.1 % | 37 814 | 0 | 7 | 1.01 | 1.01 |

| Disposition | Octets logiques | Octets physiques | Gain sur la première | Gain sur la précédente |
| --- | ---: | ---: | ---: | ---: |
| `csv` | 870 503 858 | 2 611 511 574 | 0.0 % | 0.0 % |
| `parquet` | 232 815 264 | 698 445 792 | 73.3 % | 73.3 % |
| `parquet_part` | 231 537 550 | 694 612 650 | 73.4 % | 0.5 % |
| `parquet_part_zstd` | 174 337 367 | 523 012 101 | 80.0 % | 24.7 % |
