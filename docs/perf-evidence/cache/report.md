`application_1795715756506_0413`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}

| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente | Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches | Étape clé : max/médiane durée | max/médiane octets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `no-cache-3` | 5 | 4.20 | 4.09 à 4.44 | 0.0 % | 0.0 % | 339 872 385 | 0.0 % | 311 780 | 0 | 21 | 1.00 | 1.00 |
| `cache-3` | 5 | 6.39 | 6.24 à 6.97 | -52.0 % | -52.0 % | 453 954 039 | -33.6 % | 311 780 | 0 | 24 | 1.00 | 1.00 |
| `no-cache-6` | 5 | 8.78 | 8.20 à 9.20 | -108.8 % | -37.3 % | 690 824 703 | -103.3 % | 694 030 | 0 | 42 | 1.00 | 1.00 |
| `cache-6` | 5 | 8.81 | 8.45 à 9.69 | -109.5 % | -0.4 % | 783 928 587 | -130.7 % | 694 030 | 0 | 42 | 1.00 | 1.00 |
| `no-cache-3-sort-merge` | 5 | 9.32 | 8.57 à 10.21 | -121.7 % | -5.8 % | 339 872 385 | 0.0 % | 459 254 803 | 219 010 339 | 37 | 1.09 | 1.09 |
| `cache-3-sort-merge` | 5 | 6.89 | 6.70 à 7.32 | -64.0 % | 26.0 % | 318 382 068 | 6.3 % | 251 121 795 | 31 004 877 | 72 | 4.45 | 4.45 |
