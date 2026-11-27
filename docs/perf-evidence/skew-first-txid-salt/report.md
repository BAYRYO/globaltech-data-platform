`application_1795715756506_0408`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}

| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente | Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches | Étape clé : max/médiane durée | max/médiane octets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 5 | 2.74 | 2.63 à 3.08 | 0.0 % | 0.0 % | 109 093 455 | 0.0 % | 142 656 609 | 24 720 368 | 23 | 4.09 | 4.16 |
| `salted-16` | 5 | 3.45 | 3.24 à 3.50 | -25.9 % | -25.9 % | 157 223 116 | -44.1 % | 191 840 461 | 0 | 23 | 1.57 | 1.48 |
| `aqe-skew-join` | 5 | 2.58 | 2.44 à 2.74 | 6.0 % | 25.3 % | 109 093 455 | 0.0 % | 142 664 876 | 0 | 27 | 1.46 | 1.31 |
| `broadcast` | 5 | 1.36 | 1.33 à 1.51 | 50.4 % | 47.2 % | 109 093 455 | 0.0 % | 450 | 0 | 7 | 1.03 | 1.34 |
