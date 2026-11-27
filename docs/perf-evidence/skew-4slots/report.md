`application_1795715756506_0409`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}

| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente | Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches | Étape clé : max/médiane durée | max/médiane octets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 5 | 2.63 | 2.46 à 3.06 | 0.0 % | 0.0 % | 109 093 455 | 0.0 % | 142 656 609 | 24 722 709 | 23 | 4.16 | 4.16 |
| `salted-16` | 5 | 3.17 | 2.99 à 3.29 | -20.4 % | -20.4 % | 109 093 455 | 0.0 % | 191 826 948 | 0 | 23 | 1.63 | 1.49 |
| `aqe-skew-join` | 5 | 2.63 | 2.59 à 2.87 | -0.0 % | 16.9 % | 109 093 455 | 0.0 % | 142 664 876 | 0 | 27 | 1.50 | 1.31 |
| `broadcast` | 5 | 1.37 | 1.32 à 1.47 | 48.0 % | 48.0 % | 109 093 455 | 0.0 % | 450 | 0 | 7 | 1.01 | 1.34 |
