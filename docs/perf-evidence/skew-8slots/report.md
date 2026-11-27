`application_1795715756506_0410`, {'spark.executor.instances': '2', 'spark.executor.cores': '4', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}

| Variante | n | Durée médiane (s) | Min à max (s) | Gain | Gain sur la précédente | Octets lus | Gain octets lus | Shuffle lu | Déversé disque | Tâches | Étape clé : max/médiane durée | max/médiane octets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | 5 | 2.39 | 2.05 à 2.53 | 0.0 % | 0.0 % | 109 104 845 | 0.0 % | 142 657 920 | 24 725 968 | 26 | 3.21 | 4.16 |
| `salted-16` | 5 | 2.81 | 2.50 à 3.12 | -17.5 % | -17.5 % | 109 104 845 | 0.0 % | 191 827 999 | 26 095 820 | 26 | 1.79 | 1.49 |
| `aqe-skew-join` | 5 | 2.16 | 1.86 à 2.28 | 9.9 % | 23.4 % | 109 104 845 | 0.0 % | 142 666 187 | 0 | 30 | 1.52 | 1.31 |
| `broadcast` | 5 | 1.07 | 1.02 à 1.33 | 55.4 % | 50.5 % | 109 104 845 | 0.0 % | 676 | 0 | 10 | 1.06 | 1.00 |
