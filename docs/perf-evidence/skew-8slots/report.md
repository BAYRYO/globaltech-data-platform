`application_1795715756506_0410`, {'spark.executor.instances': '2', 'spark.executor.cores': '4', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 5 | 2.39 | 2.05 à 2.53 | 109 104 845 | 142 657 920 | 24 725 968 | 26 | 0.52 | 1.69 | 3.21 | 4.16 | [82](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=82&attempt=0) |
| `salted-16` | 5 | 2.81 | 2.50 à 3.12 | 109 104 845 | 191 827 999 | 26 095 820 | 26 | 0.74 | 1.79 | 1.79 | 1.49 | [46](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=46&attempt=0) |
| `aqe-skew-join` | 5 | 2.16 | 1.86 à 2.28 | 109 104 845 | 142 666 187 | 0 | 30 | 0.56 | 1.42 | 1.52 | 1.31 | [92](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=92&attempt=0) |
| `broadcast` | 5 | 1.07 | 1.02 à 1.33 | 109 104 845 | 676 | 0 | 10 | 0.92 | 0.92 | 1.06 | 1.00 | [98](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=98&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | `salted-16` | 5 contre 5 | 2.39 → 2.81 | +17.5 % | +0.0 % | 24 / 25 | 0.016 | oui | 0.008 | 0.222 |
| `baseline` | `aqe-skew-join` | 5 contre 5 | 2.39 → 2.16 | -9.9 % | +0.0 % | 3 / 25 | 0.056 | non | 0.040 | 0.032 |
| `baseline` | `broadcast` | 5 contre 5 | 2.39 → 1.07 | -55.4 % | +0.0 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `baseline` | `salted-16` | 4 contre 4 | 2.34 → 2.86 | +21.9 % | +0.0 % | 15 / 16 | 0.057 | non | 0.029 | 0.114 | oui | change |
| `baseline` | `aqe-skew-join` | 4 contre 4 | 2.34 → 2.17 | -7.3 % | +0.0 % | 3 / 16 | 0.200 | non | 0.057 | 0.114 | non | inchangée |
| `baseline` | `broadcast` | 4 contre 4 | 2.34 → 1.11 | -52.6 % | +0.0 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
