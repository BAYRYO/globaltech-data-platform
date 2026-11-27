`application_1795715756506_0414`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `sp-4` | 5 | 6.30 | 6.02 à 6.44 | 80 651 911 | 105 516 536 | 81 044 983 | 10 | 5.12 | 1.03 | 1.02 | 1.00 | [38](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=38&attempt=0) |
| `sp-8` | 5 | 6.24 | 5.92 à 6.58 | 80 651 911 | 107 968 334 | 81 044 983 | 14 | 5.19 | 0.98 | 1.12 | 1.00 | [80](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=80&attempt=0) |
| `sp-16` | 5 | 6.29 | 5.84 à 6.39 | 80 651 911 | 110 571 506 | 81 044 983 | 22 | 5.33 | 0.86 | 1.19 | 1.00 | [50](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=50&attempt=0) |
| `sp-64` | 5 | 6.35 | 6.14 à 7.02 | 80 651 911 | 115 193 240 | 81 044 983 | 70 | 5.21 | 1.00 | 1.72 | 1.01 | [56](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=56&attempt=0) |
| `sp-200` | 5 | 6.85 | 6.32 à 7.50 | 80 651 911 | 118 048 664 | 81 044 983 | 206 | 5.69 | 1.10 | 3.15 | 1.02 | [134](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=134&attempt=0) |
| `sp-200-aqe-coalesce` | 5 | 6.47 | 6.38 à 7.49 | 80 651 911 | 118 031 121 | 81 044 983 | 11 | 5.37 | 1.02 | 1.04 | 1.02 | [104](http://localhost:18080/history/application_1795715756506_0414/stages/stage/?id=104&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `sp-16` | `sp-4` | 5 contre 5 | 6.29 → 6.30 | +0.1 % | +0.0 % | 15 / 25 | 0.690 | non | 0.421 | 0.016 |
| `sp-16` | `sp-8` | 5 contre 5 | 6.29 → 6.24 | -0.8 % | +0.0 % | 13 / 25 | 1.000 | non | 0.690 | 0.056 |
| `sp-16` | `sp-64` | 5 contre 5 | 6.29 → 6.35 | +1.0 % | +0.0 % | 16 / 25 | 0.548 | non | 0.548 | 0.056 |
| `sp-16` | `sp-200` | 5 contre 5 | 6.29 → 6.85 | +8.8 % | +0.0 % | 24 / 25 | 0.016 | oui | 0.095 | 0.008 |
| `sp-16` | `sp-200-aqe-coalesce` | 5 contre 5 | 6.29 → 6.47 | +2.9 % | +0.0 % | 24 / 25 | 0.016 | oui | 0.222 | 0.016 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `sp-16` | `sp-4` | 4 contre 4 | 6.28 → 6.28 | +0.1 % | +0.0 % | 10 / 16 | 0.686 | non | 0.686 | 0.057 | non | inchangée |
| `sp-16` | `sp-8` | 4 contre 4 | 6.28 → 6.09 | -2.9 % | +0.0 % | 7 / 16 | 0.886 | non | 0.886 | 0.200 | non | inchangée |
| `sp-16` | `sp-64` | 4 contre 4 | 6.28 → 6.42 | +2.3 % | +0.0 % | 10 / 16 | 0.686 | non | 0.343 | 0.200 | non | inchangée |
| `sp-16` | `sp-200` | 4 contre 4 | 6.28 → 6.92 | +10.2 % | +0.0 % | 16 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `sp-16` | `sp-200-aqe-coalesce` | 4 contre 4 | 6.28 → 6.78 | +8.0 % | +0.0 % | 16 / 16 | 0.029 | oui | 0.114 | 0.057 | oui | inchangée |
