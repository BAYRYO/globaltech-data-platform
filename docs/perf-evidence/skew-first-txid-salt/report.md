`application_1795715756506_0408`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 5 | 2.74 | 2.63 à 3.08 | 109 093 455 | 142 656 609 | 24 720 368 | 23 | 0.91 | 1.67 | 4.09 | 4.16 | [102](http://localhost:18080/history/application_1795715756506_0408/stages/stage/?id=102&attempt=0) |
| `salted-16` | 5 | 3.45 | 3.24 à 3.50 | 157 223 116 | 191 840 461 | 0 | 23 | 1.32 | 1.93 | 1.57 | 1.48 | [46](http://localhost:18080/history/application_1795715756506_0408/stages/stage/?id=46&attempt=0) |
| `aqe-skew-join` | 5 | 2.58 | 2.44 à 2.74 | 109 093 455 | 142 664 876 | 0 | 27 | 0.98 | 1.49 | 1.46 | 1.31 | [112](http://localhost:18080/history/application_1795715756506_0408/stages/stage/?id=112&attempt=0) |
| `broadcast` | 5 | 1.36 | 1.33 à 1.51 | 109 093 455 | 450 | 0 | 7 | 1.21 | 1.21 | 1.03 | 1.34 | [78](http://localhost:18080/history/application_1795715756506_0408/stages/stage/?id=78&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | `salted-16` | 5 contre 5 | 2.74 → 3.45 | +25.9 % | +44.1 % | 25 / 25 | 0.008 | oui | 0.008 | 0.151 |
| `baseline` | `aqe-skew-join` | 5 contre 5 | 2.74 → 2.58 | -6.0 % | +0.0 % | 3 / 25 | 0.056 | non | 0.389 | 0.016 |
| `baseline` | `broadcast` | 5 contre 5 | 2.74 → 1.36 | -50.4 % | +0.0 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `baseline` | `salted-16` | 4 contre 4 | 2.73 → 3.40 | +24.7 % | +44.1 % | 16 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `baseline` | `aqe-skew-join` | 4 contre 4 | 2.73 → 2.58 | -5.4 % | +0.0 % | 1 / 16 | 0.057 | non | 0.686 | 0.029 | non | inchangée |
| `baseline` | `broadcast` | 4 contre 4 | 2.73 → 1.35 | -50.5 % | +0.0 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
