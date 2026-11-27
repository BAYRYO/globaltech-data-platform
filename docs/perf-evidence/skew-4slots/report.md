`application_1795715756506_0409`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 5 | 2.63 | 2.46 à 3.06 | 109 093 455 | 142 656 609 | 24 722 709 | 23 | 0.91 | 1.65 | 4.16 | 4.16 | [42](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=42&attempt=0) |
| `salted-16` | 5 | 3.17 | 2.99 à 3.29 | 109 093 455 | 191 826 948 | 0 | 23 | 1.06 | 1.95 | 1.63 | 1.49 | [26](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=26&attempt=0) |
| `aqe-skew-join` | 5 | 2.63 | 2.59 à 2.87 | 109 093 455 | 142 664 876 | 0 | 27 | 0.93 | 1.58 | 1.50 | 1.31 | [92](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=92&attempt=0) |
| `broadcast` | 5 | 1.37 | 1.32 à 1.47 | 109 093 455 | 450 | 0 | 7 | 1.25 | 1.25 | 1.01 | 1.34 | [118](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=118&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `baseline` | `salted-16` | 5 contre 5 | 2.63 → 3.17 | +20.4 % | +0.0 % | 24 / 25 | 0.016 | oui | 0.056 | 0.008 |
| `baseline` | `aqe-skew-join` | 5 contre 5 | 2.63 → 2.63 | +0.0 % | +0.0 % | 14 / 25 | 0.841 | non | 0.421 | 1.000 |
| `baseline` | `broadcast` | 5 contre 5 | 2.63 → 1.37 | -48.0 % | +0.0 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `baseline` | `salted-16` | 4 contre 4 | 2.58 → 3.17 | +23.0 % | +0.0 % | 16 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
| `baseline` | `aqe-skew-join` | 4 contre 4 | 2.58 → 2.62 | +1.4 % | +0.0 % | 10 / 16 | 0.686 | non | 0.200 | 1.000 | non | inchangée |
| `baseline` | `broadcast` | 4 contre 4 | 2.58 → 1.36 | -47.3 % | +0.0 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
