`application_1795715756506_0413`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `no-cache-3` | 5 | 4.20 | 4.09 à 4.44 | 339 872 385 | 311 780 | 0 | 21 | 1.54 | 0.02 | 1.00 | 1.00 | [455](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=455&attempt=0) |
| `cache-3` | 5 | 6.39 | 6.24 à 6.97 | 453 954 039 | 311 780 | 0 | 24 | 4.23 | 0.02 | 1.00 | 1.00 | [243](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=243&attempt=0) |
| `no-cache-6` | 5 | 8.78 | 8.20 à 9.20 | 690 824 703 | 694 030 | 0 | 42 | 1.62 | 0.02 | 1.00 | 1.00 | [270](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=270&attempt=0) |
| `cache-6` | 5 | 8.81 | 8.45 à 9.69 | 783 928 587 | 694 030 | 0 | 42 | 4.23 | 0.02 | 1.00 | 1.00 | [178](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=178&attempt=0) |
| `no-cache-3-sort-merge` | 5 | 9.32 | 8.57 à 10.21 | 339 872 385 | 459 254 803 | 219 010 339 | 37 | 1.04 | 1.54 | 1.09 | 1.09 | [533](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=533&attempt=0) |
| `cache-3-sort-merge` | 5 | 6.89 | 6.70 à 7.32 | 318 382 068 | 251 121 795 | 31 004 877 | 72 | 1.52 | 3.71 | 4.45 | 4.45 | [654](http://localhost:18080/history/application_1795715756506_0413/stages/stage/?id=654&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `no-cache-3` | `cache-3` | 5 contre 5 | 4.20 → 6.39 | +52.0 % | +33.6 % | 25 / 25 | 0.008 | oui | 0.008 | 0.500 |
| `no-cache-6` | `cache-6` | 5 contre 5 | 8.78 → 8.81 | +0.4 % | +13.5 % | 17 / 25 | 0.421 | non | 0.008 | 0.381 |
| `no-cache-3-sort-merge` | `cache-3-sort-merge` | 5 contre 5 | 9.32 → 6.89 | -26.0 % | -6.3 % | 0 / 25 | 0.008 | oui | 0.008 | 0.008 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `no-cache-3` | `cache-3` | 4 contre 4 | 4.28 → 6.35 | +48.3 % | +33.6 % | 16 / 16 | 0.029 | oui | 0.029 | 0.429 | oui | inchangée |
| `no-cache-6` | `cache-6` | 4 contre 4 | 8.59 → 9.09 | +5.9 % | +13.5 % | 12 / 16 | 0.343 | non | 0.029 | 0.371 | non | inchangée |
| `no-cache-3-sort-merge` | `cache-3-sort-merge` | 4 contre 4 | 9.38 → 6.96 | -25.8 % | -6.3 % | 0 / 16 | 0.029 | oui | 0.029 | 0.029 | oui | inchangée |
