`application_1795715756506_0412`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `no-cache-3` | 5 | 3.85 | 3.76 à 4.88 | 339 872 385 | 311 780 | 0 | 21 | 1.36 | 0.02 | 1.00 | 1.00 | [275](http://localhost:18080/history/application_1795715756506_0412/stages/stage/?id=275&attempt=0) |
| `cache-3` | 5 | 6.12 | 5.86 à 7.05 | 453 954 039 | 311 780 | 0 | 24 | 3.99 | 0.02 | 1.00 | 1.00 | [287](http://localhost:18080/history/application_1795715756506_0412/stages/stage/?id=287&attempt=0) |
| `no-cache-6` | 5 | 7.83 | 7.70 à 8.89 | 690 824 703 | 694 030 | 0 | 42 | 1.51 | 0.03 | 1.00 | 1.00 | [381](http://localhost:18080/history/application_1795715756506_0412/stages/stage/?id=381&attempt=0) |
| `cache-6` | 5 | 8.35 | 8.03 à 8.97 | 783 928 587 | 694 030 | 0 | 42 | 4.04 | 0.02 | 1.00 | 1.00 | [334](http://localhost:18080/history/application_1795715756506_0412/stages/stage/?id=334&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `no-cache-3` | `cache-3` | 5 contre 5 | 3.85 → 6.12 | +58.8 % | +33.6 % | 25 / 25 | 0.008 | oui | 0.008 | 0.810 |
| `no-cache-6` | `cache-6` | 5 contre 5 | 7.83 → 8.35 | +6.7 % | +13.5 % | 21 / 25 | 0.095 | non | 0.008 | 0.333 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `no-cache-3` | `cache-3` | 4 contre 4 | 3.84 → 6.01 | +56.6 % | +33.6 % | 16 / 16 | 0.029 | oui | 0.029 | 1.000 | oui | inchangée |
| `no-cache-6` | `cache-6` | 4 contre 4 | 7.82 → 8.29 | +6.0 % | +13.5 % | 16 / 16 | 0.029 | oui | 0.029 | 0.143 | non | change |
