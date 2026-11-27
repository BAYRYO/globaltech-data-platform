`application_1795715756506_0411`, {'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '896m', 'spark.sql.shuffle.partitions': '16', 'spark.sql.adaptive.enabled': 'true'}, 1 tour(s) d'échauffement exclu(s)

| Variante | n | Durée médiane (s) | Min à max (s) | Octets lus | Shuffle lu | Déversé disque | Tâches | Étape de lecture (s) | Étape clé (s) | Étape clé : max/médiane durée | max/médiane octets | Étape clé, exécution médiane |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `no-cache` | 5 | 4.24 | 3.99 à 4.64 | 339 872 385 | 311 780 | 0 | 21 | 1.50 | 0.03 | 1.00 | 1.00 | [53](http://localhost:18080/history/application_1795715756506_0411/stages/stage/?id=53&attempt=0) |
| `cache` | 5 | 6.57 | 6.41 à 6.81 | 453 954 039 | 311 780 | 0 | 24 | 4.15 | 0.02 | 1.00 | 1.00 | [134](http://localhost:18080/history/application_1795715756506_0411/stages/stage/?id=134&attempt=0) |

Exécutions chevauchées par une autre application, exclues des statistiques : aucune.

Comparaisons : test exact de Mann-Whitney sur les durées, bilatéral ; une différence est retenue quand p < 0.05.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `no-cache` | `cache` | 5 contre 5 | 4.24 → 6.57 | +55.0 % | +33.6 % | 25 / 25 | 0.008 | oui | 0.008 | 0.452 |

Sensibilité à l'échauffement : les mêmes comparaisons sans l'exécution mesurée 1.

| Référence | Variante | n | Durée médiane (s) | Écart de durée | Écart d'octets lus | U (paires où la variante est plus lente) | p | Retenu | p étape de lecture | p étape clé | Retenu avec l'exécution 1 | Conclusion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `no-cache` | `cache` | 4 contre 4 | 4.16 → 6.54 | +57.1 % | +33.6 % | 16 / 16 | 0.029 | oui | 0.029 | 0.743 | oui | inchangée |
