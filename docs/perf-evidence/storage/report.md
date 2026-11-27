| Fichier | Mode | Logique | Physique | Surcoût | Blocs ou groupes | Unités par bloc |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `/benchmarks/rep3/orders_20260101.csv` | rep3 | 713 401 398 | 2 140 204 194 | 200.0 % | 6 | [3] |
| `/benchmarks/rep3/products_20260101.csv` | rep3 | 79 966 | 239 898 | 200.0 % | 1 | [3] |
| `/benchmarks/ec/orders_20260101.csv` | ec | 713 401 398 | 1 070 286 956 | 50.0 % | 3 | [3] |
| `/benchmarks/ec/products_20260101.csv` | ec | 79 966 | 159 932 | 100.0 % | 1 | [2] |

| Mode | copie : médiane, min à max (s) | lecture : médiane, min à max (s) | lecture du petit fichier : médiane, min à max (s) |
| --- | ---: | ---: | ---: |
| rep3 | 3.71, 3.04 à 6.57 | 1.77, 1.69 à 2.07 | 1.14, 1.13 à 1.16 |
| ec | 3.39, 3.07 à 4.11 | 1.69, 1.59 à 1.76 | 1.20, 1.13 à 1.32 |
| p bilatéral (rep3 contre ec) | 0.310 | 0.095 | 0.151 |
