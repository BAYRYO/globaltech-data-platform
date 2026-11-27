# MapReduce contre Spark : mesures

5 exécutions par variante en alternance, le 2026-11-27 02:42 UTC, par `scripts/mapreduce/compare.sh` au commit `32b18ba`. Réglages de `mapreduce-tuned` : `yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms=100`.

## Volume `ci`

Entrée : `hdfs://namenode:8020/benchmarks/mr-vs-spark/ci/landing`, partitions jusqu'au 2026-01-03 :

- `/customers/ingestion_date=2026-01-01` : 548686 octets
- `/customers/ingestion_date=2026-01-02` : 4864 octets
- `/customers/ingestion_date=2026-01-03` : 4847 octets
- `/orders/ingestion_date=2026-01-01` : 1426354 octets
- `/orders/ingestion_date=2026-01-02` : 3979 octets
- `/orders/ingestion_date=2026-01-03` : 4338 octets

### Exécutions

| Variante | Exécution | Application | Durée (s) | Dont jobs (s) | CPU compté (s) | Mo·s | Cœurs·s | Lignes lues | Octets lus | Octets de shuffle | Tâches | Exclusive | Contrôle YARN avant l'exécution |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| mapreduce | 1 | `application_1795715756506_0360` | 11.9 | 9.4 | 3.6 | 21981 | 19.9 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:14:53+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 1 | `application_1795715756506_0361` | 11.3 | 8.2 | 3.1 | 18613 | 21.6 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:15:25+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 1 | `application_1795715756506_0362` | 12.7 | 5.6 | 3.3 | 46434 | 46.3 | 25279 | 1993068 | 60121 | 11 | oui | 2026-11-27T02:15:56+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 2 | `application_1795715756506_0363` | 11.2 | 8.5 | 3.5 | 19581 | 19.0 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:16:27+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 2 | `application_1795715756506_0364` | 10.5 | 7.5 | 3.3 | 17902 | 20.3 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:16:59+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 2 | `application_1795715756506_0365` | 12.7 | 5.2 | 3.7 | 43511 | 43.1 | 25279 | 1993068 | 60121 | 11 | oui | 2026-11-27T02:17:29+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 3 | `application_1795715756506_0366` | 11.4 | 8.4 | 3.2 | 19430 | 19.0 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:17:58+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 3 | `application_1795715756506_0367` | 9.8 | 6.9 | 4.1 | 18100 | 20.7 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:18:30+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 3 | `application_1795715756506_0368` | 12.4 | 5.3 | 3.5 | 38148 | 44.7 | 25279 | 1993068 | 60121 | 11 | oui | 2026-11-27T02:18:59+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 4 | `application_1795715756506_0369` | 11.4 | 8.6 | 3.2 | 19073 | 19.2 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:19:30+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 4 | `application_1795715756506_0370` | 9.8 | 6.7 | 3.3 | 17151 | 19.5 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:20:03+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 4 | `application_1795715756506_0371` | 13.5 | 5.7 | 3.5 | 40792 | 48.3 | 25279 | 1993068 | 60121 | 11 | oui | 2026-11-27T02:20:31+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 5 | `application_1795715756506_0372` | 11.4 | 8.4 | 3.5 | 20622 | 19.6 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:21:04+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 5 | `application_1795715756506_0373` | 9.6 | 6.6 | 3.5 | 17277 | 19.5 | 20113 | 1438868 | 491 | 5 | oui | 2026-11-27T02:21:36+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 5 | `application_1795715756506_0374` | 16.8 | 6.7 | 3.9 | 48487 | 56.9 | 25279 | 1993068 | 60121 | 11 | oui | 2026-11-27T02:22:05+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |

### Synthèse

| Variante | n | Durée médiane (s) | Min | Max | Écart (max - min) / médiane | Mo·s médian | Cœurs·s médian | CPU fixe par tâche (s) | CPU par M lignes (s) | Durée fixe par tâche (s) | Durée par M lignes (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mapreduce | 5 | 11.4 | 11.2 | 11.9 | 6% | 19581 | 19.2 | 0.45 | 47.81 | 1.69 | -23.14 |
| mapreduce-tuned | 5 | 9.8 | 9.6 | 11.3 | 18% | 17902 | 20.3 | 0.52 | 42.11 | 1.90 | -19.19 |
| spark | 5 | 12.7 | 12.4 | 16.8 | 35% | 43511 | 46.3 | 0.12 | 5.14 | 0.21 | 6.29 |

### Test de Mann-Whitney exact sur la durée

| A | B | n (A, B) | U (paires où A est plus lent) | p unilatéral (A plus lent) | p bilatéral |
| --- | --- | --- | ---: | ---: | ---: |
| mapreduce | mapreduce-tuned | 5, 5 | 24 / 25 | 0.008 | 0.016 |
| mapreduce | spark | 5, 5 | 0 / 25 | 1.000 | 0.008 |
| mapreduce-tuned | spark | 5, 5 | 0 / 25 | 1.000 | 0.008 |

### Résultat de référence

| Pays | CA facturé (EUR) | Lignes COMPLETED |
| --- | ---: | ---: |
| DE | 13073794.01 | 2816 |
| ES | 12886496.67 | 2695 |
| FR | 35648576.79 | 7686 |
| IT | 11680592.98 | 2478 |
| UK | 12020796.81 | 2583 |
| UNKNOWN | 983417.77 | 211 |

| Compteur | Valeur |
| --- | ---: |
| Lines.COMPLETED | 18469 |
| Lines.HEADER | 3 |
| Lines.OTHER_STATUS | 1207 |
| Quality.BAD_ENUM | 42 |
| Quality.BAD_PRICE | 42 |
| Quality.BAD_QUANTITY | 102 |
| Quality.BAD_TIMESTAMP | 62 |
| Quality.MALFORMED_ROW | 22 |
| Quality.NULL_CUSTOMER | 102 |
| Quality.UNKNOWN_CUSTOMER | 62 |

### Vérifications

Toutes les exécutions donnent la même sortie octet pour octet et les mêmes compteurs ; les compteurs couvrent toutes les lignes lues.
Les compteurs Quality sont égaux aux anomalies des manifestes.

## Volume `dev`

Entrée : `hdfs://namenode:8020/datalake/landing`, partitions jusqu'au 2026-01-03 :

- `/customers/ingestion_date=2026-01-01` : 548686 octets
- `/customers/ingestion_date=2026-01-02` : 4864 octets
- `/customers/ingestion_date=2026-01-03` : 4847 octets
- `/orders/ingestion_date=2026-01-01` : 71331093 octets
- `/orders/ingestion_date=2026-01-02` : 195812 octets
- `/orders/ingestion_date=2026-01-03` : 211899 octets

### Exécutions

| Variante | Exécution | Application | Durée (s) | Dont jobs (s) | CPU compté (s) | Mo·s | Cœurs·s | Lignes lues | Octets lus | Octets de shuffle | Tâches | Exclusive | Contrôle YARN avant l'exécution |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| mapreduce | 1 | `application_1795715756506_0375` | 13.8 | 10.7 | 5.3 | 24569 | 26.8 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:22:45+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 1 | `application_1795715756506_0376` | 10.5 | 7.8 | 5.2 | 20398 | 24.3 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:23:20+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 1 | `application_1795715756506_0377` | 16.5 | 7.9 | 6.5 | 56710 | 60.8 | 1010649 | 72363107 | 61723 | 12 | oui | 2026-11-27T02:23:50+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 2 | `application_1795715756506_0378` | 13.7 | 10.7 | 4.9 | 24475 | 26.4 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:24:29+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 2 | `application_1795715756506_0379` | 10.8 | 7.5 | 5.1 | 20874 | 25.1 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:25:04+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 2 | `application_1795715756506_0380` | 15.5 | 6.9 | 6.5 | 53326 | 54.6 | 1010649 | 72363107 | 61723 | 12 | oui | 2026-11-27T02:25:36+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 3 | `application_1795715756506_0381` | 14.5 | 10.7 | 5.2 | 24107 | 27.6 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:26:11+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 3 | `application_1795715756506_0382` | 11.1 | 7.4 | 5.5 | 20501 | 25.2 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:26:48+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 3 | `application_1795715756506_0383` | 15.4 | 6.4 | 6.4 | 53555 | 53.7 | 1010649 | 72363107 | 61723 | 12 | oui | 2026-11-27T02:27:20+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 4 | `application_1795715756506_0384` | 13.0 | 10.3 | 4.6 | 21743 | 23.8 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:27:53+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 4 | `application_1795715756506_0385` | 9.7 | 7.4 | 5.0 | 19119 | 22.3 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:28:26+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 4 | `application_1795715756506_0386` | 18.7 | 8.5 | 7.1 | 63245 | 67.5 | 1010649 | 72363107 | 61723 | 12 | oui | 2026-11-27T02:28:54+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 5 | `application_1795715756506_0387` | 16.0 | 13.1 | 5.4 | 30192 | 32.0 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:29:34+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 5 | `application_1795715756506_0388` | 11.0 | 8.0 | 5.8 | 22032 | 27.1 | 1005483 | 71743468 | 616 | 5 | oui | 2026-11-27T02:30:11+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 5 | `application_1795715756506_0389` | 14.5 | 6.6 | 6.5 | 44009 | 52.3 | 1010649 | 72363107 | 61723 | 12 | oui | 2026-11-27T02:30:42+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |

### Synthèse

| Variante | n | Durée médiane (s) | Min | Max | Écart (max - min) / médiane | Mo·s médian | Cœurs·s médian | CPU fixe par tâche (s) | CPU par M lignes (s) | Durée fixe par tâche (s) | Durée par M lignes (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mapreduce | 5 | 13.8 | 13.0 | 16.0 | 21% | 24475 | 26.8 | 0.64 | 2.00 | 1.77 | 4.32 |
| mapreduce-tuned | 5 | 10.8 | 9.7 | 11.1 | 13% | 20501 | 25.1 | 0.68 | 1.92 | 2.11 | 3.89 |
| spark | 5 | 15.5 | 14.5 | 18.7 | 28% | 53555 | 54.6 | 0.27 | 2.21 | 0.58 | 1.96 |

### Test de Mann-Whitney exact sur la durée

| A | B | n (A, B) | U (paires où A est plus lent) | p unilatéral (A plus lent) | p bilatéral |
| --- | --- | --- | ---: | ---: | ---: |
| mapreduce | mapreduce-tuned | 5, 5 | 25 / 25 | 0.004 | 0.008 |
| mapreduce | spark | 5, 5 | 3 / 25 | 0.984 | 0.056 |
| mapreduce-tuned | spark | 5, 5 | 0 / 25 | 1.000 | 0.008 |

### Résultat de référence

| Pays | CA facturé (EUR) | Lignes COMPLETED |
| --- | ---: | ---: |
| DE | 611968919.64 | 134617 |
| ES | 625633965.16 | 137240 |
| FR | 1767230531.51 | 389235 |
| IT | 569828023.93 | 125040 |
| UK | 590921796.50 | 129499 |
| UNKNOWN | 43477866.59 | 9640 |

| Compteur | Valeur |
| --- | ---: |
| Lines.COMPLETED | 925271 |
| Lines.HEADER | 3 |
| Lines.OTHER_STATUS | 59095 |
| Quality.BAD_ENUM | 2010 |
| Quality.BAD_PRICE | 2010 |
| Quality.BAD_QUANTITY | 5028 |
| Quality.BAD_TIMESTAMP | 3016 |
| Quality.MALFORMED_ROW | 1006 |
| Quality.NULL_CUSTOMER | 5028 |
| Quality.UNKNOWN_CUSTOMER | 3016 |

### Vérifications

Toutes les exécutions donnent la même sortie octet pour octet et les mêmes compteurs ; les compteurs couvrent toutes les lignes lues.
Les compteurs Quality sont égaux aux anomalies des manifestes.

## Volume `full`

Entrée : `hdfs://namenode:8020/benchmarks/mr-vs-spark/full/landing`, partitions jusqu'au 2026-01-03 :

- `/customers/ingestion_date=2026-01-01` : 548686 octets
- `/customers/ingestion_date=2026-01-02` : 4864 octets
- `/customers/ingestion_date=2026-01-03` : 4847 octets
- `/orders/ingestion_date=2026-01-01` : 713401398 octets
- `/orders/ingestion_date=2026-01-02` : 1954175 octets
- `/orders/ingestion_date=2026-01-03` : 2117782 octets

### Exécutions

| Variante | Exécution | Application | Durée (s) | Dont jobs (s) | CPU compté (s) | Mo·s | Cœurs·s | Lignes lues | Octets lus | Octets de shuffle | Tâches | Exclusive | Contrôle YARN avant l'exécution |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| mapreduce | 1 | `application_1795715756506_0390` | 22.9 | 19.2 | 19.4 | 59214 | 87.8 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:31:17+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 1 | `application_1795715756506_0391` | 18.8 | 16.0 | 18.5 | 53891 | 83.9 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:32:00+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 1 | `application_1795715756506_0392` | 20.0 | 12.0 | 20.5 | 69857 | 79.2 | 10059963 | 718359432 | 64276 | 14 | oui | 2026-11-27T02:32:39+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 2 | `application_1795715756506_0393` | 21.6 | 18.5 | 18.5 | 58800 | 86.3 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:33:21+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 2 | `application_1795715756506_0394` | 20.7 | 17.8 | 18.6 | 55245 | 85.8 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:34:03+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 2 | `application_1795715756506_0395` | 20.7 | 12.2 | 20.9 | 64786 | 81.9 | 10059963 | 718359432 | 64276 | 14 | oui | 2026-11-27T02:34:44+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 3 | `application_1795715756506_0396` | 21.2 | 18.5 | 17.2 | 55736 | 82.1 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:35:23+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 3 | `application_1795715756506_0397` | 20.3 | 17.6 | 17.5 | 53836 | 83.4 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:36:05+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 3 | `application_1795715756506_0398` | 19.9 | 11.9 | 20.8 | 68741 | 77.7 | 10059963 | 718359432 | 64276 | 14 | oui | 2026-11-27T02:36:45+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 4 | `application_1795715756506_0399` | 21.4 | 18.6 | 17.9 | 55673 | 82.4 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:37:28+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 4 | `application_1795715756506_0400` | 21.7 | 18.5 | 19.7 | 59098 | 93.0 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:38:10+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 4 | `application_1795715756506_0401` | 21.2 | 12.9 | 23.2 | 73159 | 84.3 | 10059963 | 718359432 | 64276 | 14 | oui | 2026-11-27T02:38:51+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce | 5 | `application_1795715756506_0402` | 23.1 | 19.7 | 18.3 | 61619 | 91.6 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:39:29+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| mapreduce-tuned | 5 | `application_1795715756506_0403` | 21.1 | 18.3 | 19.1 | 58363 | 91.5 | 10054797 | 717495123 | 1353 | 9 | oui | 2026-11-27T02:40:13+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |
| spark | 5 | `application_1795715756506_0404` | 22.6 | 13.4 | 22.8 | 69741 | 88.1 | 10059963 | 718359432 | 64276 | 14 | oui | 2026-11-27T02:40:56+00:00 : 0 application(s) NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING |

### Synthèse

| Variante | n | Durée médiane (s) | Min | Max | Écart (max - min) / médiane | Mo·s médian | Cœurs·s médian | CPU fixe par tâche (s) | CPU par M lignes (s) | Durée fixe par tâche (s) | Durée par M lignes (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mapreduce | 5 | 21.6 | 21.2 | 23.1 | 9% | 58800 | 86.3 | 0.98 | 0.95 | 2.38 | 4.10 |
| mapreduce-tuned | 5 | 20.7 | 18.8 | 21.7 | 14% | 55245 | 85.8 | 1.05 | 0.90 | 2.35 | 4.05 |
| spark | 5 | 20.7 | 19.9 | 22.6 | 13% | 69741 | 81.9 | -0.05 | 1.92 | -0.13 | 2.07 |

### Test de Mann-Whitney exact sur la durée

| A | B | n (A, B) | U (paires où A est plus lent) | p unilatéral (A plus lent) | p bilatéral |
| --- | --- | --- | ---: | ---: | ---: |
| mapreduce | mapreduce-tuned | 5, 5 | 22 / 25 | 0.028 | 0.056 |
| mapreduce | spark | 5, 5 | 21 / 25 | 0.048 | 0.095 |
| mapreduce-tuned | spark | 5, 5 | 11 / 25 | 0.655 | 0.841 |

### Résultat de référence

| Pays | CA facturé (EUR) | Lignes COMPLETED |
| --- | ---: | ---: |
| DE | 6032933271.06 | 1349183 |
| ES | 6130969854.42 | 1369109 |
| FR | 17422094258.86 | 3890383 |
| IT | 5592770351.66 | 1249510 |
| UK | 5807140524.12 | 1298215 |
| UNKNOWN | 433226117.01 | 96614 |

| Compteur | Valeur |
| --- | ---: |
| Lines.COMPLETED | 9253014 |
| Lines.HEADER | 3 |
| Lines.OTHER_STATUS | 590630 |
| Quality.BAD_ENUM | 20110 |
| Quality.BAD_PRICE | 20110 |
| Quality.BAD_QUANTITY | 50274 |
| Quality.BAD_TIMESTAMP | 30164 |
| Quality.MALFORMED_ROW | 10054 |
| Quality.NULL_CUSTOMER | 50274 |
| Quality.UNKNOWN_CUSTOMER | 30164 |

### Vérifications

Toutes les exécutions donnent la même sortie octet pour octet et les mêmes compteurs ; les compteurs couvrent toutes les lignes lues.
Les compteurs Quality sont égaux aux anomalies des manifestes.

## Taille du code

| Moteur | Fichier | Lignes | Lignes de code |
| --- | --- | ---: | ---: |
| mapreduce | `mapreduce/mapper.py` | 157 | 101 |
| mapreduce | `mapreduce/reducer.py` | 44 | 23 |
| mapreduce | `scripts/mapreduce/run_mr.sh` | 57 | 35 |
| spark | `src/gtdp/perf/revenue_by_country.py` | 162 | 117 |
| spark | `src/gtdp/jobs/revenue_by_country.py` | 77 | 63 |