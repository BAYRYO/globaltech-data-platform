# Kit de données fourni

Fichiers du kit étudiant (`Kit_BigData_Spark_Etudiants.zip`), compressés en gzip sans autre
modification. Ils servent de référentiel au générateur `gtdp.datagen` (voir
[contrat de données](../../docs/data-contract.md)).

| Fichier | Lignes | SHA-256 du fichier décompressé |
| --- | --- | --- |
| `customers.csv.gz` | 5 000 | `2bcff188403dee3e23053682dffc6c8dcdeba53b208eecc8e51dd387ff08d448` |
| `products.csv.gz` | 1 000 | `d157628e4b18f9381005fbb8db1880053917b033ec1f10ebf75a7cd849da5b3f` |
| `transactions.csv.gz` | 200 000 | `f192a039c7e6deaedbe97fe39ebc6f420e9cc9e8e3c13e675729989789fca9f9` |
| `events.jsonl.gz` | 100 000 | `971aade3b6fd0449d88904202d084999bc6eee9670e56e782b535f108d70dd91` |

Profil mesuré (script de profilage, novembre 2026) : aucun doublon, aucune valeur manquante,
aucune référence orpheline, transactions du 2025-01-01 au 2025-12-31, ratio max/médiane du
nombre de transactions par produit de 1,24. Les anomalies du sujet sont donc injectées par le
générateur.
