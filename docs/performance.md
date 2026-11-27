# Performance et optimisation (Lot 7)

Ce document rassemble les mesures de performance de la plateforme : format, partitionnement et
compression (KAN-44), déséquilibre de clé (KAN-45), cache, partitions de shuffle et plans
d'exécution (KAN-46), réplication contre erasure coding (KAN-47). Chaque nombre vient d'un script
qu'on relance tel quel (voir [Reproduire](#reproduire)). Les tableaux sont faits pour être repris
dans le rapport technique (KAN-56). La comparaison MapReduce contre Spark a son propre document,
[MapReduce contre Spark](mapreduce-vs-spark.md), avec la campagne de mesure du même créneau.

## En bref

| Sujet | Résultat mesuré | Retenu |
| --- | --- | --- |
| Format (KAN-44) | CSV 5,08 s, Parquet 0,61 s : −88 % de durée, −89 % d'octets lus, −73 % d'octets stockés | Parquet partout |
| Partitionnement (KAN-44) | −92 % d'octets lus sur un mois, mais +79 % de durée tant que Spark liste les 396 dossiers par un job ; 0,62 s quand le driver les liste, 0,35 s une fois listés | partitionner par date, et lister côté driver |
| Compression (KAN-44) | zstd contre snappy : −25 % stockés et lus, durée identique (p = 0,84) | zstd pour les tables froides, snappy reste correct |
| Skew (KAN-45) | max/médiane 4,2 sur la jointure par `product_id` ; AQE ramène à 1,3 à 1,5 sans coût, le salage aussi mais 17 à 20 % plus lent ; la diffusion supprime le shuffle : −48 à −55 % | diffusion de `dim_product`, AQE skew join en filet |
| Cache (KAN-46) | perte de 52 % avec 3 indicateurs sur une lignée bon marché, égalité à 6, gain de 26 % quand la lignée contient un shuffle | cache seulement sur une lignée coûteuse et réutilisée |
| Partitions de shuffle (KAN-46) | 4 à 64 partitions : même durée (6,2 à 6,4 s) ; 200 : +9 % (p = 0,016), même avec AQE | 16, la valeur de la plateforme |
| Stockage (KAN-47) | XOR-2-1 : 50 % de surcoût contre 200 %, lecture complète identique ; un fichier de moins d'une cellule paie 100 % | erasure coding pour les gros fichiers froids, réplication 3 ailleurs |
| `_record_hash` en bronze | 73 % des octets de `bronze/orders` ; en binaire, −47 % sur la colonne | constat transmis, bronze inchangé |

## Protocole commun

Deux outils produisent toutes les mesures :

- `scripts/perf/bench.sh <expérience>` soumet `src/gtdp/perf/bench.py` à YARN en mode client.
  Le driver lance l'expérience, puis lit ses propres étapes dans le Spark History Server une fois
  l'application arrêtée. Il écrit `runs.json` et `report.md` dans `build/perf/<expérience>/`.
- `scripts/perf/storage.sh` compare la réplication 3 et l'erasure coding sur le même fichier avec
  les seules commandes HDFS. Il écrit `result.json`, `report.md` et les sorties de `hdfs fsck`
  dans `build/perf/storage/`.

**Cluster.** 2 NodeManagers de 2 560 Mio, 2 exécuteurs de 2 cœurs (896 Mio + 384 Mio chacun),
driver en mode client (896 Mio), `spark.sql.shuffle.partitions=16` et AQE active
(`conf/env/cluster.toml`, `docker/conf/spark/spark-defaults.conf`), soit 4 tâches en parallèle.
Une seule série tourne sur 8 tâches en parallèle (2 exécuteurs de 4 cœurs), signalée comme telle.

**Données.** Profil `full` du générateur, graine 42, J1 à J3 : 10 054 794 lignes de commandes
brutes. `scripts/perf/bench.sh prepare` en garde les 9 843 644 lignes qui passent les règles du
lot 6 (`INVALID_RULES`), typées, avec une colonne `order_date`. Il écrit les mêmes lignes dans
quatre dispositions sous `/benchmarks/perf/full/` et relit chaque copie pour vérifier qu'elle a
le même nombre de lignes. Il écrit aussi les 5 024 clients avec leur pays, la catégorie courante
des 1 006 produits et les 1 326 versions SCD2 du catalogue.

| Disposition | Format | Compression | `orders` partitionnée | Fichiers de `orders` |
| --- | --- | --- | --- | ---: |
| `csv` | CSV avec en-tête, lu avec un schéma explicite | aucune | non | 6 |
| `parquet` | Parquet | snappy | non | 6 |
| `parquet_part` | Parquet | snappy | par `order_date` | 396 |
| `parquet_part_zstd` | Parquet | zstd | par `order_date` | 396 |

**Règles de mesure**, communes à toutes les expériences chronométrées :

- **Exclusivité.** Avant chaque exécution, le driver interroge l'API REST du ResourceManager et
  attend qu'aucune autre application ne soit active. Le contrôle est noté avec l'heure dans
  `runs.json` (champ `yarn_check`, par exemple
  `2026-11-27T02:49:03+00:00: 0 other application NEW/NEW_SAVING/SUBMITTED/ACCEPTED/RUNNING`).
  Après coup, l'outil liste les applications qui ont chevauché chaque exécution (champ
  `overlapping`). Cette liste est vide pour toutes les exécutions rapportées ici.
- **Alternance et échauffement.** Un tour d'échauffement (exécution 0), exclu des résultats, puis
  5 tours. Chaque tour exécute chaque variante une fois, dans le même ordre. Une variante pose ses
  réglages SQL pour sa seule exécution.
- **Durée.** Le temps mural de l'action Spark, mesuré par le driver avec `time.perf_counter`. Il
  comprend la planification, le listage des fichiers et les jobs. Les tableaux donnent la médiane,
  le minimum et le maximum des 5 exécutions.
- **Différence retenue.** Test exact de Mann-Whitney sur les durées, 5 contre 5. Une différence
  est retenue quand le p bilatéral est inférieur à 0,05. Le plus petit p possible vaut 0,008.
- **Octets lus, shuffle, déversement.** Chaque exécution étiquette ses jobs Spark avec un groupe
  (`setJobGroup`). Le driver relit les jobs du groupe dans l'API REST du History Server
  (`/api/v1/applications/<id>/jobs`), puis somme `inputBytes`, `shuffleReadBytes` et
  `diskBytesSpilled` sur les étapes terminées de ces jobs (`/api/v1/applications/<id>/stages`).
- **Déséquilibre.** Pour l'étape clé de chaque exécution, le driver lit
  `/api/v1/applications/<id>/stages/<étape>/<tentative>/taskSummary?quantiles=0.5,1.0`, les
  « Summary Metrics » de l'interface Spark. Il en tire max/médiane de la durée des tâches et des
  octets lus. Une application en mode client n'a pas d'identifiant de tentative, donc l'URL n'a
  pas de segment `<attempt>` après l'identifiant d'application.
- **Contrôle du résultat.** Chaque exécution rend une empreinte de son résultat : nombre de lignes
  et SHA-256 des lignes triées, ou sommes de contrôle. L'outil sort en erreur si deux exécutions
  qui doivent calculer la même chose rendent des empreintes différentes. Toutes les empreintes
  concordent dans les expériences rapportées ici.
- **Volume stocké.** `hdfs dfs -du -s` donne deux nombres : les octets logiques et les octets
  occupés sur les DataNodes, répliques ou parité comprises.

## Naïf contre optimisé (KAN-44)

### Requête mesurée

Le chiffre d'affaires `COMPLETED` par jour, pays et catégorie sur décembre 2025 : les commandes
filtrées sur `order_date` et `status`, jointes aux clients pour le pays et aux produits pour la
catégorie, puis agrégées en 930 lignes (`datasets.revenue_by_day`). Ce choix reprend la forme de
l'indicateur gold `revenue_daily` et de l'indicateur du banc défini dans
[l'architecture](architecture.md#banc-de-performance-lot-7). Le filtre de dates est celui d'un
rapport sur une période : il laisse une table partitionnée par date sauter 365 de ses 396
partitions. Les jointures sont les recherches de dimension d'un indicateur. Chaque exécution lit
ses tables dans la mesure, comme le ferait un job batch. Les quatre dispositions rendent la même
empreinte : 930 lignes, `bd45a992d46c55b8`.

### Résultats

Application `application_1795715756506_0407`, 2026-11-27, 5 exécutions par variante après un tour
d'échauffement. Gains par rapport à la ligne précédente, sauf mention contraire.

| Variante | Durée médiane (s) | Min à max (s) | Octets lus | Octets stockés (logiques) | Octets stockés (physiques) | Tâches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `csv` | 5,08 | 4,88 à 5,41 | 870 831 538 | 870 503 858 | 2 611 511 574 | 10 |
| `parquet` | 0,61 | 0,58 à 0,79 | 93 169 973 | 232 815 264 | 698 445 792 | 8 |
| `parquet_part` | 1,10 | 0,95 à 1,27 | 7 759 742 | 231 537 550 | 694 612 650 | 403 |
| `parquet_part_zstd` | 1,04 | 0,93 à 1,24 | 5 872 951 | 174 337 367 | 523 012 101 | 403 |
| `parquet_part`, listage par le driver | 0,62 | 0,57 à 0,65 | 7 759 742 | | | 7 |
| `parquet_part`, déjà listée | 0,35 | 0,34 à 0,40 | 7 759 742 | | | 7 |

| Levier | Durée | Octets lus | Octets stockés | p bilatéral (durée) |
| --- | ---: | ---: | ---: | ---: |
| Format : CSV vers Parquet | −87,9 % | −89,3 % | −73,3 % | 0,008 |
| Partitionnement : Parquet vers Parquet partitionné | +79,4 % | −91,7 % | −0,5 % | 0,008 |
| Partitionnement, listage par le driver | +1,4 % | −91,7 % | −0,5 % | non testé |
| Partitionnement, table déjà listée | −43,1 % | −91,7 % | −0,5 % | 0,008 |
| Compression : snappy vers zstd | −5,6 % | −24,3 % | −24,7 % | 0,841 |
| Ensemble : CSV vers Parquet partitionné zstd | −79,5 % | −99,3 % | −80,0 % | 0,008 |

### Ce que disent ces chiffres

**Format.** Le CSV est lu en entier : 871 Mo pour 7 colonnes utiles sur 10. Dans l'exécution 3,
les 10 tâches cumulent 15,6 s de temps d'exécuteur en 4,97 s de temps mural sur 4 cœurs : la
lecture est limitée par l'analyse du texte, environ 56 Mo/s par cœur. Parquet ne lit que les 7
colonnes de la requête, 93 Mo sur 233, déjà typées, avec un lecteur vectorisé : 1,1 s de temps
d'exécuteur. La
date ne permet pas de sauter de groupe de lignes, car les lignes ne sont pas triées par date : les
statistiques min/max de chaque groupe couvrent toute l'année.

**Partitionnement.** L'élagage fonctionne : la lecture ne touche que les 31 fichiers de décembre,
7,8 Mo. Le temps, lui, augmente. Spark liste toutes les partitions d'une table quand il crée le
DataFrame, avant d'appliquer le filtre. Au-delà de 32 chemins
(`spark.sql.sources.parallelPartitionDiscovery.threshold`), il confie ce listage à un job Spark
d'une tâche par dossier : les 396 tâches de plus du tableau. Deux variantes isolent ce coût :

- lister les 396 dossiers depuis le driver (seuil porté à 1 024) ramène la durée à 0,62 s, soit
  autant que Parquet non partitionné ;
- une table listée une fois avant la mesure, comme dans une session longue ou un catalogue de
  partitions, descend à 0,35 s : c'est le gain propre de l'élagage, −43 % contre Parquet non
  partitionné.

Le partitionnement par jour donne ici des fichiers de 0,6 Mo, très loin du bloc HDFS de 128 Mo.
À ce volume, le partitionnement par date ne paie que si le listage reste bon marché.

**Compression.** zstd stocke et lit 25 % d'octets de moins que snappy. Le gain de durée (5,6 %)
reste dans la dispersion. La décompression ne limite pas cette requête : ses 31 petits fichiers
sont lus en 7 tâches.

**Version naïve.** Le CSV est lu avec un schéma explicite, pour mesurer le format et non
l'inférence de types. `inferSchema` ajouterait une lecture complète du fichier.

## Déséquilibre de clé (KAN-45)

### Mise en place

La jointure mesurée est celle de la construction de `fact_sales` en gold : chaque ligne de
commande rejoint la version du produit valide à son `event_time`
(`gtdp.silver.products.with_applicable_price`, appelée par `gtdp.gold.facts.sales`), soit
9 843 644 commandes contre 1 326 versions. Le produit `P00042` porte 20 % des lignes
synthétiques. L'action calcule le nombre de lignes, les lignes avec une version et le CA au prix
applicable. Toutes les variantes rendent la même empreinte : 9 843 644 lignes, 9 813 480 avec une
version, 46 234 195 001,75221 EUR.

| Variante | Réglages | Code |
| --- | --- | --- |
| `baseline` | AQE désactivée, `autoBroadcastJoinThreshold=-1` : jointure triée sur 16 partitions | `with_applicable_price` |
| `salted-16` | mêmes réglages, clé salée en 16 sous-clés | `gtdp.perf.skew.salted_applicable_price` |
| `aqe-skew-join` | AQE et skew join actives, `skewedPartitionFactor=2`, `skewedPartitionThresholdInBytes=16m`, `advisoryPartitionSizeInBytes=8m`, diffusion désactivée | `with_applicable_price` |
| `broadcast` | AQE désactivée, diffusion par défaut (10 Mo) | `with_applicable_price` |

**Seuils d'AQE.** Dans `baseline`, la partition chaude reçoit 31,0 Mo de shuffle et la médiane
7,4 Mo, soit un ratio de 4,2. Avec les valeurs par défaut (facteur 5, seuil 256 Mo), AQE ne
détecte rien. Le facteur 2 et le seuil de 16 Mo encadrent la partition chaude. La taille visée de
8 Mo, proche de la médiane, la découpe en 4 à 5 morceaux.

**Salage.** Une ligne de commande prend la sous-clé `xxhash64(event_time) mod 16`, et chaque
version du catalogue est copiée 16 fois. La première série salait sur `transaction_id`
(`application_1795715756506_0408`) : cette colonne, inutile à la jointure, ajoutait 48 Mo à la
lecture, pour 3,45 s. `event_time`, que la condition d'intervalle lit déjà, supprime ce surcoût
de lecture. Dans les deux cas, la sous-clé elle-même ajoute 49 Mo au shuffle. Un test vérifie que la jointure salée rend les mêmes lignes que
`with_applicable_price` (`tests/perf/test_skew.py`).

### Résultats

Étape clé : l'étape de la jointure, celle qui lit le plus d'octets de shuffle (pour `broadcast`,
celle qui lit le plus d'octets en entrée). Ratios : médiane des 5 exécutions. Liens : exécution 3
de chaque série.

4 tâches en parallèle, `application_1795715756506_0409` :

| Variante | Durée médiane (s) | Min à max (s) | p contre `baseline` | Max/médiane durée des tâches | Max/médiane octets lus | Shuffle lu | Déversé sur disque | Étape dans le History Server |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 2,63 | 2,46 à 3,06 | — | 4,16 | 4,16 | 142 656 609 | 24 722 709 | [étape 62](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=62&attempt=0) |
| `salted-16` | 3,17 | 2,99 à 3,29 | 0,016 | 1,63 | 1,49 | 191 826 948 | 0 | [étape 66](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=66&attempt=0) |
| `aqe-skew-join` | 2,63 | 2,59 à 2,87 | 0,841 | 1,50 | 1,31 | 142 664 876 | 0 | [étape 72](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=72&attempt=0) |
| `broadcast` | 1,37 | 1,32 à 1,47 | 0,008 | 1,01 | 1,34 | 450 | 0 | [étape 78](http://localhost:18080/history/application_1795715756506_0409/stages/stage/?id=78&attempt=0) |

8 tâches en parallèle (`spark.executor.cores=4`), `application_1795715756506_0410` :

| Variante | Durée médiane (s) | Min à max (s) | p contre `baseline` | Max/médiane durée des tâches | Max/médiane octets lus | Shuffle lu | Déversé sur disque | Étape dans le History Server |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `baseline` | 2,39 | 2,05 à 2,53 | — | 3,21 | 4,16 | 142 657 920 | 24 725 968 | [étape 62](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=62&attempt=0) |
| `salted-16` | 2,81 | 2,50 à 3,12 | 0,016 | 1,79 | 1,49 | 191 827 999 | 26 095 820 | [étape 66](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=66&attempt=0) |
| `aqe-skew-join` | 2,16 | 1,86 à 2,28 | 0,056 | 1,52 | 1,31 | 142 666 187 | 0 | [étape 72](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=72&attempt=0) |
| `broadcast` | 1,07 | 1,02 à 1,33 | 0,008 | 1,06 | 1,00 | 676 | 0 | [étape 78](http://localhost:18080/history/application_1795715756506_0410/stages/stage/?id=78&attempt=0) |

Détail de l'étape 62 de `baseline` (4 tâches en parallèle) : 16 tâches, durée médiane 321 ms et
maximale 1 359 ms, shuffle lu médian 7 448 950 octets et maximal 30 990 762 octets. Seule la
tâche chaude déverse sur disque.

### Ce que disent ces chiffres

**Le déséquilibre est réel, son coût en temps est petit sur ce cluster.** La tâche chaude dure
4,2 fois la médiane. Mais les 16 tâches de l'étape cumulent 6,0 s de temps d'exécuteur, soit
1,5 s par cœur sur 4 cœurs : la tâche chaude (1,36 s) tient dans cette durée idéale. Avec 4 tâches en parallèle,
une clé qui porte 20 % des lignes ne dépasse pas la part d'un cœur (25 %). Sur 8 tâches en
parallèle, la part d'un cœur tombe à 12,5 % : les tâches cumulent 9,2 s, soit 1,15 s par cœur,
et la tâche chaude (1,66 s) dépasse cette durée idéale d'environ 0,5 s, et c'est là qu'AQE gagne 0,23 s (−9,9 %, p = 0,056, à la limite du seuil).

**AQE skew join** découpe la partition chaude (20 tâches au lieu de 16) et supprime le
déversement, sans changer le volume de shuffle. Il ne coûte rien quand le déséquilibre ne coûte
rien.

**Le salage** corrige aussi le ratio (1,5 à 1,8), mais il ajoute la sous-clé à chaque ligne
mélangée : +34 % de shuffle. Il est plus lent dans les deux configurations (p = 0,016).

**La diffusion** supprime le shuffle des commandes : les 1 326 versions vont à chaque exécuteur
et la jointure se fait pendant la lecture. Elle est 48 à 55 % plus rapide que la jointure triée
et ne peut pas être déséquilibrée.

### Correction retenue

| Correction | Ratio de durée | Durée (4 cœurs) | Durée (8 cœurs) | Coût | Quand l'utiliser |
| --- | ---: | ---: | ---: | --- | --- |
| Diffusion | 1,0 | −48 % | −55 % | le petit côté doit tenir en mémoire de chaque exécuteur | dimension sous le seuil de diffusion |
| AQE skew join | 1,5 | 0 % | −10 % (p = 0,056) | seuils à adapter au volume | jointure triée inévitable |
| Salage, 16 sous-clés | 1,6 à 1,8 | +20 % | +18 % | +34 % de shuffle, code à maintenir | AQE indisponible, ou agrégation déséquilibrée |

Pour le pipeline, la construction de `fact_sales` doit diffuser `dim_product` : la dimension
compte 1 326 versions sur le profil `full`, très loin du seuil de 10 Mo, et le
[plan commenté plus bas](#jointure-de-la-construction-de-fact_sales) montre que le code gold
actuel le fait déjà avec les réglages de la plateforme. Si la dimension dépassait un jour le
seuil, AQE skew join suffirait, à condition d'ajuster `skewedPartitionFactor` et
`skewedPartitionThresholdInBytes` au volume : les valeurs par défaut ne se déclenchent pas ici.
Recommandation seulement : ce lot ne modifie pas le code gold.

## Cache, partitions de shuffle et plans (KAN-46)

### Cache

Le DataFrame réutilisé est le fait des ventes : les commandes au prix applicable
(`with_applicable_price`), réduites aux colonnes des indicateurs, avec le CA au prix applicable.
Les indicateurs sont des actions Spark distinctes (`bench.INDICATORS`) : CA quotidien, 10 produits
de plus fort CA, parts d'annulations et de retours par canal, puis, pour la série à 6, CA mensuel
par canal, lignes par jour et statut, ligne moyenne par produit. La variante `cache` appelle
`persist(StorageLevel.MEMORY_AND_DISK)`, calcule les indicateurs, relit l'état du stockage, puis
appelle `unpersist(blocking=True)` et relit l'état une seconde fois.

Application `application_1795715756506_0413`, 5 exécutions par variante :

| Lignée du fait | Indicateurs | Sans cache (s) | Avec cache (s) | Écart | p bilatéral | Données en cache |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Jointure diffusée (celle de gold) | 3 | 4,20 (4,09 à 4,44) | 6,39 (6,24 à 6,97) | +52,0 % | 0,008 | 5 partitions, 109 991 516 octets en mémoire, 0 sur disque |
| Jointure diffusée | 6 | 8,78 (8,20 à 9,20) | 8,81 (8,45 à 9,69) | +0,4 % | 0,421 | idem |
| Jointure triée (`autoBroadcastJoinThreshold=-1`) | 3 | 9,32 (8,57 à 10,21) | **6,89** (6,70 à 7,32) | **−26,0 %** | 0,008 | 16 partitions, 64 795 080 octets en mémoire, 0 sur disque |

Après chaque `unpersist(blocking=True)`, `SparkContext.getRDDStorageInfo()` ne liste plus aucun
RDD en cache (champ `after_unpersist` de `runs.json`, vide à chaque exécution).

**Pourquoi le cache perd sur la lignée de gold.** Décomposition de l'exécution 3 par job (History
Server) :

| Étape | Sans cache | Avec cache |
| --- | --- | --- |
| Premier indicateur, ou construction du cache | 1,42 s (3,4 s de temps d'exécuteur) | 4,23 s (10,4 s de temps d'exécuteur) |
| Indicateurs suivants | 1,51 s et 0,81 s | 0,71 s, 0,80 s et 0,34 s, lus dans le cache |

Recalculer ce fait coûte peu : une lecture Parquet vectorisée de 7 colonnes et une jointure
diffusée contre 1 326 versions. Le remplir dans le cache coûte 3 fois plus de temps d'exécuteur :
l'encodage en colonnes compressées de 9,8 M de lignes. Un indicateur coûte environ 1,25 s s'il
recalcule le fait, et 0,6 s s'il le lit dans le cache : avec 4,23 s de construction, le cache ne
paie qu'à partir d'environ 7 indicateurs (inféré de ces durées ; l'égalité mesurée à 6 le
confirme). Quand la lignée contient un shuffle, chaque recalcul coûte 3,1 à 3,5 s : le cache paie
dès 3 indicateurs, et supprime en plus 188 Mo de déversement.

**Règle retenue.** Mettre en cache un DataFrame seulement si sa lignée est coûteuse (shuffle,
lecture CSV) et réutilisée, ou bon marché mais réutilisée plus de 6 fois. La construction de gold
persiste `fact_sales`, puis le relit pour le comptage des lignes, les violations de grain, les
invariants, 6 des 9 indicateurs et l'écriture : au moins 10 lectures, au-delà du point où le cache
paie sur cette mesure.

### Nombre de partitions de shuffle

Agrégation large : CA et lignes par client et produit sur les 9,8 M de lignes (3 982 974 groupes),
suivie d'un comptage. Valeurs testées : 4, 8 (cœurs × 2), 16 (valeur de la plateforme), 64 et
200 (défaut de Spark), regroupement AQE désactivé
(`spark.sql.adaptive.coalescePartitions.enabled=false`), puis 200 avec regroupement, pour
comparaison. Application `application_1795715756506_0414`.

| Partitions | Durée médiane (s) | Min à max (s) | p contre 16 | Tâches de réduction | Octets de shuffle | Étape de lecture et pré-agrégation (exécution 3) | Étape de réduction (exécution 3) |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 4 | 6,30 | 6,02 à 6,44 | 0,690 | 4 | 105 516 536 | 4,93 s | 0,98 s |
| 8 | 6,24 | 5,92 à 6,58 | 1,000 | 8 | 107 968 334 | | |
| 16 | 6,29 | 5,84 à 6,39 | — | 16 | 110 571 506 | 5,11 s | 1,00 s |
| 64 | 6,35 | 6,14 à 7,02 | 0,548 | 64 | 115 193 240 | | |
| 200 | 6,85 | 6,32 à 7,50 | 0,016 | 200 | 118 048 664 | 5,69 s | 1,05 s |
| 200, regroupement AQE | 6,47 | 6,38 à 7,49 | 0,016 | 5 | 118 031 121 | 5,84 s | 1,11 s |

**Forme du résultat.** La durée ne dépend presque pas du nombre de partitions, parce que la
réduction n'est pas le limiteur. L'étape qui lit les commandes et pré-agrège prend 4,9 à 5,8 s sur
environ 6,3 : 5 tâches pour 4 cœurs, et 81 Mo déversés sur disque (814 Mo en mémoire), car avec
presque 4 M de groupes la pré-agrégation ne réduit presque rien. La réduction prend environ 1 s
quel que soit le nombre de partitions :

- avec 4 partitions, chaque tâche reçoit 26 Mo et les 4 cœurs travaillent : pas de déversement,
  pas de temps perdu ;
- avec 200, chaque tâche ne reçoit que 0,6 Mo : le coût fixe d'une tâche (lancement, 5 blocs à
  récupérer) domine, et surtout chaque tâche de lecture écrit 200 segments de shuffle au lieu de
  16. Les octets de shuffle augmentent de 7 %, et l'étape de lecture de 0,6 s dans
  l'exécution 3.

Le regroupement d'AQE ramène la réduction à 5 tâches, mais il agit à la lecture du shuffle :
l'écriture des 200 segments reste. Il ne rattrape donc pas 16 partitions (p = 0,016). La valeur
de la plateforme, 16, est dans la zone plate : elle reste le bon choix pour ces volumes. Ce
balayage ne montre pas de déversement côté réduction, même avec 4 partitions de 26 Mo.

### Trois plans commentés

Les plans viennent de `scripts/perf/bench.sh explain`, sur le lac `dev` réel (J1 à J3), en lecture
seule : `explain("formatted")` du code tel qu'il tourne, avec la configuration du cluster. Texte
complet dans [`docs/perf-evidence/explain/`](perf-evidence/explain/).

#### Lecture avec élagage de partitions

Requête : CA de `gold/fact_sales` par `order_date` sur décembre 2025.

```text
AdaptiveSparkPlan (5)
+- HashAggregate (4)
   +- Exchange (3)
      +- HashAggregate (2)
         +- Scan parquet  (1)

(1) Scan parquet
Output [2]: [revenue_eur#12, order_date#14]
Location: InMemoryFileIndex [hdfs://namenode:8020/datalake/gold/fact_sales]
PartitionFilters: [isnotnull(order_date#14), (order_date#14 >= 2025-12-01), (order_date#14 <= 2025-12-31)]
ReadSchema: struct<revenue_eur:decimal(20,2)>
(3) Exchange
Arguments: hashpartitioning(order_date#14, 16), ENSURE_REQUIREMENTS, [plan_id=11]
```

| Opérateur | Ce qu'il fait | Ce qu'il coûte |
| --- | --- | --- |
| `Scan parquet (1)` | lit `fact_sales` ; `PartitionFilters` porte sur la colonne de partition, donc Spark ne lit que les dossiers `order_date=2025-12-*` | le listage de tous les dossiers, puis 31 dossiers lus sur 396 ; `ReadSchema` ne contient que `revenue_eur` : `order_date` vient du nom du dossier, pas du fichier |
| `HashAggregate (2)` | somme partielle par `order_date` dans chaque tâche (`partial_sum`) | une table de hachage par tâche, au plus 31 clés |
| `Exchange (3)` | shuffle des sommes partielles en 16 partitions par `order_date` | écriture puis lecture réseau de quelques lignes par tâche |
| `HashAggregate (4)` | somme finale par jour | négligeable |
| `AdaptiveSparkPlan (5)` | plan initial d'AQE (`isFinalPlan=false`), réoptimisé après le shuffle | aucun en soi ; AQE peut regrouper les 16 petites partitions une fois leur taille connue |

#### Filtre poussé dans Parquet

Requête : commandes annulées d'au moins 10 unités dans `silver/orders`.

```text
* Project (4)
+- * Filter (3)
   +- * ColumnarToRow (2)
      +- Scan parquet  (1)

(1) Scan parquet
Output [4]: [transaction_id#72, quantity#76, status#79, ingestion_date#81]
Location: InMemoryFileIndex [hdfs://namenode:8020/datalake/silver/orders]
PushedFilters: [IsNotNull(status), IsNotNull(quantity), EqualTo(status,CANCELLED), GreaterThanOrEqual(quantity,10)]
ReadSchema: struct<transaction_id:string,quantity:int,status:string>
(3) Filter [codegen id : 1]
Condition : (((isnotnull(status#79) AND isnotnull(quantity#76)) AND (status#79 = CANCELLED)) AND (quantity#76 >= 10))
```

| Opérateur | Ce qu'il fait | Ce qu'il coûte |
| --- | --- | --- |
| `Scan parquet (1)` | `PushedFilters` : le lecteur Parquet reçoit les prédicats et saute les groupes de lignes dont les statistiques min/max les excluent ; `ReadSchema` limite la lecture à 3 colonnes | les octets des seules colonnes lues ; un groupe de lignes n'est sauté que si ses statistiques le permettent, donc le gain dépend du tri des données |
| `ColumnarToRow (2)` | convertit les lots colonnes du lecteur vectorisé en lignes | une copie par ligne lue |
| `Filter (3)` | réapplique la même condition à chaque ligne : le filtre poussé n'est qu'un saut de groupes, pas un filtre exact | une comparaison par ligne, compilée (`codegen id : 1`, l'astérisque) |
| `Project (4)` | garde `transaction_id` et `quantity` | aucun : même étape compilée |

Aucun `Exchange` : la requête tient en une étape sans shuffle.

#### Jointure de la construction de `fact_sales`

Requête : `gtdp.gold.facts.sales(orders, products)`, le code gold, sur `silver/orders` et
`gold/dim_product`. Avec les réglages de la plateforme, Spark diffuse la dimension :

```text
AdaptiveSparkPlan (9)
+- Project (8)
   +- BroadcastHashJoin LeftOuter BuildRight (7)
      :- Project (2)
      :  +- Scan parquet  (1)
      +- BroadcastExchange (6)
         +- Project (5)
            +- Filter (4)
               +- Scan parquet  (3)

(6) BroadcastExchange
Arguments: HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=50]
(7) BroadcastHashJoin
Left keys [1]: [product_id#98]
Right keys [1]: [_version_of#115]
Join type: LeftOuter
Join condition: ((valid_from#35 <= event_time#96) AND (event_time#96 < valid_to#36))
```

Avec `spark.sql.autoBroadcastJoinThreshold=-1`, la même jointure devient triée :

```text
AdaptiveSparkPlan (12)
+- Project (11)
   +- SortMergeJoin LeftOuter (10)
      :- Sort (4)
      :  +- Exchange (3)
      :     +- Project (2)
      :        +- Scan parquet  (1)
      +- Sort (9)
         +- Exchange (8)
            +- Project (7)
               +- Filter (6)
                  +- Scan parquet  (5)

(3) Exchange
Arguments: hashpartitioning(product_id#221, 16), ENSURE_REQUIREMENTS, [plan_id=76]
(8) Exchange
Arguments: hashpartitioning(_version_of#238, 16), ENSURE_REQUIREMENTS, [plan_id=77]
(10) SortMergeJoin
Left keys [1]: [product_id#221]
Right keys [1]: [_version_of#238]
Join type: LeftOuter
Join condition: ((valid_from#35 <= event_time#219) AND (event_time#219 < valid_to#36))
```

| Opérateur | Ce qu'il fait | Ce qu'il coûte |
| --- | --- | --- |
| `Scan parquet (1)` et `Project (2)` | lisent les 9 colonnes de `silver/orders`, puis retirent `ingestion_date` | toutes les colonnes : le fait les garde toutes |
| `Scan parquet (3)`, `Filter (4)`, `Project (5)` | lisent `dim_product` avec `IsNotNull` poussés sur les clés de jointure, puis renomment `product_id` en `_version_of` | une ligne par version de produit, 5 colonnes |
| `BroadcastExchange (6)` | rassemble la dimension sur le driver, la transforme en table de hachage sur `_version_of` et l'envoie à chaque exécuteur | une copie de la dimension par exécuteur ; possible seulement sous le seuil de diffusion (10 Mo) |
| `BroadcastHashJoin LeftOuter BuildRight (7)` | pour chaque commande, cherche les versions de son produit dans la table de hachage locale, puis applique la condition d'intervalle | aucun shuffle des commandes : la jointure se fait dans l'étape de lecture, donc aucun déséquilibre possible |
| `Exchange (3)` et `Exchange (8)` (jointure triée) | `hashpartitioning(..., 16)` : chaque ligne part vers la partition `hash(product_id) mod 16`, des deux côtés | écriture et lecture réseau de toutes les commandes ; toutes les lignes de `P00042` vont dans la même partition |
| `Sort (4)` et `Sort (9)` | trient chaque partition par clé de jointure | tri en mémoire, déversé sur disque si la partition dépasse la mémoire de la tâche : c'est la partition chaude qui déverse dans KAN-45 |
| `SortMergeJoin LeftOuter (10)` | parcourt les deux côtés triés en parallèle ; pour une clé, compare chaque commande aux versions de son produit avec la condition d'intervalle | coût proportionnel à la taille de la partition, donc la partition chaude fixe la durée de l'étape |
| `Project (8)` ou `(11)` | calcule `date_key`, `revenue_eur`, `invoiced_amount_eur` et `order_date` | arithmétique décimale par ligne |

Le code gold, avec les réglages de la plateforme, produit donc la jointure diffusée, la plus
rapide de KAN-45. Limite de ce plan : il lit la table `dim_product` publiée, alors que le job gold
joint la dimension qu'il vient de construire et de persister. La décision de diffusion du job
s'appuie alors sur l'estimation de taille de ce DataFrame, non mesurée ici.

## Réplication 3 contre erasure coding (KAN-47)

### Politique et topologie

`/benchmarks/ec` porte la politique `XOR-2-1-1024k` (posée par `init-hdfs.sh`) :
`hdfs ec -getPolicy -path /benchmarks/ec` répond `XOR-2-1-1024k`, et pour `/benchmarks/rep3`
`The erasure coding policy of /benchmarks/rep3 is unspecified`. Un groupe de blocs XOR-2-1 a
2 blocs de données et 1 bloc de parité, chacun sur un DataNode distinct, découpés en cellules de
1 Mio. Les cellules de données vont alternativement sur les deux blocs de données. La parité
d'une bande est le XOR de ses deux cellules.

Les politiques Reed-Solomon ne tiennent pas sur 3 DataNodes.
`hdfs ec -verifyClusterSetup -policy <politique>` répond :

```text
RS-6-3-1024k : 9 DataNodes are required for the erasure coding policies: RS-6-3-1024k. The number of DataNodes is only 3.
RS-3-2-1024k : 5 DataNodes are required for the erasure coding policies: RS-3-2-1024k. The number of DataNodes is only 3.
XOR-2-1-1024k : 3 racks are required for the erasure coding policies: XOR-2-1-1024k. The number of racks is only 1.
```

RS-6-3 place 9 blocs par groupe et RS-3-2 en place 5, chacun sur un nœud distinct : avec 3 nœuds,
un groupe ne peut pas être complet, et la perte d'un nœud emporterait plusieurs blocs du même
groupe. XOR-2-1 place exactement 3 blocs. L'avertissement sur les racks concerne la tolérance à
la perte d'un rack entier, impossible sur un cluster d'un seul rack. La perte d'un DataNode reste
couverte. RS-6-3 est pourtant activée sur le cluster (politique par défaut de Hadoop) : un
répertoire qui la recevrait ne pourrait pas écrire de groupe complet.

### Volumes mesurés

`scripts/perf/storage.sh` copie le fichier de commandes J1 du profil `full` (713 401 398 octets)
et le fichier produits J1 (79 966 octets, plus petit qu'une cellule) dans les deux répertoires.
Les sommes de contrôle CRC composites (`hdfs dfs -checksum` avec
`dfs.checksum.combine.mode=COMPOSITE_CRC`) sont égales dans les deux modes : `5056866c` pour les
commandes, `a52f42e8` pour les produits. Les deux copies contiennent donc les mêmes octets.

| Fichier | Mode | Octets logiques | Octets physiques | Surcoût | Blocs ou groupes | Unités par bloc ou groupe |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| commandes J1 | réplication 3 | 713 401 398 | 2 140 204 194 | 200,0 % | 6 blocs (5 × 128 Mio et 42 312 758 octets) | 3 répliques sur 3 DataNodes |
| commandes J1 | XOR-2-1-1024k | 713 401 398 | 1 070 286 956 | 50,0 % | 3 groupes (2 × 256 Mio de données et 176 530 486 octets) | 2 blocs de données et 1 de parité sur 3 DataNodes |
| produits J1 | réplication 3 | 79 966 | 239 898 | 200,0 % | 1 bloc | 3 répliques |
| produits J1 | XOR-2-1-1024k | 79 966 | 159 932 | 100,0 % | 1 groupe | 1 bloc de données et 1 de parité |

Le volume physique du fichier en erasure coding se vérifie à l'octet près. Le fichier occupe
681 cellules de données (680 pleines et une de 369 718 octets), réparties en 341 bandes. La parité
compte une cellule par bande, de la taille de la plus grande cellule de la bande :
340 × 1 048 576 + 369 718 = 356 885 558 octets. Données et parité font
713 401 398 + 356 885 558 = 1 070 286 956 octets, la valeur de `hdfs dfs -du`.

**Petits fichiers.** Un fichier plus petit qu'une cellule ne remplit qu'un bloc de données. Sa
parité a la même taille : le surcoût passe de 50 % à 100 %. Chaque fichier coûte en plus ses
objets au NameNode (le groupe et ses blocs internes), comme en réplication. L'erasure coding
convient donc aux gros fichiers. Les petits fichiers d'un flux de streaming ou les partitions
journalières de 0,6 Mo de KAN-44 n'y gagnent presque rien.

### Tolérance aux pannes

| Mode | DataNodes perdus sans perte de données | Lecture avec un nœud manquant | Réparation |
| --- | --- | --- | --- |
| Réplication 3 | 2 par bloc (une réplique suffit) | le client lit une autre réplique, sans calcul | le NameNode recopie le bloc ; avec 3 nœuds dont un mort, aucun 4ᵉ nœud ne peut recevoir la copie, le bloc reste sous-répliqué jusqu'au retour du nœud |
| XOR-2-1 | 1 par groupe | lecture dégradée : le client lit la cellule restante et la parité, et reconstruit la cellule manquante par XOR (codec natif ISA-L) | un DataNode reconstruit le bloc manquant ; avec 3 nœuds dont un mort, aucun nœud libre ne peut le recevoir, le groupe reste incomplet jusqu'au retour du nœud |

La réplication 3 tolère donc une panne de plus par bloc (2 contre 1), pour un surcoût quatre
fois plus grand (200 % contre 50 %). L'erasure coding a d'autres limites : pas de `hflush` ni `hsync`, donc pas d'écriture en
continu, et plus de localité des données, puisqu'une lecture touche toujours plusieurs nœuds.
KAN-52 montrera une lecture dégradée en direct sur les fichiers laissés en place.

### Coût et lecture

Lecture complète (`hdfs dfs -cat` vers `/dev/null` depuis le conteneur client), un tour
d'échauffement puis 5 lectures par mode en alternance. Copie (`hdfs dfs -cp`, lecture de la source
comprise), 5 copies par mode en alternance.

| Mode | Surcoût de stockage | Lecture complète (s) | Lecture du petit fichier (s) | Copie (s) |
| --- | ---: | ---: | ---: | ---: |
| Réplication 3 | 200 % | 1,77 (1,69 à 2,07) | 1,14 (1,13 à 1,16) | 3,71 (3,04 à 6,57) |
| XOR-2-1-1024k | 50 % | 1,69 (1,59 à 1,76) | 1,20 (1,13 à 1,32) | 3,39 (3,07 à 4,11) |
| p bilatéral | | 0,095 | 0,151 | 0,310 |

Aucune différence de durée n'est démontrée, en lecture comme en copie. La lecture du petit
fichier mesure le coût fixe du client `hdfs` (démarrage de la JVM, environ 1,1 s) : le transfert
de 713 Mo prend 0,5 à 0,6 s dans les deux modes. Ces lectures viennent du cache de pages de la
machine, que les trois DataNodes partagent : elles mesurent le chemin logiciel, pas les disques.
Sur ce cluster, l'erasure coding divise le volume physique par deux sans pénalité de lecture
mesurable quand tous les nœuds sont présents.

### Fichiers laissés en place pour KAN-52

| Chemin | Mode | Octets logiques | Octets physiques |
| --- | --- | ---: | ---: |
| `/benchmarks/ec/orders_20260101.csv` | XOR-2-1-1024k | 713 401 398 | 1 070 286 956 |
| `/benchmarks/rep3/orders_20260101.csv` | réplication 3 | 713 401 398 | 2 140 204 194 |
| `/benchmarks/ec/products_20260101.csv` | XOR-2-1-1024k | 79 966 | 159 932 |
| `/benchmarks/rep3/products_20260101.csv` | réplication 3 | 79 966 | 239 898 |

Les emplacements des blocs sont dans les sorties de `hdfs fsck`
([`docs/perf-evidence/storage/`](perf-evidence/storage/)). Pour la démonstration, comparer la
somme `COMPOSITE_CRC` de la copie en erasure coding, avant et pendant la panne, à `5056866c`.

## Constat : `_record_hash` en bronze

Mesure sur la table réelle `/datalake/bronze/orders` (lac `dev`, J1 à J3, 1 005 480 lignes),
taille de chaque colonne sommée depuis les pieds de page Parquet (`scripts/perf/bench.sh
record-hash`, `application_1795715756506_0415`) :

| Colonne | Octets compressés | Part des colonnes |
| --- | ---: | ---: |
| `_record_hash` | 66 420 644 | 73,4 % |
| `event_time` | 9 557 319 | 10,6 % |
| `unit_price_eur` | 5 737 140 | 6,3 % |
| `transaction_id` | 4 608 561 | 5,1 % |
| 10 autres colonnes | 4 152 997 | 4,6 % |
| Total des colonnes | 90 476 661 | 100 % |

`hdfs dfs -du -s` donne 90 541 119 octets logiques pour la table. Le haché est un SHA-256 écrit
en 64 caractères hexadécimaux : un texte aléatoire que snappy ne compresse presque pas (97,1 %
de sa taille non compressée). Récrit seul en snappy, le même haché occupe 66 216 693 octets en
texte et 34 840 415 octets en `binary` (`unhex`), soit −47,4 %. Stocker les 32 octets du haché
plutôt que leur forme hexadécimale réduirait la table bronze des commandes d'environ un tiers.
Les chiffres de la revue précédente (62,8 Mo sur 85,9 Mo) portaient sur un état antérieur de la
table. Ce lot ne modifie pas le code bronze : constat transmis au lead.

## Reproduire

Depuis la racine du dépôt, cluster démarré, sans autre application sur YARN :

```bash
export GTDP_PSEUDO_SALT=<valeur hors dépôt>      # exigé par --env cluster, rien n'est pseudonymisé

# Données : profil full, graine 42, déposé sous /benchmarks/mr-vs-spark/full/landing
scripts/cluster/client.sh python3 -m gtdp.datagen generate --profile full --seed 42 --out data/generated
#   (puis les commandes de dépôt de docs/mapreduce-vs-spark.md#campagne-de-mesure)

scripts/perf/bench.sh prepare                    # tables des 4 dispositions sous /benchmarks/perf/full
scripts/perf/bench.sh formats                    # KAN-44
scripts/perf/bench.sh skew --label 4slots        # KAN-45, 4 tâches en parallèle
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.cores=4" \
  scripts/perf/bench.sh skew --label 8slots      # KAN-45, 8 tâches en parallèle
scripts/perf/bench.sh cache                      # KAN-46
scripts/perf/bench.sh partitions                 # KAN-46
scripts/perf/bench.sh explain                    # KAN-46, plans du lac dev
scripts/perf/storage.sh \
  --source /benchmarks/mr-vs-spark/full/landing/orders/ingestion_date=2026-01-01/orders_20260101.csv \
  --small /benchmarks/mr-vs-spark/full/landing/products/ingestion_date=2026-01-01/products_20260101.csv
scripts/perf/bench.sh record-hash                # constat bronze

# Nettoyage : garder /benchmarks/ec et /benchmarks/rep3 pour KAN-52
scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash /benchmarks/perf /benchmarks/mr-vs-spark
```

Chaque commande écrit son rapport dans `build/perf/<expérience>/`. Les rapports et les CSV
d'exécutions des résultats cités ici sont copiés dans
[`docs/perf-evidence/`](perf-evidence/). Le rapport complet de la campagne MapReduce contre Spark
y est aussi : [`mr-vs-spark-campaign.md`](perf-evidence/mr-vs-spark-campaign.md).
