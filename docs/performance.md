# Performance et optimisation (Lot 7)

Ce document rassemble les mesures de performance de la plateforme : format, partitionnement et
compression (KAN-44), déséquilibre de clé (KAN-45), cache, partitions de shuffle et plans
d'exécution (KAN-46), réplication contre erasure coding (KAN-47). Chaque durée, octet, écart et
p bilatéral des tableaux sort d'un outil qu'on relance (voir [Reproduire](#reproduire)) ou se
recalcule des preuves versionnées dans [`docs/perf-evidence/`](perf-evidence/). Les quelques
lectures du History Server faites à la main donnent leur application, leur étape et leur
exécution. Les tableaux sont faits pour être repris dans le
[rapport technique](rapport-technique.md) (KAN-56). La
comparaison MapReduce contre Spark a son propre document,
[MapReduce contre Spark](mapreduce-vs-spark.md), avec la campagne de mesure du même créneau.

## En bref

| Sujet | Résultat mesuré | Retenu |
| --- | --- | --- |
| Format (KAN-44) | CSV 5,08 s, Parquet 0,61 s : −88 % de durée, −89 % d'octets lus, −73 % d'octets stockés | Parquet partout |
| Partitionnement (KAN-44) | −92 % d'octets lus sur un mois, mais +79 % de durée tant que Spark liste les 396 dossiers par un job ; 0,62 s quand le driver les liste, 0,35 s une fois listés | partitionner par date, et lister côté driver |
| Compression (KAN-44) | zstd contre snappy : −25 % stockés et lus, pas de différence de durée détectable (p = 0,841) | zstd pour les tables froides, snappy reste correct |
| Skew (KAN-45) | max/médiane 4,2 sur la jointure par `product_id` ; AQE ramène à 1,3 à 1,5 sans ralentir, sans gain de durée retenu ; le salage aussi, mais 17 à 20 % plus lent ; la diffusion supprime le shuffle : −48 à −55 % | diffusion de `dim_product`, AQE skew join en filet |
| Cache (KAN-46) | perte de 52 % avec 3 indicateurs sur une lignée bon marché, pas de différence détectable à 6 (p = 0,421), gain de 26 % quand la lignée contient un shuffle | cache seulement sur une lignée coûteuse et réutilisée |
| Partitions de shuffle (KAN-46) | 4 à 64 partitions : pas de différence de durée détectable (6,2 à 6,4 s) ; 200 : +9 % (p = 0,016), même avec AQE ; l'étape de réduction dessine un U | 16, la valeur de la plateforme |
| Stockage (KAN-47) | XOR-2-1 : 50 % de surcoût contre 200 %, pas de différence de lecture détectable (p = 0,095) ; un fichier de moins d'une cellule paie 100 % | erasure coding pour les gros fichiers froids, réplication 3 ailleurs |
| `_record_hash` en bronze | 73 % des octets de `bronze/orders` ; en binaire, −47 % sur la colonne | constat transmis, bronze inchangé |

## Protocole commun

Deux outils produisent toutes les mesures :

- `scripts/perf/bench.sh <expérience>` soumet `src/gtdp/perf/bench.py` à YARN en mode client.
  Le driver lance l'expérience, puis lit ses propres étapes dans le Spark History Server une fois
  l'application arrêtée. Il écrit `runs.json`, chaque exécution à pleine précision, et
  `report.md` dans `build/perf/<expérience>/`. `bench.sh report` refait `report.md` à partir du
  seul `runs.json`, sans cluster.
- `scripts/perf/storage.sh` compare la réplication 3 et l'erasure coding sur le même fichier avec
  les seules commandes HDFS. Il écrit `result.json`, `report.md` et les sorties de `hdfs fsck`
  dans `build/perf/storage/`. `storage.sh --report` refait `report.md` à partir de `result.json`.

Les dossiers HDFS viennent de la section `[perf]` de `conf/app.toml` (racine du landing, des
tables, des exécutions de `storage.sh`), qu'on surcharge avec `GTDP_PERF__<CLÉ>` ou les options
`--landing`, `--root` des outils.

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
  `overlapping`). Une exécution chevauchée sort des statistiques, et `report.md` la nomme sous
  le tableau des variantes. Aucune exécution rapportée ici n'a été chevauchée.
- **Alternance et échauffement.** Des tours d'échauffement, exclus des résultats, puis 5 tours.
  Chaque tour exécute chaque variante une fois, dans le même ordre. Une variante pose ses réglages
  SQL pour sa seule exécution. Les séries rapportées ici ont tourné avec un seul tour
  d'échauffement (exécution 0). Il ne suffit pas pour les expériences courtes : l'exécution 1
  reste la plus lente des 5 dans 12 des 14 variantes de `formats` et des deux séries
  `skew` à 4 tâches en parallèle, et dans 13 des 30 variantes des six séries (6 attendues si
  les exécutions étaient interchangeables). L'outil fait donc 2 tours d'échauffement par défaut
  (`[perf] warmup_rounds`, option `--warmup`). Les séries ne sont pas refaites : chaque
  `report.md` recalcule toutes les comparaisons sans l'exécution mesurée 1 (section
  « Sensibilité à l'échauffement »). Une seule conclusion change, voir
  [le salage à 8 tâches](#ce-que-disent-ces-chiffres-1).
- **Durée.** Le temps mural de l'action Spark, mesuré par le driver avec `time.perf_counter` et
  gardé à pleine précision dans `runs.json`. Il comprend la planification, le listage des
  fichiers et les jobs. Les tableaux donnent la médiane, le minimum et le maximum des 5
  exécutions.
- **Différence retenue.** Test exact de Mann-Whitney sur les durées, 5 contre 5, calculé par
  l'outil pour chaque comparaison que l'expérience déclare (`report.md`, section
  « Comparaisons »). Une différence est retenue quand le p bilatéral est inférieur à 0,05.
  Avec 5 exécutions contre 5, il y a 252 façons de répartir les 10 durées. Le plus petit p
  bilatéral vaut 2/252 ≈ 0,008, quand toutes les exécutions d'une variante sont plus rapides que
  toutes celles de l'autre. Le p passe sous 0,05 seulement si 2 des 25 paires au plus sont dans
  l'autre sens (U ≤ 2 donne 0,032, U = 3 donne 0,056). Un écart petit devant la dispersion
  d'une série, quelques pour cent ici, ne peut donc pas être retenu. « Pas de différence
  détectable » ne veut pas dire « égalité ». Sans l'exécution 1, à 4 contre 4, le plus petit p
  vaut 2/70 ≈ 0,029 et une seule paire dans l'autre sens donne 0,057.
- **Octets lus, shuffle, déversement.** Chaque exécution étiquette ses jobs Spark avec un groupe
  (`setJobGroup`). Le driver relit les jobs du groupe dans l'API REST du History Server
  (`/api/v1/applications/<id>/jobs`), puis somme `inputBytes`, `shuffleReadBytes` et
  `diskBytesSpilled` sur la dernière tentative terminée de chaque étape de ces jobs
  (`/api/v1/applications/<id>/stages`). Il sort en erreur si le History Server ne liste plus
  une étape ou un job de l'application (`spark.ui.retainedStages` et `retainedJobs` valent
  1 000), au lieu de sommer une partie des étapes.
- **Étapes.** Pour chaque exécution, l'outil garde aussi la durée de l'étape de lecture (celle
  qui lit le plus d'octets en entrée) et de l'étape clé, du lancement de la première tâche à la
  fin de l'étape, et les compare comme les durées totales.
- **Déséquilibre.** Pour l'étape clé de chaque exécution, le driver lit
  `/api/v1/applications/<id>/stages/<étape>/<tentative>/taskSummary?quantiles=0.5,1.0`, les
  « Summary Metrics » de l'interface Spark. Il en tire max/médiane de la durée des tâches et des
  octets lus. Une application en mode client n'a pas d'identifiant de tentative, donc l'URL n'a
  pas de segment `<attempt>` après l'identifiant d'application.
- **Contrôle du résultat.** Chaque exécution rend une empreinte de son résultat. L'outil sort en
  erreur si deux exécutions qui doivent calculer la même chose rendent des empreintes
  différentes. Toutes les empreintes concordent dans les expériences rapportées ici. Pour
  `formats` et `cache`, l'empreinte est le nombre de lignes et le SHA-256 des lignes triées.
  Pour `skew` et `partitions`, ce sont des sommes de contrôle : lignes, lignes avec une version
  et CA au prix applicable, ou groupes, CA et lignes. Une jointure qui rattacherait une ligne à
  une autre version au même prix passerait ce contrôle. L'égalité ligne à ligne de la jointure
  salée et de la jointure de gold est prouvée par les tests (`tests/perf/test_skew.py`,
  `tests/perf/test_review_b.py`).
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
d'échauffement. Chaque levier compare une variante à sa référence. Écarts et p viennent de
[`perf-evidence/formats/report.md`](perf-evidence/formats/report.md), les octets stockés de la
même application (`hdfs dfs -du -s`).

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
| Partitionnement, listage par le driver | +1,4 % | −91,7 % | −0,5 % | 0,690 |
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

- lister les 396 dossiers depuis le driver (seuil porté à 1 024) ramène la durée à 0,62 s, sans
  différence détectable avec Parquet non partitionné (+1,4 %, p = 0,690) ;
- une table listée une fois avant la mesure, comme dans une session longue ou un catalogue de
  partitions, descend à 0,35 s, −43,1 % contre Parquet non partitionné. Cette variante saute
  aussi la création des trois DataFrames dans la mesure : la durée murale moins la durée des
  exécutions SQL vaut 0,097 s pour `parquet` et 0,050 s pour elle (médianes, onglet SQL du
  History Server). Sur la seule durée d'exécution SQL, l'élagage gagne −40,8 % (0,517 à
  0,306 s, `/api/v1/applications/application_1795715756506_0407/sql`).

Le partitionnement par jour donne ici des fichiers de 0,6 Mo, très loin du bloc HDFS de 128 Mo.
À ce volume, le partitionnement par date ne paie que si le listage reste bon marché.

**Compression.** zstd stocke et lit 25 % d'octets de moins que snappy. Sur la durée, aucune
différence n'est détectable (−5,6 %, p = 0,841). La décompression ne limite pas cette requête :
l'étape qui lit les commandes lit leurs 31 petits fichiers en 4 tâches (champ `key_stage.tasks`
de `runs.json`). Les 403 tâches du tableau comprennent les 396 du listage.

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
(`application_1795715756506_0408`, option `--salt-on transaction_id`, son plan dans le History
Server lit `pmod(xxhash64(transaction_id#0, 42), 16)`). Cette colonne, inutile à la jointure,
ajoutait 48 Mo à la lecture (+44,1 % d'octets lus), pour 3,45 s. `event_time`, que la condition
d'intervalle lit déjà, supprime ce surcoût de lecture. Dans les deux cas, la sous-clé elle-même
ajoute 49 Mo au shuffle. Les tests vérifient que la jointure salée rend les mêmes lignes que
`with_applicable_price`, pour les deux colonnes (`tests/perf/test_skew.py`).

### Résultats

Étape clé : l'étape de la jointure, celle qui lit le plus d'octets de shuffle (pour `broadcast`,
celle qui lit le plus d'octets en entrée). Ratios : médiane des 5 exécutions. Liens : exécution 3
de chaque série. Durées, ratios et p : `report.md` de
[`skew-4slots`](perf-evidence/skew-4slots/report.md) et
[`skew-8slots`](perf-evidence/skew-8slots/report.md).

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

Détail de l'étape 62 de `baseline` (4 tâches en parallèle, exécution 3, `runs.json`) :
16 tâches, durée médiane 321 ms et maximale 1 359 ms, shuffle lu médian 7 448 950 octets et
maximal 30 990 762 octets. Seule la tâche chaude déverse sur disque.

La première série à 4 tâches en parallèle (`application_1795715756506_0408`, salage sur
`transaction_id`, mêmes réglages AQE,
[`skew-first-txid-salt`](perf-evidence/skew-first-txid-salt/report.md)) donne `baseline`
2,74 s, `salted-16` 3,45 s (+25,9 %, p = 0,008), `aqe-skew-join` 2,58 s et `broadcast` 1,36 s
(−50,4 %, p = 0,008).

### Ce que disent ces chiffres

**Le déséquilibre est réel, son coût en temps est petit sur ce cluster.** La tâche chaude dure
4,2 fois la médiane. Mais les 16 tâches de l'étape cumulent 6,0 s de temps d'exécuteur, soit
1,5 s par cœur sur 4 cœurs : la tâche chaude (1,36 s) tient dans cette durée idéale. Avec
4 tâches en parallèle, une clé qui porte 20 % des lignes ne dépasse pas la part d'un cœur
(25 %). Sur 8 tâches en parallèle, la part d'un cœur tombe à 12,5 % : les tâches cumulent 9,2 s,
soit 1,15 s par cœur, et la tâche chaude (1,66 s) dépasse cette durée idéale d'environ 0,5 s.

**AQE skew join** découpe la partition chaude (20 tâches au lieu de 16) et supprime le
déversement, sans changer le volume de shuffle. Il ne ralentit aucune série. Aucun de ses gains
de durée n'est retenu par la règle du protocole. À 4 tâches en parallèle, la durée ne change
pas (+0,0 %, p = 0,841). Dans la première série à 4 tâches, AQE gagne 0,16 s (−6,0 %,
p = 0,056), non retenu non plus. Sur 8 tâches, AQE gagne 0,23 s (−9,9 %, p = 0,056), une
différence que la règle ne retient pas. La série à 8 tâches change d'ailleurs plus que la part
d'un cœur. Avec 4 cœurs par exécuteur de 896 Mio, chaque tâche dispose de la moitié de la
mémoire d'exécution : le pic de la tâche chaude de l'étape 62 passe de 184,6 Mo à 92,3 Mo
(exécution 3), le temps de GC de l'étape triple (médiane des 5 exécutions : 228 ms à 4 tâches,
731 ms à 8), et le salage se met à déverser 26 Mo. Ces lectures viennent de
`/api/v1/applications/<id>/stages/62/0` et de son `taskSummary`, pour `_0409` et `_0410`. La
mesure laisse donc au plus un petit gain d'AQE, entre 0 et 10 %, sans montrer qu'il dépend du
nombre de tâches en parallèle.

**Le salage** corrige aussi le ratio (1,5 à 1,8), mais il ajoute la sous-clé à chaque ligne
mélangée : +34 % de shuffle. Il est plus lent dans les trois séries (p = 0,016 à 4 et 8 tâches,
p = 0,008 dans la première série). Sans l'exécution mesurée 1, le ralentissement à 8 tâches reste
de +21,9 %, mais une paire sur 16 est dans l'autre sens : à 4 contre 4, p = 0,057. C'est
la seule conclusion que la [sensibilité à l'échauffement](#protocole-commun) change, par manque
de puissance et non de sens.

**La diffusion** supprime le shuffle des commandes : les 1 326 versions vont à chaque exécuteur
et la jointure se fait pendant la lecture. Elle est 48 à 55 % plus rapide que la jointure triée
et ne peut pas être déséquilibrée.

### Correction retenue

| Correction | Ratio de durée | Durée (4 cœurs) | Durée (8 cœurs) | Coût | Quand l'utiliser |
| --- | ---: | ---: | ---: | --- | --- |
| Diffusion | 1,0 | −48 % | −55 % | le petit côté doit tenir en mémoire de chaque exécuteur | dimension sous le seuil de diffusion |
| AQE skew join | 1,5 | 0 %, non retenu | −10 %, non retenu | seuils à adapter au volume | jointure triée inévitable |
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

**Pourquoi le cache perd sur la lignée de gold.** Décomposition de l'exécution 3 par job
(`/api/v1/applications/application_1795715756506_0413/jobs`). Sur les 5 exécutions, l'étape de
lecture de `cache-3`, qui remplit le cache, dure 4,23 s en médiane (`report.md`).

| Étape | Sans cache | Avec cache |
| --- | --- | --- |
| Premier indicateur, ou construction du cache | 1,42 s (3,4 s de temps d'exécuteur) | 4,23 s (10,4 s de temps d'exécuteur) |
| Indicateurs suivants | 1,51 s et 0,81 s | 0,71 s, 0,80 s et 0,34 s, lus dans le cache |

Recalculer ce fait coûte peu : une lecture Parquet vectorisée de 7 colonnes et une jointure
diffusée contre 1 326 versions. Le remplir dans le cache coûte 3 fois plus de temps d'exécuteur :
l'encodage en colonnes compressées de 9,8 M de lignes. Un indicateur coûte environ 1,25 s s'il
recalcule le fait, et 0,6 s s'il le lit dans le cache : avec 4,23 s de construction, le cache ne
paie qu'à partir d'environ 7 indicateurs (inféré de ces durées). À 6 indicateurs, la mesure ne
montre pas de différence détectable (+0,4 %, p = 0,421), ce qui s'accorde avec ce seuil sans le
prouver. Quand la lignée contient un shuffle, chaque recalcul coûte 3,1 à 3,5 s : le cache paie
dès 3 indicateurs, et supprime en plus 188 Mo de déversement.

**Règle retenue.** Mettre en cache un DataFrame seulement si sa lignée est coûteuse (shuffle,
lecture CSV) et réutilisée, ou bon marché mais réutilisée plus de 6 fois. Le cas de la lecture CSV
et le seuil de 6 sont inférés, pas mesurés : aucune variante ne lit du CSV ni ne réutilise le fait
plus de 6 fois. La construction de gold persiste `fact_sales`, puis le relit pour le comptage des
lignes, les violations de grain, les invariants, 6 des 9 indicateurs et l'écriture : au moins
10 lectures, au-delà du seuil inféré. Le gain du cache dans le job gold n'est pas mesuré.

### Nombre de partitions de shuffle

Agrégation large : CA et lignes par client et produit sur les 9,8 M de lignes (3 982 974 groupes),
suivie d'un comptage. Valeurs testées : 4, 8 (cœurs × 2), 16 (valeur de la plateforme), 64 et
200 (défaut de Spark), regroupement AQE désactivé
(`spark.sql.adaptive.coalescePartitions.enabled=false`), puis 200 avec regroupement, pour
comparaison. Application `application_1795715756506_0414`. Les deux étapes sont l'étape de
lecture, qui lit les commandes, pré-agrège et écrit le shuffle, et l'étape de réduction, qui
lit le shuffle et finit l'agrégation. Leur durée va du lancement de la première tâche à la fin
de l'étape, en médiane des 5 exécutions, avec le p contre 16 partitions
([`perf-evidence/partitions/report.md`](perf-evidence/partitions/report.md)).

| Partitions | Durée médiane (s) | Min à max (s) | p contre 16 | Tâches de réduction | Octets de shuffle | Étape de lecture (s) | p | Étape de réduction (s) | p |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 6,30 | 6,02 à 6,44 | 0,690 | 4 | 105 516 536 | 5,12 | 0,421 | 1,03 | 0,016 |
| 8 | 6,24 | 5,92 à 6,58 | 1,000 | 8 | 107 968 334 | 5,19 | 0,690 | 0,98 | 0,056 |
| 16 | 6,29 | 5,84 à 6,39 | — | 16 | 110 571 506 | 5,33 | — | 0,86 | — |
| 64 | 6,35 | 6,14 à 7,02 | 0,548 | 64 | 115 193 240 | 5,21 | 0,548 | 1,00 | 0,056 |
| 200 | 6,85 | 6,32 à 7,50 | 0,016 | 200 | 118 048 664 | 5,69 | 0,095 | 1,10 | 0,008 |
| 200, regroupement AQE | 6,47 | 6,38 à 7,49 | 0,016 | 5 | 118 031 121 | 5,37 | 0,222 | 1,02 | 0,016 |

**Forme du résultat.** De 4 à 64 partitions, aucune différence de durée totale n'est
détectable. À 200 partitions, la durée augmente de 9 % (6,29 à 6,85 s, p = 0,016).

L'étape de lecture domine : 5,1 à 5,7 s sur environ 6,3. Elle a 5 tâches pour 4 cœurs et
déverse 81 Mo sur disque, car la pré-agrégation ne réduit les 9 843 644 lignes lues qu'à
6 744 189 (−31 %, métriques SQL de `/api/v1/applications/application_1795715756506_0414/sql`) :
il reste presque 4 M de groupes. Ses écarts entre nombres de partitions ne sont pas retenus par
la règle du protocole (p = 0,095 au mieux, pour 200). À 200 partitions, chaque tâche de lecture
écrit 200 segments de shuffle au lieu de 16, et les octets de shuffle augmentent de 7 %. Les deux
variantes à 200 partitions écrivent les mêmes segments, mais leurs étapes de lecture diffèrent
de 0,32 s en médiane (5,69 et 5,37 s), autant que l'écart avec 16 partitions : la mesure ne
permet pas d'attribuer un surcoût à l'écriture des segments.

L'étape de réduction dessine un U : 1,03 s à 4 partitions, 0,98 à 8, 0,86 à 16, 1,00 à 64 et
1,10 à 200. Contre 16, l'écart est retenu à 4 (p = 0,016) et à 200 (p = 0,008), pas à 8 ni à 64
(p = 0,056).

- À 4 partitions, il y a autant de tâches de réduction que de cœurs : une seule vague, chaque
  tâche agrège un quart des groupes (26 Mo de shuffle), et aucune tâche ne reste pour occuper un
  cœur libéré. La cause précise des 0,17 s de plus n'est pas mesurée.
- À 200, chaque tâche ne reçoit que 0,6 Mo. Le coût fixe d'une tâche (lancement, 5 blocs à
  récupérer) se paie 200 fois sur 4 cœurs, soit 50 vagues.

Le regroupement d'AQE ramène la réduction à 5 tâches. Son étape de réduction (1,02 s) reste plus
lente que celle à 16 partitions (p = 0,016), comme à 4 partitions, et sa durée totale aussi
(+2,9 %, p = 0,016). Un point reste sans explication : la durée de `sp-200` augmente à chaque
tour (6,32, 6,79, 6,85, 6,99 puis 7,50 s), alors que celle de `sp-16` ne bouge pas. La valeur de
la plateforme, 16, est au creux du U et dans la zone plate de la durée totale : elle reste le bon
choix pour ces volumes. Le balayage la mesure sans regroupement d'AQE ; la plateforme l'active,
et 16 partitions avec regroupement n'a pas été mesuré. Les octets déversés sont les mêmes pour toutes les variantes : le
déversement vient de l'étape de lecture, et aucune étape de réduction ne déverse, même avec
4 partitions de 26 Mo.

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
| `Scan parquet (1)` | lit `fact_sales` ; `PartitionFilters` porte sur la colonne de partition, donc Spark ne lit que les dossiers `order_date=2025-12-*` | le listage de tous les dossiers, puis 31 dossiers lus sur les 367 de `fact_sales` dans le lac `dev` ; `ReadSchema` ne contient que `revenue_eur` : `order_date` vient du nom du dossier, pas du fichier |
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

RS-6-3 place 9 blocs par groupe et RS-3-2 en place 5. HDFS ne met jamais deux blocs d'un même
groupe sur un DataNode : chaque bloc demande un nœud distinct. Sur 3 DataNodes, une écriture sous
RS-6-3 est donc refusée. Dans un répertoire de test qui porte la politique RS-6-3-1024k,
`hdfs dfs -put` d'un fichier de 3 Mo répond
`could only be written to 3 of the 6 required nodes for RS-6-3-1024k. There are 3 datanode(s) running and 3 node(s) are excluded in this operation.`,
sort en erreur, et le répertoire reste vide. Le NameNode exige au moins un nœud par bloc de
données d'un groupe : 6 pour RS-6-3. RS-6-3 est activée sur le cluster (politique par défaut de
Hadoop) : toute écriture dans un répertoire qui la recevrait échouerait de la même façon.
RS-3-2 est désactivée sur ce cluster : `hdfs ec -setPolicy` la refuse
(`does not match any enabled erasure coding policies`). Activée, elle demanderait 3 nœuds pour
ses 3 blocs de données, ce que le cluster a, mais aucun nœud ne resterait pour ses 2 blocs de
parité : ses fichiers s'écriraient sans parité, donc sans tolérance à la perte d'un nœud. Ce
dernier point est inféré de la règle du NameNode et n'a pas été essayé, car activer une
politique change la configuration de la plateforme. XOR-2-1 place exactement 3 blocs, un par
DataNode. L'avertissement sur les racks concerne la tolérance à la perte d'un rack entier,
impossible sur un cluster d'un seul rack. La perte d'un DataNode reste couverte.

### Volumes mesurés

La mesure rapportée ici a copié le fichier de commandes J1 du profil `full` (713 401 398
octets) et le fichier produits J1 (79 966 octets, plus petit qu'une cellule) dans
`/benchmarks/rep3` et `/benchmarks/ec`, les fichiers gardés pour KAN-52. Depuis, chaque
exécution de `scripts/perf/storage.sh` écrit dans sa propre zone,
`/benchmarks/storage/<exécution>/rep3` et `.../ec` (`[perf] storage`, options `--root` et
`--run`). L'outil refuse une zone qui existe déjà, ne remplace jamais un fichier, pose lui-même
`XOR-2-1-1024k` sur `ec` et vérifie les deux politiques avec `hdfs ec -getPolicy` avant la
première copie.
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
comprise), 5 copies par mode en alternance. Médianes, étendues et p :
[`perf-evidence/storage/report.md`](perf-evidence/storage/report.md), refait depuis
`result.json`.

| Mode | Surcoût de stockage | Lecture complète (s) | Lecture du petit fichier (s) | Copie (s) |
| --- | ---: | ---: | ---: | ---: |
| Réplication 3 | 200 % | 1,77 (1,69 à 2,07) | 1,14 (1,13 à 1,16) | 3,71 (3,04 à 6,57) |
| XOR-2-1-1024k | 50 % | 1,69 (1,59 à 1,76) | 1,20 (1,13 à 1,32) | 3,39 (3,07 à 4,11) |
| p bilatéral | | 0,095 | 0,151 | 0,310 |

Aucune différence de durée n'est détectable, en lecture comme en copie. La lecture du petit
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
([`docs/perf-evidence/storage/`](perf-evidence/storage/)). Les identifiants de blocs relus
après le lot de corrections sont ceux de ces sorties : aucune commande documentée n'écrit plus
dans ces deux répertoires. Pour la démonstration, comparer la somme `COMPOSITE_CRC` de la copie
en erasure coding, avant et pendant la panne, à `5056866c`.

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

## Séries non rapportées

Trois applications complètes du même créneau ne sont pas les séries des tableaux. Ce sont des
versions antérieures de l'outil, remplacées par une série plus complète du même code. Leurs
exécutions et rapports sont dans
[`docs/perf-evidence/superseded/`](perf-evidence/superseded/). Elles vont dans le même sens que
les séries rapportées.

| Application | Expérience | Pourquoi elle n'est pas rapportée | Résultat |
| --- | --- | --- | --- |
| `application_1795715756506_0406` | `formats`, 4 dispositions | remplacée par `_0407`, qui ajoute les deux variantes de listage du Parquet partitionné | CSV 5,09 s, Parquet 0,69 s, partitionné 1,23 s, zstd 1,06 s |
| `application_1795715756506_0411` | `cache`, 3 indicateurs | remplacée par `_0412` puis `_0413`, qui ajoutent 6 indicateurs puis la lignée triée | cache +55,0 % (p = 0,008) |
| `application_1795715756506_0412` | `cache`, 3 et 6 indicateurs | remplacée par `_0413`, qui refait les deux et ajoute la lignée triée | cache +58,8 % à 3 indicateurs (p = 0,008), +6,7 % à 6 (p = 0,095, non retenu) |

## Reproduire

Depuis la racine du dépôt, cluster démarré, sans autre application sur YARN. Les séries
rapportées ont tourné avec un seul tour d'échauffement : ajouter `--warmup 1` à chaque commande
chronométrée pour refaire le même protocole.

```bash
export GTDP_PSEUDO_SALT=<valeur hors dépôt>      # inutile au banc : seuls silver et son rapport lisent le sel

scripts/perf/bench.sh land                       # profil full, graine 42 : orders, customers, products et manifestes sous /benchmarks/mr-vs-spark/full/landing
scripts/perf/bench.sh prepare                    # tables des 4 dispositions sous /benchmarks/perf/full
scripts/perf/bench.sh formats                    # KAN-44
scripts/perf/bench.sh skew --label 4slots        # KAN-45, 4 tâches en parallèle
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.cores=4" \
  scripts/perf/bench.sh skew --label 8slots      # KAN-45, 8 tâches en parallèle
scripts/perf/bench.sh skew --label first-txid-salt --salt-on transaction_id   # KAN-45, première série
scripts/perf/bench.sh cache                      # KAN-46
scripts/perf/bench.sh partitions                 # KAN-46
scripts/perf/bench.sh explain                    # KAN-46, plans du lac dev
scripts/perf/storage.sh \
  --source /benchmarks/mr-vs-spark/full/landing/orders/ingestion_date=2026-01-01/orders_20260101.csv \
  --small /benchmarks/mr-vs-spark/full/landing/products/ingestion_date=2026-01-01/products_20260101.csv
scripts/perf/bench.sh record-hash                # constat bronze

# Nettoyage : garder /benchmarks/ec et /benchmarks/rep3 pour KAN-52
scripts/cluster/client.sh hdfs dfs -rm -r -skipTrash /benchmarks/perf /benchmarks/mr-vs-spark /benchmarks/storage
```

Chaque commande écrit son rapport dans `build/perf/<expérience>/`, et `storage.sh` sa zone HDFS
dans `/benchmarks/storage/<heure UTC>/`. Les preuves des résultats cités ici sont dans
[`docs/perf-evidence/`](perf-evidence/) : `runs.json` et `report.md` de chaque série,
`result.json` et sorties de `hdfs fsck` du stockage, plans et résultats non chronométrés. Le
rapport complet de la campagne MapReduce contre Spark y est aussi :
[`mr-vs-spark-campaign.md`](perf-evidence/mr-vs-spark-campaign.md). Les rapports se refont des
preuves versionnées, sans exécuter de benchmark :

```bash
scripts/perf/bench.sh report --out docs/perf-evidence/formats      # idem pour chaque série
scripts/perf/storage.sh --report docs/perf-evidence/storage
scripts/perf/bench.sh remeasure --out docs/perf-evidence/formats   # relit les étapes dans le History Server
```
