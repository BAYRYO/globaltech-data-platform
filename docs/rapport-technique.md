# Rapport technique : GlobalTech Data Platform

Ce rapport présente l'architecture de la plateforme, les choix qui la fondent, les résultats
mesurés et leurs limites. Il synthétise : chaque section donne la décision, le résultat mesuré et
la limite, puis renvoie à l'ADR, au document ou à la preuve qui tient le détail. Il est rédigé
pour un correcteur et pour l'équipe qui reprendrait la plateforme (story Jira KAN-56).

Chaque chiffre vient d'un document du dépôt, d'un fichier de preuve versionné ou d'un commentaire
de preuve d'une PR, cité à côté du tableau qui le donne. Les PR sont notées #n. Sauf mention
contraire, un p est le p bilatéral d'un test exact de Mann-Whitney sur 5 exécutions contre 5, et
une différence n'est retenue que si p < 0,05 ([protocole](performance.md#protocole-commun)).
« Pas de différence détectable » ne veut pas dire « égalité ».

## 1. Résumé

La plateforme est un data lake HDFS en trois couches, bronze, silver et gold, sur un cluster
Docker reproductible : 1 NameNode, 3 DataNodes, YARN avec 2 NodeManagers, Spark History Server et
JobHistory MapReduce. Des jobs Spark sur YARN ingèrent trois dépôts quotidiens J1, J2 et J3,
les nettoient, les historisent, les pseudonymisent et calculent neuf indicateurs dans un schéma en
étoile. Un job Structured Streaming agrège un flux de capteurs. Le même calcul est écrit en
Hadoop Streaming et en Spark pour comparer les deux moteurs. Chaque écriture se rejoue sans
doublon, chaque exécution laisse une ligne d'audit, et la perte d'un DataNode pendant l'écriture
de gold a été provoquée, mesurée et documentée.

| Ce qui compte | Résultat mesuré | Source |
| --- | --- | --- |
| Réconciliation silver de J1 | 1 106 403 lignes bronze = 1 070 797 propres + 25 518 rejets + 10 088 doublons ; 108 mesures comparées aux manifestes, 0 écart | [#11, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441) |
| Rejeu de gold | sur un silver inchangé : 0 date réécrite sur 367, les 14 tables gold ont le même `content_sum` | [#15, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7207818096) |
| Format de stockage | même requête sur 9 843 644 commandes : CSV 5,08 s, Parquet 0,61 s, soit −87,9 % (p = 0,008) | [performance](performance.md#naïf-contre-optimisé-kan-44) |
| Réplication contre erasure coding | XOR-2-1 : 50 % de surcoût contre 200 %, pas de différence de lecture détectable (p = 0,095) | [performance](performance.md#réplication-3-contre-erasure-coding-kan-47) |
| Perte d'un DataNode | mort déclarée entre 86 et 91 s après le dernier battement de cœur (90 s attendues), 0 bloc perdu, gold réécrite pendant la panne identique à la gold publiée | [fiche d'incident](incidents/2026-11-27-perte-datanode.md) |

Limites principales, détaillées en [section 12](#12-limites-et-pistes-damélioration) :

- 3 DataNodes pour une réplication 3 : après une panne, les blocs perdus n'ont aucune cible de
  copie. Pendant l'exercice, ils sont restés à 2 répliques pendant 73 min 08 s.
- Un seul NameNode et un seul rack : la perte du NameNode arrête HDFS.
- Gold se recalcule depuis tout l'historique silver à chaque exécution, et un rejeu silver
  lancé pendant une exécution gold n'est pas encore détecté.
- Le verrou de table garde une fenêtre résiduelle si le ResourceManager reste injoignable.
- Toutes les mesures viennent d'une seule machine de 16 cœurs, avec 10 M de lignes de commandes
  au plus.

## 2. Contexte et objectifs

GlobalTech Industries pilote sa chaîne logistique avec des fichiers Excel et des exports isolés,
sans historique fiable. Le sujet demande un prototype de plateforme Big Data sur Hadoop et Spark
qui collecte, nettoie, historise et analyse ces données, puis les expose en indicateurs fiables.

### Les données

Le kit fourni (`data/kit/`) est propre : aucun doublon, aucune valeur manquante, une seule
version par produit. Le générateur `gtdp.datagen` le prend comme référentiel et produit, à graine
fixe, trois dépôts avec des anomalies volontaires. Chaque dépôt porte un manifeste qui compte les
anomalies injectées, et chaque ligne porte au plus une anomalie : les comptes attendus sont
exacts et testables ([contrat de données](data-contract.md)).

| Dépôt | `ingestion_date` | Contenu des commandes |
| --- | --- | --- |
| J1 | 2026-01-01 | historique 2025 |
| J2 | 2026-01-02 | commandes du 2026-01-01, retardataires de décembre, renvois de lignes de J1 |
| J3 | 2026-01-03 | commandes du 2026-01-02, nouvelle colonne `discount_pct` |

| Profil | Commandes J1 | Commandes J2, J3 (chacun) | Usage |
| --- | ---: | ---: | --- |
| `ci` | 20 000 | ~55 | tests et CI |
| `dev` | 1 000 000 | ~2 740 | démonstration, preuves sur le cluster |
| `full` | 10 000 000 | ~27 400 | benchmarks |

Source : [contrat de données](data-contract.md#profils-de-volume).

Le produit `P00042` porte 20 % des lignes synthétiques : c'est le déséquilibre de clé du sujet.
Sur `full`, le ratio max/médiane des lignes par produit du catalogue vaut 244,6
([contrat](data-contract.md#commandes-orders)). Les capteurs envoient un fichier JSON Lines
toutes les 5 s, avec des événements en retard, des doublons et des pics.

### Les questions auxquelles la plateforme répond

| Question | Table gold |
| --- | --- |
| Quel chiffre d'affaires par jour et par pays ? | `kpi_revenue_daily` |
| Quels sont les 5 meilleurs produits de chaque pays ? | `kpi_top_products_country` |
| Quelle tendance sur 7 jours ? | `kpi_revenue_moving_avg` |
| Le prix facturé suit-il le prix catalogue applicable ? | `kpi_price_deviation` |
| Quels taux d'annulation et de retour ? | `kpi_cancel_return_rate` |
| Quel panier moyen par segment ? | `kpi_avg_basket` |
| Le site web répond-il ? | `kpi_http_errors_hourly` |
| Combien de visites vont jusqu'à la commande ? | `kpi_conversion_funnel` |
| Quelle part des données est rejetée, et pourquoi ? | `kpi_data_quality` |
| Quelles machines sortent de leurs seuils ? | `sensor_aggregates`, `sensor_alerts` |

Définitions : [modèle de données](data-model.md#indicateurs-gold).

**Limite.** Les données sont synthétiques. Le kit garde des incohérences, conservées et
signalées : 49 580 commandes sur 200 000 sont antérieures à l'inscription du client, et le prix
facturé du kit est indépendant du catalogue (corrélation −0,0003)
([contrat](data-contract.md#incohérences-connues-du-kit)).

## 3. Architecture

Le [schéma d'architecture légendé](architecture.md#vue-densemble) montre les services, les zones
du lake et les flux. Une seule image Docker contient Java 11, Hadoop 3.3.6, Spark 3.5.3 et
Python 3.11. Chaque service lance un rôle de cette image
([ADR-0001](adr/0001-image-hadoop-spark.md)).

| Composant | Instances | Rôle |
| --- | ---: | --- |
| NameNode | 1 | métadonnées HDFS, ACL, quotas, politiques d'erasure coding |
| DataNode | 3 | blocs ; minimum pour la réplication 3 et pour `XOR-2-1` |
| ResourceManager | 1 | ordonnancement YARN |
| NodeManager | 2 | conteneurs Spark et MapReduce, 2 560 Mio chacun |
| Spark History Server, JobHistory | 1 + 1 | historique des applications |
| Client | à la demande | conteneur éphémère : `land.sh`, `submit.sh`, driver Spark en mode client |

### Choix et justification

| Décision | Justification mesurée ou calculée | Référence |
| --- | --- | --- |
| Une image construite par le projet | l'image du kit n'a ni HDFS ni YARN ; une seule version de Hadoop, client compris | [ADR-0001](adr/0001-image-hadoop-spark.md) |
| Mémoire fixée service par service | Docker dispose de 15 444 Mio ; sans réglage, chaque NodeManager annoncerait 8 192 Mio. Budget : 12 032 Mio pour le cluster, 13 312 Mio avec un driver, 13 824 Mio au pire cas de deux drivers | [budget mémoire](platform.md#budget-mémoire) |
| 3 DataNodes, réplication 3 | 2 DataNodes ne permettent ni réplication 3 ni erasure coding ; 5 DataNodes pour `RS-3-2` retireraient environ 2 Gio aux exécuteurs | [ADR-0006](adr/0006-topologie-hdfs.md) |
| Détection d'un DataNode mort en 90 s | `2 × 30 s + 10 × 3 s` ; 630 s avec les valeurs par défaut | [ADR-0006](adr/0006-topologie-hdfs.md) |
| Une racine HDFS par couche | droits, quotas et politiques s'appliquent à un répertoire : écriture pour l'ETL, lecture de gold pour les analystes | [architecture](architecture.md#arborescence-hdfs) |
| Rejets fermés aux analystes | `silver/rejects` garde les lignes brutes, données personnelles en clair : ACL `group:analysts:---` héritée | [droits et quotas](platform.md#arborescence-hdfs-droits-et-quotas) |
| Quotas de noms | un micro-batch crée plusieurs fichiers ; 500 000 noms sur `/datalake/streaming` | [droits et quotas](platform.md#arborescence-hdfs-droits-et-quotas) |
| `maximum-am-resource-percent` = 0,5 | à 0,1, une seule application tournerait à la fois sur ce cluster | [notes de conception](platform.md#notes-de-conception) |

Un NodeManager de 2 560 Mio loge deux exécuteurs Spark de 1 280 Mio, ou un exécuteur et un
ApplicationMaster. Deux applications en mode client tournent côte à côte avec un exécuteur
chacune, ce qui permet de faire tourner le streaming à côté d'un batch
([placements mesurés](platform.md#budget-mémoire)).

### Relance par un tiers

| Mesure | Valeur | Source |
| --- | --- | --- |
| Premier build de l'image | 6 min 52 s, 1,04 Go d'archives Apache | [guide](platform.md#prérequis) |
| Clone neuf, image construite, volumes vidés : `up.sh`, `init-hdfs.sh`, `smoke/run.sh` | 15 s + 26 s + 54 s = 99 s | [guide](platform.md#démarrage-en-moins-de-10-minutes) |
| Second `init-hdfs.sh` | 22 s, ne change rien | [guide](platform.md#démarrage-en-moins-de-10-minutes) |

Le smoke test lance un job Spark sur YARN visible dans le History Server, le job MapReduce `pi`
et un job Hadoop Streaming. La CI le rejoue sur un cluster neuf quand une PR change la
plateforme (workflow `platform.yml`).

**Limite.** Les 3 DataNodes et les 2 NodeManagers partagent les 16 cœurs, la mémoire et le disque
d'une seule machine. Il y a un seul NameNode et un seul rack : `hdfs ec` avertit que `XOR-2-1`
demande 3 racks pour tolérer la perte d'un rack. Les 10 minutes du sujet supposent l'image
construite. Spark 3.5.3 ne se télécharge plus que sur archive.apache.org, dont le débit fixe la
durée d'un build à froid : 27 min, puis 3 min 23 s, mesurés le même jour
([#24](https://github.com/BAYRYO/globaltech-data-platform/pull/24)).

## 4. Ingestion et couche bronze

### Décisions

- **Contrôle avant écriture.** Le job `bronze` vérifie chaque fichier du landing contre le
  manifeste : SHA-256, nombre de lignes, UTF-8 strict, colonnes obligatoires dans l'en-tête. Un
  fichier non conforme arrête le job avant toute écriture.
- **Aucune perte.** Chaque fichier est lu avec un schéma tout en `string` construit à partir de
  son propre en-tête ([ADR-0007](adr/0007-evolution-schema.md)). Bronze ajoute `_ingested_at`,
  `_source_file`, `_run_id`, `_record_hash` (SHA-256 de la ligne brute) et `_corrupt_record`.
- **Rejeu sans doublon : écrire puis échanger.** Le job nomme les partitions qu'il produit.
  `write_partitions` écrit tout dans un répertoire de transit, puis remplace chaque partition
  nommée par un renommage. Une écriture qui échoue laisse la table intacte
  ([ADR-0002](adr/0002-parquet-idempotence.md)).
- **Verrou de table.** Chaque écriture prend `_<table>.lock`, créé par un renommage sans
  écrasement que le NameNode refuse s'il existe. Un verrou existant est jugé d'après l'état YARN
  de l'application qu'il nomme : terminée, il est repris ; vivante, il est tenu ; sans réponse
  de YARN, une règle d'âge de 6 h s'applique ([garantie](adr/0002-parquet-idempotence.md#garantie)).
- **Audit.** Chaque exécution ajoute une ligne à `ops/audit`, en échec comme en succès
  ([table d'audit](adr/0002-parquet-idempotence.md#table-daudit-et-arrêt-dune-exécution)).

Le mode `partitionOverwriteMode=dynamic` de Spark a été écarté sur mesure : il ne remplace que les
partitions qui reçoivent une ligne, et en mode statique le même `overwrite` efface toutes les
autres partitions ([ADR-0002](adr/0002-parquet-idempotence.md#pourquoi-nommer-les-partitions)).

### Résultats

| Jour | Lignes lues = écrites | Ce qui le prouve |
| --- | ---: | --- |
| J1 | 1 106 403 | `manifest_rows` = `landing_lines` = `rows_read` = `rows_written` pour les 16 fichiers des 4 exécutions |
| J2 | 3 075 | idem |
| J3 | 3 075 | idem ; `discount_pct` présent sur 2 725 lignes |
| J2 rejoué | 3 075 | les 12 `hash_sum` (une somme de contrôle par source et par jour) identiques à ceux du premier tour : le rejeu donne le même contenu |

Source : [#7, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/7#issuecomment-7202646257).
Le rapport bronze compare landing et bronze : 0 ligne de bronze absente du landing sur les
12 partitions, et une ligne de landing absente de bronze par fichier CSV, son en-tête.

| Preuve du verrou | Résultat | Source |
| --- | --- | --- |
| Vérificateur de modèle : tous les entrelacements de 2 à 4 exécutions | 16 scénarios, 419 888 états, 0 violation | [#18](https://github.com/BAYRYO/globaltech-data-platform/pull/18) |
| Mutations de `io.py` | 17 sur 17 tuées par les tests | [#18](https://github.com/BAYRYO/globaltech-data-platform/pull/18) |
| `kill -9` du driver en mode client | YARN rapporte l'application terminée en moins de 0,5 s ; le rejeu reprend le verrou aussitôt | [ADR-0002](adr/0002-parquet-idempotence.md#verrou-de-table) |

**Limite.** `_record_hash`, écrit en 64 caractères hexadécimaux, occupe 73,4 % des octets de
`bronze/orders`. Stocké en binaire, il prendrait 47,4 % de place en moins
([constat](performance.md#constat--_record_hash-en-bronze)). Un `kill -9` entre la suppression
et le renommage d'une partition la laisse absente jusqu'au rejeu du jour. Le verrou couvre la
table entière : une seconde exécution bronze échoue même si elle traite un autre jour.

## 5. Couche silver

### Décisions

- **Contrôles en un seul passage.** Un registre de règles par source (code, action, condition)
  produit les motifs de rejet et les drapeaux en une seule projection. Chaque ligne bronze reçoit
  une seule disposition, dans cet ordre : doublon exact, rejet, doublon de clé dans le dépôt,
  doublon inter-jours, propre ([ordre des opérations](data-model.md#ordre-des-opérations-et-réconciliation)).
- **Rejets avec motif.** Une ligne rejetée va dans `silver/rejects/<source>` avec la liste de
  ses codes et la ligne brute. Une fiche client n'est rejetée que si elle est inexploitable : un
  email manquant devient un drapeau, pas un rejet.
- **Réconciliation à chaque exécution.** `bronze(D) = propres + rejets + doublons` et l'unicité de
  la clé sont vérifiées avant l'écriture, puis sur les données relues. Un échec avant l'écriture
  ne publie rien. Un échec après retire la publication de la source.
- **Jours complets.** Un marqueur par source publiée, puis `_SUCCESS` quand les quatre sources
  sont publiées. Les doublons inter-jours ne se cherchent que dans les jours complets antérieurs,
  ce qui rend le rejeu d'un jour idempotent. Le job refuse un jour si un jour antérieur est
  incomplet ou si un jour postérieur est complet ([ADR-0005](adr/0005-silver-incremental.md)).
- **SCD type 2 des produits.** `products_scd2` est reconstruite depuis bronze jusqu'à `D`. Une
  version ne s'ouvre que si un attribut historisé change. La version 1 vaut depuis le
  `1900-01-01`, ce qui donne exactement un prix à chaque commande. Le prix applicable est celui de
  la version telle que `valid_from <= event_time < valid_to`.
- **Changement de schéma de J3.** Bronze lit chaque fichier avec son en-tête, silver lit bronze
  avec le schéma canonique. `discount_pct` absent ou vide vaut `0.000`. Une colonne inconnue est
  gardée en bronze et signalée dans l'audit ([ADR-0007](adr/0007-evolution-schema.md)).
- **Pseudonymisation.** `email`, IP et ligne de log deviennent `sha2(sel ‖ valeur, 256)` ; nom et
  téléphone sont supprimés. Le sel vient d'une variable hors dépôt et entre dans le calcul comme
  une donnée diffusée, jamais comme un littéral. Silver refuse un sel faible et tout sel différent
  de celui dont il a enregistré l'empreinte ([ADR-0004](adr/0004-pseudonymisation.md)).

Le sel est passé comme donnée parce que la revue du Lot 0 a trouvé un `lit(sel)` en clair dans
`explain` et dans l'event log servi par le History Server. Le hachage salé de la ligne de log est
nécessaire parce qu'un hachage non salé, recalculé depuis les autres colonnes et les 256 adresses
du `/24`, redonnait l'adresse IP.

### Réconciliation de J1 à J3

Silver réel reconstruit sur le cluster, profil `dev`, lignes d'audit des exécutions :

| Jour | Lignes bronze | Propres | Rejets | Doublons | Somme |
| --- | ---: | ---: | ---: | ---: | --- |
| J1 | 1 106 403 | 1 070 797 | 25 518 | 10 088 | égale |
| J2 | 3 075 | 2 958 | 73 | 44 | égale |
| J3 | 3 075 | 2 958 | 73 | 44 | égale |

Source : [#11, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7202378972)
et [tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441).

Le rapport silver compare chaque compte au manifeste, à l'audit et aux tables relues : 108
mesures, 0 écart, dont 105 égales et 3 `info` pour le drapeau `FUTURE_SIGNUP` que le manifeste ne
prévoit pas (688 clients en J1, incohérence connue du kit). Extrait :

| Source | Jour | Mesure | Manifeste | Audit | Table |
| --- | --- | --- | ---: | ---: | ---: |
| `orders` | J1 | lignes propres | 965 000 | 965 000 | 965 000 |
| `orders` | J1 | doublons exacts | 10 000 | 10 000 | |
| `orders` | J1 | rejets `BAD_QUANTITY` | 5 000 | 5 000 | 5 000 |
| `orders` | J2 | doublons inter-jours | 14 | 14 | |
| `customers` | J1 | doublons de clé | 25 | 25 | |
| `products` | J1 | rejets `CONFLICTING_VERSION` | 6 | 6 | 6 |
| `weblogs` | J1 | rejets `MALFORMED_LINE` | 500 | 500 | 500 |

Le manifeste compte 3 anomalies `CONFLICTING_VERSION` en J1. Silver rejette les 2 lignes de
chaque groupe en conflit, soit 6.

### Autres preuves

| Critère | Résultat | Source |
| --- | --- | --- |
| Rejeu de J2 | `content_sum` des 17 partitions et tables identique avant et après ; noms, tailles et dates des partitions de J1 inchangés | [#11, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441), [preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7202378972) |
| J2 après J3 | refusé (`LaterPartitionError`) ; accepté avec `--force`, puis J3 rejoué donne le même contenu | [#11](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7202378972) |
| Contrôle en échec après écriture, puis `kill -9` pendant l'écriture | jour incomplet, jour suivant refusé, rapport en échec ; réparation selon le runbook, contenu identique | [#11](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441) |
| SCD2 de `P00042` | 2 versions, 504,06 puis 530,97 EUR à partir du 2025-09-25 08:19:03 ; une commande de 08:11:51 prend 504,06, une de 08:19:09 prend 530,97 | [#11](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7202378972) |
| Invariants SCD2 | 0 ligne en violation : clé, intervalle vide, recouvrement, trou, `is_current`, version 1 depuis 1900 | idem |
| Changement de schéma | `discount_pct` = `0.000` sur J1 et J2, jamais nul ; les 14 doublons inter-jours renvoyés dans J3 avec un champ vide sont comptés comme doublons | idem |
| Ré-identification des IP sans le sel | 0 sur 100 046 ; avec le sel, 100 046 sur 100 046 | [#11, vérification du lead](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7205140926) |
| Confirmation de clients sans le sel | 0 sur 5 024 | idem |
| Sel dans ce que sert le History Server | 0 occurrence (plans, environnement, jobs, étapes) | [#11](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441) |
| Sel faible ou différent | sel de développement, sel de 15 caractères et autre sel refusés, rien d'écrit | idem |
| Date antérieure à 1900 (KAN-59) | une commande, un log ou une version produit est rejeté (`ANCIENT_TIMESTAMP`), un client est gardé avec un drapeau ; avant le correctif, une seule ligne faisait échouer tout le job (`WRITE_ANCIENT_DATETIME`) | [#22](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7214321020) |

Coût des contrôles : J2 passe de 19,1 s à 53,5 s, dont 8,2 s pour les deux contrôles d'unicité
de clé sur les 965 000 commandes de J1. Ces durées ont été prises avec le streaming à côté : elles
sont indicatives ([runbook silver](runbook/silver.md#traiter-un-jour)).

**Limite.** La donnée est pseudonymisée, pas anonymisée : avec le sel, la correspondance reste
calculable. Restent en clair `user_agent`, la requête de `path`, le préfixe IP et les
quasi-identifiants des clients ([risque résiduel](data-dictionary/silver.md#pseudonymisation)).
Changer le sel impose de reconstruire silver puis gold. Il n'y a pas d'option `--cascade` :
corriger un jour passé demande `--force`, puis le rejeu de chaque jour suivant.

## 6. Couche gold

### Décisions

- **Schéma en étoile.** `fact_sales` (grain : une ligne de commande, `transaction_id` unique),
  `fact_web_events`, `dim_date`, `dim_customer`, `dim_product` ([modèle](data-model.md#gold--modèle-en-étoile)).
- **Prix applicable.** Chaque commande rejoint la version de `dim_product` valide à son
  `event_time`. `revenue_eur` est calculé au prix catalogue applicable, `invoiced_amount_eur` au
  prix facturé, tous deux en `decimal(20,2)`.
- **Attributs du client à jour.** `dim_customer` est de type SCD 1 et `fact_sales` ne copie
  aucun attribut du client. Les indicateurs par pays ou par segment joignent `dim_customer` à
  chaque calcul.
- **Gold est une fonction des jours silver complets.** Chaque exécution recalcule tout depuis ces
  jours. Elle compare une empreinte par `order_date` à la table publiée, réécrit les dates qui
  diffèrent et supprime les dates sans commande. Gold refuse de s'exécuter si `customers` ou
  `products_scd2` portent une ligne ou un `_built_for.json` d'un jour incomplet (KAN-58).
- **Contrôles fermés avant toute écriture.** Chaque commande a exactement une version, chaque
  table a une clé unique et aucune colonne obligatoire nulle, aucune référence n'est orpheline.
  Au premier écart, gold s'arrête sans rien écrire ([références et contrôles](data-dictionary/gold.md#références-et-contrôles)).
- **Publication.** Un verrou couvre toute l'exécution, et `gold/_state.json` vaut `PUBLISHING`
  pendant l'écriture, puis `PUBLISHED` après la relecture de `fact_sales`.

Le recalcul limité aux dates touchées par le jour a été écarté : une date dépend aussi des
versions des produits et des clients, qu'un jour silver peut changer pour des dates anciennes
([runbook gold](runbook/gold.md#ce-que-fait-une-exécution)). La revue de #15 l'a mesuré sur la
première version, qui copiait le pays du client dans le fait : après J3, 5 202 lignes de 27
clients portaient un pays périmé, et le rapport gold restait entièrement `ok`
([revue](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7206133610)).

| Indicateur | Mécanisme Spark | Lignes |
| --- | --- | ---: |
| `kpi_revenue_daily` | agrégation | 2 202 |
| `kpi_top_products_country` | `dense_rank()` sur une fenêtre par pays, top 5 | 30 |
| `kpi_revenue_moving_avg` | fenêtre `rowsBetween(1 - jours, 0)`, 7 jours, sur un calendrier complet | 2 202 |
| `kpi_price_deviation` | jointure SCD2 par intervalle, médiane exacte | 65 |
| `kpi_cancel_return_rate` | agrégation conditionnelle | 32 020 |
| `kpi_avg_basket` | agrégation | 6 603 |
| `kpi_http_errors_hourly` | agrégation conditionnelle | 8 808 |
| `kpi_conversion_funnel` | agrégation conditionnelle | 3 |
| `kpi_data_quality` | agrégation des rejets silver | 42 |

Lignes : [#15, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7205455223), lac `dev`.

### Résultats

Les retardataires recalculent leur date sans perdre les lignes déjà publiées. Lignes de
`fact_sales` par `order_date`, sur un silver qui grandit de J1 à J3 :

| `order_date` | après gold J1 | après gold J2 | après gold J3 |
| --- | ---: | ---: | ---: |
| 2025-12-29 | 2 625 | 2 654 | 2 654 |
| 2025-12-30 | 2 582 | 2 609 | 2 629 |
| 2025-12-31 | 2 560 | 2 583 | 2 612 |
| 2026-01-01 | | 2 552 | 2 582 |
| 2026-01-02 | | | 2 552 |
| total (dates) | 965 000 (365) | 967 631 (366) | 970 262 (367) |

Source : [#15, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7205455223).
Une partition du 2025-12-30 recalculée à partir de J2 seul aurait 27 lignes au lieu de 2 609.

| Preuve | Résultat | Source |
| --- | --- | --- |
| Silver et gold entrelacés jour par jour, contre gold construit une fois sur le silver final | les 14 tables ont le même `content_sum` | [#15, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7207818096) |
| Rejeu de gold J1 après J3 | 0 date réécrite, 367 inchangées, 14 `content_sum` identiques | idem |
| `silver --force J1`, puis gold J1 | 3 dates réécrites, 2 supprimées, `PUBLISHED` ; après le rejeu de J2 et J3, les 14 tables redeviennent identiques | idem |
| `kill -9` pendant l'écriture de `fact_sales` | état `PUBLISHING`, verrous laissés ; procédure du runbook, puis `PUBLISHED` et contenu identique | idem |
| Clés et références | 0 doublon de clé, 0 orphelin | [#15, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7205455223) |
| Totaux gold contre silver recalculé | 970 262 commandes ; `revenue_eur` 4 632 664 974,40 ; `invoiced_amount_eur` 4 414 921 584,34 ; CA `COMPLETED` 4 353 641 385,18 = total de `kpi_revenue_daily` | idem |
| Top 5 de la France recalculé en Python simple | égal à l'indicateur, `P00042` en tête à 203 874 707,20 EUR | [#15, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7207818096) |
| Moyenne mobile de la France recalculée à la main | 367 jours, 0 différence | idem |
| Silver incomplet (KAN-58) | gold refuse avant toute écriture ; les 6 tests qui le prouvent échouent avant le correctif et passent après | [#22](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7214321020) |

Exécution gold de référence avant la panne, seule sur le cluster : 40,0 s. Reconstruction
complète de gold dans une zone vide : 49,1 s
([fiche d'incident](incidents/2026-11-27-perte-datanode.md#chronologie-horodatée)). Les autres
durées gold publiées ont été prises avec d'autres applications sur le cluster.

**Limite.** Chaque exécution relit tout l'historique silver : le coût croît avec l'historique,
pas avec le jour, ce qui reste acceptable à un million de commandes par an. Un rejeu silver d'un
jour complet lancé pendant une exécution gold n'est pas détecté : gold peut lire certaines tables
avant la réécriture et d'autres après ([#22](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7217275110)).
Sur une table écrite avant #22, sans `_built_for.json`, une version produit renvoyée sur un jour
incomplet passe inaperçue jusqu'à la reconstruction suivante. Le pays d'un indicateur est le
pays actuel du client : un déménagement déplace tout son historique.

## 7. Streaming

### Décisions

Les choix viennent de mesures de la revue du Lot 0, sur Spark 3.5.3 et un flux conforme au
contrat ([ADR-0008](adr/0008-streaming.md)) :

| Mesure de la revue | Décision |
| --- | --- |
| En `append`, avec un watermark de 10 min sur 10 min de flux, aucune fenêtre écrite ; avec 5 min, 4 fenêtres sur 10 | watermark de 2 min : les retards ordinaires du contrat (10 à 90 s) passent, les très grands (plus de 5 min) sont écartés |
| En `update`, 7 082 lignes écrites pour 679 clés : chaque fenêtre est réécrite à chaque micro-batch | mode `append` : chaque fenêtre d'une minute est écrite une fois, définitive |
| Les deux premiers micro-batches passent avant que le watermark soit établi | aucun très grand retard dans les 36 premiers fichiers du jour |
| Un arrêt brutal puis une reprise sur checkpoint donnent les mêmes lignes qu'une exécution sans arrêt | écriture `foreachBatch` dans des partitions `<jour>=<d>/batch_id=<n>` remplacées |

- **Déduplication** par `dropDuplicatesWithinWatermark("event_id")` : un doublon réémis avec un
  autre `event_time` est aussi écarté, et l'état reste borné.
- **Alertes** par événement : une seule par capteur et par fenêtre d'une minute, émise dès le
  micro-batch où arrive la première valeur hors seuil, d'identifiant `alert_id` stable. Les seuils
  sont une table de configuration par métrique.
- **Déclenchement toutes les 30 s, 12 fichiers au plus par micro-batch.** Après une reprise, deux
  micro-batches de `N` fichiers couvrent `2 × N × 5 s` : un événement en retard de 5 minutes reste
  écarté tant que `N ≤ 18`.
- **Exploitation 24/7.** Un marqueur d'arrêt (`scripts/streaming/stop.sh`) arrête le job
  proprement. Le job `streaming-housekeeping`, lancé toutes les heures, archive les fichiers lus
  par les trois requêtes, purge l'archive et fusionne les partitions que Spark ne rejoue plus.

### Résultats

Exécution de référence sur `/datalake`, un fichier toutes les 5 s, journée `dev` de 180 fichiers
(`application_1795715756506_0180`) :

| Compteur | Valeur |
| --- | ---: |
| Événements lus | 11 772 |
| Lignes d'agrégats écrites (fenêtre × capteur) | 743 |
| Alertes | 10 |
| Rejets (`MALFORMED_JSON` 54, `NULL_VALUE` 54) | 108 |
| Événements écartés par le watermark | 108 |
| Doublons écartés | 324 |
| Micro-batches des requêtes d'agrégats et d'alertes | 32 |

Source : [#9, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/9#issuecomment-7205894447)
et [preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/9#issuecomment-7201974606).

**Reprise après arrêt et écriture idempotente.** Le conteneur du driver est tué pendant que la
requête des agrégats a planifié le micro-batch 30 sans le valider. Le job relancé reprend au
micro-batch 29 validé et rejoue le 30 avec les mêmes fichiers. La comparaison avec l'exécution
sans arrêt donne `IDENTICAL` : 743 agrégats, 10 alertes et 108 rejets de part et d'autre, 0 clé
en double, 0 différence de valeur
([#9, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/9#issuecomment-7201974606)).
Le cas « écrit mais non validé » est forcé à chaque exécution par le test local
`test_a_batch_replayed_after_a_crash_rewrites_its_output_instead_of_adding_to_it`. Le tour de
correction a enchaîné 13 arrêts à un instant précis dans une même zone de test, le job relancé
après chacun : pendant une écriture, entre écriture et validation, pendant le housekeeping,
`kill -9`, marqueur, SIGTERM. Le résultat final est `IDENTICAL` à la référence, sans clé en
double. Ces arrêts ont trouvé trois défauts, corrigés dans #9.

| Zone (quota de noms) | Avant : micro-batch toutes les 5 s, sans housekeeping | Après : 30 s, housekeeping horaire |
| --- | --- | --- |
| `/datalake/landing` (100 000) | plein en 5,8 jours | 720 fichiers au plus dans `incoming` |
| `/datalake/streaming` (500 000) | plein en 28 jours | environ 55 000 noms ; plein en 120 jours par les octets |
| `/spark-logs` (100 000, 15 Go) | plein en 1,8 jour | borné à environ 55 Mo stockés |

Source : [runbook streaming](runbook/streaming.md#quotas--projection-sur-24-h).

**Limite.** Le journal de la source fichier de chaque checkpoint garde une entrée par fichier
lu, sans fin : 7 Mo par jour pour les trois requêtes, d'où `/datalake/streaming` plein en
environ 120 jours. Repartir d'un nouveau checkpoint le remet à zéro, avec de nouvelles tables. Le
flux de démonstration étant fini, les fenêtres des 3 dernières minutes restent ouvertes. Un
Ctrl+C sur `submit.sh` tue la JVM du driver avec le processus Python : la ligne d'audit est
perdue, les données restent cohérentes.

## 8. MapReduce contre Spark

Le sujet demande le chiffre d'affaires par pays avec des compteurs de lignes invalides. Les deux
programmes lisent les mêmes fichiers CSV bruts du landing, joignent le pays du client (cache
distribué et jointure côté map en Hadoop Streaming, jointure diffusée en Spark) et appliquent huit
règles d'invalidité dans le même ordre ([le calcul](mapreduce-vs-spark.md#le-calcul)).

### Même résultat, prouvé

- En local, 65 tests passent les mêmes fichiers dans le vrai tube `mapper.py | sort | reducer.py`
  et dans une session Spark, y compris des octets hors contrat : BOM, fins de ligne CR LF et CR,
  caractères de contrôle.
- Sur le cluster, `compare.sh` vérifie à chaque exécution que la sortie est identique octet pour
  octet, que les compteurs sont égaux, que leur somme vaut `Map input records` et que les
  compteurs `Quality` égalent les anomalies des manifestes.

Sur `full` (10 054 794 lignes), par exemple : `BAD_QUANTITY` 50 274 = 50 000 + 137 + 137 du
manifeste, `MALFORMED_ROW` 10 054 = 10 000 + 27 + 27, et 17 422 094 258,86 EUR pour la France
dans les deux moteurs ([preuve d'égalité](mapreduce-vs-spark.md#preuve-dégalité-des-résultats)).

### Campagne de mesure

45 exécutions exclusives, 5 par variante et par volume, en alternance. `mapreduce-tuned` réduit
le battement ApplicationMaster ↔ ResourceManager à 100 ms.

| Volume | Variante | Durée médiane (s) | p contre `spark` | Mo·s médian | Cœurs·s médian |
| --- | --- | ---: | ---: | ---: | ---: |
| `ci`, 20 110 lignes | `mapreduce` | 11,4 | 0,008 | 19 581 | 19,2 |
| `ci` | `mapreduce-tuned` | 9,8 | 0,008 | 17 902 | 20,3 |
| `ci` | `spark` | 12,7 | | 43 511 | 46,3 |
| `dev`, 1,0 M lignes | `mapreduce` | 13,8 | 0,056 | 24 475 | 26,8 |
| `dev` | `mapreduce-tuned` | 10,8 | 0,008 | 20 501 | 25,1 |
| `dev` | `spark` | 15,5 | | 53 555 | 54,6 |
| `full`, 10,1 M lignes | `mapreduce` | 21,6 | 0,095 | 58 800 | 86,3 |
| `full` | `mapreduce-tuned` | 20,7 | 0,841 | 55 245 | 85,8 |
| `full` | `spark` | 20,7 | | 69 741 | 81,9 |

Source : [résultats de la campagne](mapreduce-vs-spark.md#résultats-de-la-campagne), rapport
complet dans [`mr-vs-spark-campaign.md`](perf-evidence/mr-vs-spark-campaign.md).

- Sur les petits volumes, MapReduce est plus rapide : Spark paie un démarrage fixe (driver,
  ApplicationMaster, deux exécuteurs, 7,7 s avant le premier job) que 20 000 ou 1 M de lignes ne
  compensent pas.
- Sur 10 M de lignes, aucune différence de durée n'est détectable (p = 0,095 et 0,841).
- MapReduce paie à chaque tâche : 0,94 s de CPU de JVM et 1,9 s de durée fixes par map, puis
  environ 1,7 à 2,0 fois le CPU par ligne de Spark sur `full`. Ce rapport est inféré : la part
  Python du mapper est mesurée hors du cluster.
- Avec les réglages par défaut, MapReduce est limité par l'arrivée de ses conteneurs, environ un
  par seconde. Le battement à 100 ms gagne 1,6 s sur `ci` et 3,0 s sur `dev` (p = 0,016 et 0,008).

### Complexité du code contre performance

| Moteur | Lignes de code | Ce qu'il faut écrire à la main |
| --- | ---: | --- |
| MapReduce | 159 | tri et format texte, combiner et son format, cache distribué, arithmétique entière en 10⁻⁵ EUR, relecture des lignes comme Hadoop |
| Spark | 180 | les règles ; la jointure diffusée tient en un appel, l'agrégation partielle vient de l'optimiseur |

Source : [taille et lisibilité du code](mapreduce-vs-spark.md#taille-et-lisibilité-du-code).
Le job Spark est plus long parce qu'il s'inscrit dans la plateforme : configuration, audit,
journal JSON. Les optimisations coûtent du code en MapReduce et rien en Spark
([arbitrage](mapreduce-vs-spark.md#arbitrage-complexité-contre-performance)).

**Décision.** Les traitements batch sont écrits en Spark pour le CPU par ligne et pour la
plomberie distribuée qu'ils n'ont pas à écrire, pas pour une vitesse que la mesure ne montre pas.

**Limite.** Une seule machine, des réglages par défaut, trois volumes : la campagne ne montre
aucun volume où Spark serait plus rapide. Deux cas hors contrat donnent encore des résultats
différents : un UTF-8 invalide et un U+FEFF en tête d'un split autre que le premier.

## 9. Performance

Ce chapitre résume [performance et optimisation](performance.md) (KAN-44 à KAN-47). Il reprend
les conclusions du Lot 7 sur le cache, les partitions de shuffle et les plans, dernier critère
de KAN-46.

**Protocole.** Les mesures tournent sur YARN, 2 exécuteurs de 2 cœurs, 16 partitions de shuffle,
AQE active. Avant chaque exécution, l'outil vérifie qu'aucune autre application n'est active, et
il écarte après coup une exécution chevauchée : aucune ne l'a été. Chaque série compte un tour
d'échauffement puis 5 exécutions par variante en alternance. Chaque exécution rend une empreinte
de son résultat, et l'outil sort en erreur si deux variantes ne calculent pas la même chose. Les
octets lus et de shuffle viennent de l'API du History Server
([protocole](performance.md#protocole-commun)). Données : profil `full`, 9 843 644 commandes
valides.

### Version naïve contre version optimisée

Même requête : CA `COMPLETED` par jour, pays et catégorie sur décembre 2025.

| Levier | Durée | Octets lus | Octets stockés | p (durée) |
| --- | ---: | ---: | ---: | ---: |
| Format : CSV vers Parquet | −87,9 % | −89,3 % | −73,3 % | 0,008 |
| Partitionnement par date, listage par un job Spark | +79,4 % | −91,7 % | −0,5 % | 0,008 |
| Partitionnement par date, listage par le driver | +1,4 % | −91,7 % | −0,5 % | 0,690 |
| Partitionnement par date, table déjà listée | −43,1 % | −91,7 % | −0,5 % | 0,008 |
| Compression : snappy vers zstd | −5,6 % | −24,3 % | −24,7 % | 0,841 |
| Ensemble : CSV vers Parquet partitionné zstd | −79,5 % | −99,3 % | −80,0 % | 0,008 |

Source : [naïf contre optimisé](performance.md#naïf-contre-optimisé-kan-44),
[`perf-evidence/formats/report.md`](perf-evidence/formats/report.md). Durées médianes : CSV
5,08 s, Parquet 0,61 s, Parquet partitionné zstd 1,04 s, Parquet partitionné listé par le driver
0,62 s.

Le partitionnement réduit la lecture à 31 fichiers, mais Spark liste les 396 dossiers par un job
d'une tâche par dossier au-delà de 32 chemins : c'est ce listage qui ralentit, pas la lecture.

### Déséquilibre de clé : deux corrections et la diffusion

Jointure de la construction de `fact_sales` : 9 843 644 commandes contre 1 326 versions de
produits. Dans la jointure triée, la partition chaude reçoit 31,0 Mo de shuffle contre 7,4 Mo en
médiane.

| Variante (4 tâches en parallèle) | Durée médiane (s) | p contre `baseline` | Max/médiane de la durée des tâches | Octets de shuffle lus | Déversé sur disque |
| --- | ---: | ---: | ---: | ---: | ---: |
| `baseline` : jointure triée, sans AQE ni diffusion | 2,63 | | 4,16 | 142 656 609 | 24 722 709 |
| `salted-16` : salage en 16 sous-clés | 3,17 | 0,016 | 1,63 | 191 826 948 | 0 |
| `aqe-skew-join` : seuils ajustés | 2,63 | 0,841 | 1,50 | 142 664 876 | 0 |
| `broadcast` : diffusion de la dimension | 1,37 | 0,008 | 1,01 | 450 | 0 |

Source : [déséquilibre de clé](performance.md#déséquilibre-de-clé-kan-45),
[`skew-4slots/report.md`](perf-evidence/skew-4slots/report.md). Sur 8 tâches en parallèle :
`baseline` 2,39 s, salage 2,81 s (p = 0,016), AQE 2,16 s (p = 0,056, non retenu), diffusion
1,07 s (p = 0,008), [`skew-8slots/report.md`](perf-evidence/skew-8slots/report.md).

Le diagnostic est le ratio max/médiane de la durée des tâches de l'étape de jointure : 4,16. Le
déséquilibre est réel mais coûte peu de temps ici : avec 4 cœurs, une clé qui porte 20 % des
lignes ne dépasse pas la part d'un cœur (25 %). Les deux corrections ramènent le ratio à 1,5
pour AQE et entre 1,6 et 1,8 pour le salage. AQE ne ralentit rien, sans gain de durée retenu.
Le salage ajoute 34 % de shuffle et ralentit de 18 à 20 %. Avec les seuils par défaut
(facteur 5, 256 Mo), AQE ne détecte pas la partition chaude. La diffusion supprime le shuffle,
et le plan de gold la fait déjà avec les réglages de la plateforme.

### Cache justifié

Le DataFrame réutilisé est le fait des ventes au prix applicable, relu par plusieurs indicateurs.

| Lignée du fait | Indicateurs | Sans cache (s) | Avec cache (s) | Écart | p |
| --- | ---: | ---: | ---: | ---: | ---: |
| Jointure diffusée (celle de gold) | 3 | 4,20 | 6,39 | +52,0 % | 0,008 |
| Jointure diffusée | 6 | 8,78 | 8,81 | +0,4 % | 0,421 |
| Jointure triée | 3 | 9,32 | 6,89 | −26,0 % | 0,008 |

Source : [cache](performance.md#cache), [`cache/report.md`](perf-evidence/cache/report.md).

Remplir le cache coûte trois fois le temps d'exécuteur d'un recalcul bon marché. Le cache ne
paie donc que sur une lignée coûteuse (shuffle, lecture CSV) ou réutilisée plus de 6 fois.
Gold persiste `fact_sales` puis le relit au moins 10 fois (comptages, invariants, 6 indicateurs
sur 9, écriture) : au-delà du point où le cache paie sur cette mesure.

### Nombre de partitions de shuffle

Agrégation large : 3 982 974 groupes client × produit, sur 4 cœurs.

| Partitions | 4 | 8 | 16 | 64 | 200 | 200 avec regroupement AQE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Durée médiane (s) | 6,30 | 6,24 | 6,29 | 6,35 | 6,85 | 6,47 |
| p contre 16 | 0,690 | 1,000 | | 0,548 | 0,016 | 0,016 |

Source : [partitions de shuffle](performance.md#nombre-de-partitions-de-shuffle),
[`partitions/report.md`](perf-evidence/partitions/report.md).

De 4 à 64 partitions, aucune différence de durée n'est détectable. À 200, la valeur par défaut de
Spark, la durée augmente de 9 %. L'étape de réduction dessine un U dont le creux est à 16 : la
valeur de la plateforme reste 16.

### Trois plans commentés

| Plan | Ce qu'il montre | Conséquence |
| --- | --- | --- |
| [Lecture avec élagage](perf-evidence/explain/pruning.txt) | `PartitionFilters` sur `order_date` : 31 dossiers lus sur 367 ; `ReadSchema` réduit à `revenue_eur` | le filtre sur une colonne de partition évite de lire les fichiers, pas de les lister |
| [Filtre poussé dans Parquet](perf-evidence/explain/pushdown.txt) | `PushedFilters` et 3 colonnes lues ; le `Filter` réapplique la condition ligne à ligne | le gain dépend du tri des données, car un groupe de lignes n'est sauté que si ses statistiques l'excluent |
| [Jointure de `fact_sales`](perf-evidence/explain/join-broadcast.txt), [sans diffusion](perf-evidence/explain/join-sort-merge.txt) | `BroadcastHashJoin` avec la condition d'intervalle ; sans diffusion, deux `Exchange hashpartitioning(..., 16)` et un `SortMergeJoin` | sans diffusion, toutes les lignes de `P00042` vont dans une partition, celle qui déverse dans KAN-45 |

Commentaire complet : [trois plans commentés](performance.md#trois-plans-commentés).

### Réplication 3 contre erasure coding

Même fichier (commandes J1 du profil `full`), copié dans `/benchmarks/rep3` et `/benchmarks/ec`
(`XOR-2-1-1024k`), sommes CRC composites égales.

| Mode | Octets logiques | Octets physiques | Surcoût | Pertes tolérées | Lecture complète (s) |
| --- | ---: | ---: | ---: | --- | ---: |
| Réplication 3 | 713 401 398 | 2 140 204 194 | 200,0 % | 2 DataNodes par bloc | 1,77 |
| XOR-2-1-1024k | 713 401 398 | 1 070 286 956 | 50,0 % | 1 DataNode par groupe | 1,69 |

Source : [réplication contre erasure coding](performance.md#réplication-3-contre-erasure-coding-kan-47),
[`storage/report.md`](perf-evidence/storage/report.md), sorties `hdfs fsck` dans
[`perf-evidence/storage/`](perf-evidence/storage/). Aucune différence de lecture n'est
détectable (p = 0,095), ni de copie (p = 0,310).

- Le volume physique en erasure coding se vérifie à l'octet : 681 cellules de données, une cellule
  de parité par bande, 713 401 398 + 356 885 558 = 1 070 286 956 octets.
- Un fichier plus petit qu'une cellule de 1 Mio paie 100 % de surcoût : le fichier produits de
  79 966 octets en occupe 159 932.
- `RS-6-3` demande 9 DataNodes et `RS-3-2` 5 : une écriture `RS-6-3` sur 3 DataNodes est refusée.
- Coût : la réplication 3 tolère une panne de plus par bloc pour un surcoût quatre fois plus
  grand. L'erasure coding n'a ni `hflush` ni `hsync` et perd la localité des données.

### Leviers classés par impact

Classement par effet mesuré sur la durée, puis leviers de stockage. Chaque ligne cite sa mesure
ci-dessus.

| Rang | Levier | Effet mesuré | p | Coût | Limite | Décision |
| ---: | --- | --- | ---: | --- | --- | --- |
| 1 | Parquet au lieu de CSV | −87,9 % de durée, −89,3 % d'octets lus | 0,008 | aucun à la lecture ; bronze écrit déjà en Parquet | mesuré sur une requête qui lit 7 colonnes sur 10 | Parquet partout |
| 2 | Diffusion de la dimension produit | −48 % (4 cœurs), −55 % (8 cœurs), shuffle supprimé | 0,008 | une copie de la dimension par exécuteur | seulement sous le seuil de 10 Mo ; 1 326 versions aujourd'hui | plan actuel de gold |
| 3 | Cache d'une lignée coûteuse et réutilisée | −26,0 % avec un shuffle dans la lignée ; +52,0 % sur une lignée bon marché à 3 indicateurs | 0,008 | 65 à 110 Mo en mémoire | perd sur une lignée bon marché peu réutilisée | `fact_sales` persisté dans gold |
| 4 | Élagage de partitions par date | −43,1 % une fois la table listée ; +79,4 % si Spark liste par un job | 0,008 | listage de 396 dossiers, fichiers de 0,6 Mo | ne paie que si le listage reste bon marché | partitionner par date, lister côté driver |
| 5 | 16 partitions de shuffle au lieu de 200 | 200 : +9 % ; 4 à 64 : pas de différence détectable | 0,016 | aucun | mesuré sur 4 cœurs et 4 M de groupes | 16 |
| 6 | AQE skew join, seuils ajustés | ratio 4,16 vers 1,50 ; durée non retenue (0 % et −9,9 %) | 0,841 et 0,056 | seuils à adapter au volume | inactif avec les seuils par défaut | filet si la diffusion devient impossible |
| 7 | Salage en 16 sous-clés | ratio 4,16 vers 1,63 ; +20 % (4 cœurs), +18 % (8 cœurs) | 0,016 | +34 % de shuffle, code à maintenir | plus lent dans les trois séries | écarté |
| Stockage | Erasure coding `XOR-2-1` | −50 % d'octets physiques, lecture sans différence détectable | 0,095 | 1 panne tolérée au lieu de 2, 100 % sous 1 Mio | aucune réparation possible sans 4ᵉ nœud | gros fichiers froids |
| Stockage | zstd au lieu de snappy | −24,7 % stockés, durée sans différence détectable | 0,841 | | | tables froides |
| Stockage | `_record_hash` en binaire | −47,4 % sur la colonne | | changement de schéma bronze | non appliqué | constat transmis |

### Reproductibilité

`scripts/perf/bench.sh <expérience>` et `scripts/perf/storage.sh` refont chaque série sur le
cluster. `bench.sh report` et `storage.sh --report` refont chaque rapport à partir des preuves
versionnées, sans cluster. Le test `tests/perf/test_evidence.py` vérifie que chaque p de
`performance.md` est celui que l'outil calcule à partir des `runs.json` versionnés
([reproduire](performance.md#reproduire)).

**Limite.** Une seule machine : les trois DataNodes partagent le cache de pages, et les lectures
mesurent le chemin logiciel, pas les disques. Les séries ont un seul tour d'échauffement :
l'exécution 1 reste la plus lente dans 13 des 30 variantes des six séries. Recalculées sans elle,
les comparaisons ne changent qu'une conclusion, le salage à 8 tâches (p = 0,057). La version
naïve lit le CSV avec un schéma explicite, et non avec `inferSchema` comme le prévoyait
l'architecture : la mesure isole le format. La série à 8 tâches change aussi la mémoire par tâche.

## 10. Industrialisation et qualité

### Code et configuration

- **Modules.** Les transformations sont des fonctions pures `DataFrame -> DataFrame` dans
  `src/gtdp/<couche>/`. Les jobs sont des coquilles minces enregistrées par `@job`, lancées par
  `run.py` qui ouvre l'exécution auditée ([organisation du code](architecture.md#organisation-du-code)).
- **Règles en tables.** Règles de qualité, dispositions, seuils de capteurs et codes de rejet
  sont des registres, pas des suites de conditions.
- **Aucune UDF Python.** Contrôles, parsing des logs (`regexp_extract`) et pseudonymisation
  (`sha2`) utilisent des fonctions natives. HMAC-SHA256, absent des fonctions natives, a été
  écarté pour cette raison ([ADR-0004](adr/0004-pseudonymisation.md)).
- **Configuration TOML** par environnement, surchargeable par variable `GTDP_<SECTION>__<CLE>` et
  par `--set`. Une clé inconnue ou mal typée arrête le job avant Spark, code 2. Le sel n'existe
  que dans une variable d'environnement ([ADR-0003](adr/0003-configuration-toml.md)).
- **Journalisation** en lignes JSON, configuration effective journalisée au démarrage, sel masqué.
- **Table d'audit** `ops/audit` : une ligne par exécution, statut `SUCCESS`, `FAILED` ou
  `STOPPED`, compteurs, durée, application YARN, métriques détaillées
  ([dictionnaire ops](data-dictionary/ops.md#opsaudit), [requêtes](runbook/README.md#interroger-laudit)).

### Tests et CI

| Mesure | Valeur | Source |
| --- | --- | --- |
| Tests pytest, `scripts/test.sh` sur `main` à `dd18d20` | 868, tous verts | commentaire de preuves de la PR de ce rapport |
| Couverture du paquet `gtdp`, lignes et branches | 91,2 % | idem |
| Couverture de `io.py` (écritures, verrous, lectures) | 96,7 % | idem |
| Modules les moins couverts | les rapports en ligne de commande, par exemple `gold/report.py` à 69,5 % : leur `main()`, non testé, finit par `spark.stop()`, qui arrêterait la session Spark partagée des tests | idem, et [#16](https://github.com/BAYRYO/globaltech-data-platform/pull/16) |

Les tests vérifient un comportement observable : comptes égaux aux manifestes du générateur,
rejeu à contenu identique, refus avant écriture, arrêt tué à chaque étape d'un échange. Les
tests écrits par les relecteurs restent dans la suite. Les tests de `tests/docs/` vérifient que
le dictionnaire de données documente chaque colonne du code avec son type, et
`tests/perf/test_evidence.py` vérifie les p de `performance.md`.

La CI GitHub Actions a deux workflows. `ci.yml` lance ruff, shellcheck et pytest avec la
couverture. `platform.yml` lance hadolint, valide le compose et, quand la plateforme change,
démarre un cluster neuf pour le smoke test. La PR de démonstration #12, fermée sans fusion, a montré qu'un test en échec fait
échouer la CI.

### Revues adverses

Aucune relecture humaine : chaque livrable passe une revue par deux relecteurs indépendants, sur
deux modèles différents, chacun dans un rôle différent de celui de l'auteur. Un constat n'est
retenu que s'il est prouvé par une exécution, et le pilote relance chaque preuve
([pilotage](project-management.md#revues)).

| Revue | Constats | Source |
| --- | --- | --- |
| Lot 0, cadrage | relecteur A : 2 bloquants, 5 majeurs, 6 mineurs ; relecteur B : 1 bloquant, 5 majeurs, 7 mineurs ; aucun écarté | [revue du Lot 0](reviews/2026-11-26-revue-adverse-lot0.md) |
| Silver (#11) | 12 constats, de A1 à A7 et de B1 à B5, chacun relié à son commit ; 30 tests de non-régression échouaient sur l'ancienne tête | [#11](https://github.com/BAYRYO/globaltech-data-platform/pull/11#issuecomment-7204818441) |
| Gold (#15) | 1 bloquant (le pays du client figé dans le fait), 3 majeurs, 10 mineurs ; 17 tests des relecteurs en échec, 10 de A et 7 de B | [#15](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7206133610) |
| Verrous (#18) | aucun bloquant, 2 majeurs, 6 mineurs ; 9 tests du relecteur en échec, 3 mutations survivaient aux tests | [#18](https://github.com/BAYRYO/globaltech-data-platform/pull/18#issuecomment-7208384827) |

### Suivi

Une epic Jira par lot, une story par livrable vérifiable, avec critères d'acceptation, rôle et
jalon. Le tableau Kanban suit le cycle d'une PR : branche ouverte, PR ouverte avec CI verte,
PR fusionnée. Une story est terminée quand sa PR est fusionnée, sa CI verte, sa revue passée et
ses critères démontrés sur YARN ([pilotage](project-management.md)). Avant la PR de ce rapport,
le dépôt compte 22 PR fusionnées, une PR fermée sans fusion (#12) et une PR ouverte (#24).

**Limite.** La CI mesure la couverture sans seuil minimal (`fail_under` absent de
`pyproject.toml`). Les preuves sur le cluster sont des exécutions publiées dans les PR, pas des
tests automatiques. Les relecteurs sont des modèles : la règle « constat prouvé par une
exécution » limite les faux constats, pas les angles morts communs aux modèles.

## 11. Résilience

Deux exercices ont arrêté `datanode1` (`docker stop`) pendant que le job gold écrivait sur YARN.
Le premier a mesuré la détection, la re-réplication et le retour au nominal. Le second prouve
qu'aucune donnée n'est perdue pendant l'écriture. La [fiche d'incident](incidents/2026-11-27-perte-datanode.md)
suit le format du sujet : symptôme, hypothèses, preuves, cause, correction, validation,
prévention. La procédure d'exploitation est dans le [runbook HDFS](runbook/hdfs.md#perte-dun-datanode).

| Mesure | Valeur |
| --- | --- |
| Déclaration de mort du DataNode | entre 86 et 91 s après le dernier battement de cœur, pour 90 s attendues |
| Retrait de ses répliques par le NameNode | 94 s après le dernier battement de cœur |
| Blocs touchés | 5 568 blocs répliqués passés à 2 répliques, 3 groupes en erasure coding à 2 unités sur 3 |
| Blocs perdus | 0 manquant, 0 corrompu ; `fsck /` `HEALTHY` pendant les deux pannes |
| Lecture dégradée du fichier en erasure coding (713 401 398 octets) | 3,0 s contre 2,6 s pour la copie répliquée ; MD5 identique |
| Re-réplication vers le DataNode de secours | 5 571 répliques et 3 unités en 69 min 50 s, soit 1,33 bloc par seconde |
| Exposition | blocs répliqués à 2 répliques pendant 73 min 08 s ; groupes en erasure coding sans tolérance pendant 49 min 04 s |
| Rejeu gold pendant la panne (1ᵉʳ exercice) | 178,2 s contre 40,0 s, `SUCCESS` |
| Reconstruction complète de gold pendant la panne (2ᵉ exercice) | 159,3 s contre 49,1 s, `SUCCESS` ; `fact_sales` écrite en 106,7 s au lieu de 4,4 s |
| Écriture pendant la panne | 160 des 367 fichiers de `fact_sales` écrits sur les 2 DataNodes restants ; empreinte identique à `/datalake/gold` sur 14 tables et 367 partitions |
| Retour au nominal | 3 DataNodes vivants, 0 bloc sous-répliqué, manquant ou corrompu |

Source : [fiche d'incident](incidents/2026-11-27-perte-datanode.md), preuves dans
[`incidents/2026-11-27-perte-datanode/`](incidents/2026-11-27-perte-datanode/).

**Pourquoi le ralentissement.** Avec 3 DataNodes pour une réplication 3, chaque nouveau bloc
reçoit un pipeline qui contient le nœud arrêté tant que le NameNode le croit vivant. Chaque
écriture échoue une fois, puis redemande son bloc sans lui. Un échec dure 60 s ou 65 s tant que
l'adresse du nœud se résout encore, environ 3 s ensuite. Au premier exercice, le driver a attendu
4 fois, 136,4 s des 138,2 s d'écart.

**Ce qui limite la re-réplication.** Chaque DataNode source reçoit 2 copies à la fois
(`dfs.namenode.replication.max-streams`), 4 au plus au total, à chaque battement de cœur de 3 s :
1,33 bloc par seconde. Les groupes en erasure coding, les plus exposés, sont passés en dernier,
après 46 min d'attente, à cause du signet de parcours des files de réparation
([cause](incidents/2026-11-27-perte-datanode.md#cause)).

**Hypothèses réfutées par la mesure.** Le rejeu ne perd ni ne duplique de lignes. Le réglage
`dfs.namenode.avoid.write.stale.datanode` n'évite pas les attentes avec 3 DataNodes : 8 premières
allocations sur 8 nomment le nœud arrêté, réglage actif ou non ; avec 4 DataNodes, 0 sur 8
([hypothèses et preuves](incidents/2026-11-27-perte-datanode.md#hypothèses-et-preuves)).

| Action préventive | Effet attendu |
| --- | --- |
| Garder 4 DataNodes, ou le secours `datanode4` prêt | les répliques perdues ont une cible dès la détection ; 640 Mio de mémoire en plus |
| Alerter sur les métriques JMX du NameNode (nœuds morts, blocs manquants, redondance réduite) | détection en moins de 2 min sans échantillonneur manuel |
| `max-streams` 8, multiplicateur 4, limite dure 8 | 4,0 blocs par seconde mesurés dans un HDFS jetable, contre 1,33 ; les 5 571 répliques en 23 min environ (calculé) |
| `dfs.namenode.redundancy.queue.restart.iterations` à 20 | groupes en erasure coding réparés dans la minute après l'arrivée du secours (déduit, non mesuré) |
| Réserver `XOR-2-1` aux données froides et reproductibles | pas de donnée de production sans tolérance pendant une panne |

Liste complète et rôles propriétaires : [actions préventives](incidents/2026-11-27-perte-datanode.md#actions-préventives).

**Limite.** Deux exercices, un seul DataNode arrêté, Docker sur une machine. Au premier exercice,
un autre conteneur a repris l'adresse IP du nœud arrêté pendant 3 s, ce qui a raccourci certains
échecs : le second exercice, sans réattribution, est la référence la plus proche de machines
physiques. Le cluster n'a aucune alerte : la détection a été faite par l'échantillonneur du
scénario.

## 12. Limites et pistes d'amélioration

Classées par risque pour les données, puis pour l'exploitation. Le coût est celui de la piste.

| # | Limite | Conséquence documentée | Piste | Coût | Source |
| ---: | --- | --- | --- | --- | --- |
| 1 | 3 DataNodes pour une réplication 3 | après une panne, aucune cible de copie : 73 min 08 s à 2 répliques, et une seconde panne parmi les deux autres nœuds rend illisible un fichier `XOR-2-1` | 4ᵉ DataNode en service | 640 Mio de mémoire | [incident](incidents/2026-11-27-perte-datanode.md#actions-préventives) |
| 2 | Un seul NameNode | sa perte arrête HDFS ; un redémarrage planifié de l'exercice a laissé HDFS en safe mode 7 s | NameNode de secours et journal partagé | au moins deux JVM de plus, non chiffrées dans ce projet | [architecture](architecture.md#composants), [incident](incidents/2026-11-27-perte-datanode.md#chronologie-horodatée) |
| 3 | Aucune alerte sur HDFS | la panne a été détectée par l'échantillonneur du scénario | alertes JMX du NameNode | un outil de supervision à ajouter | [incident](incidents/2026-11-27-perte-datanode.md#détection-symptôme) |
| 4 | Re-réplication bridée à 1,33 bloc par seconde, groupes en erasure coding réparés en dernier | 69 min 50 s de re-réplication, 46 min d'attente pour les groupes les plus exposés | `max-streams`, multiplicateur, limite dure, `restart.iterations` | changement à chaud de la configuration du NameNode | [incident](incidents/2026-11-27-perte-datanode.md#actions-préventives) |
| 5 | Fenêtre résiduelle du verrou de table | si le ResourceManager reste injoignable pendant qu'une exécution vivante garde un verrou plus de 6 h, deux exécutions écrivent la même table | format de table transactionnel | hors programme du module (Delta Lake, Iceberg) | [ADR-0002](adr/0002-parquet-idempotence.md#garantie) |
| 6 | Rejeu silver pendant une exécution gold (bord de KAN-58) | gold peut lire certaines tables avant la réécriture d'un jour et d'autres après | revérifier les jours complets avant `PUBLISHING` | une relecture des marqueurs avant la publication | [#22](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7217275110) |
| 7 | `kill -9` entre suppression et renommage d'une partition | partition absente jusqu'au rejeu du jour | rejouer le jour (procédure documentée) | aucun | [ADR-0002](adr/0002-parquet-idempotence.md#conséquences) |
| 8 | `XOR-2-1` seule politique possible ; `RS-6-3` refusée, `RS-3-2` demande 5 DataNodes | 1 panne tolérée par groupe, aucune réparation sans nœud libre | plus de DataNodes, puis `RS-3-2` | environ 2 Gio retirés aux exécuteurs pour 2 DataNodes de plus | [ADR-0006](adr/0006-topologie-hdfs.md) |
| 9 | Gold recalcule tout l'historique | coût en O(historique) à chaque exécution | recalcul incrémental avec le suivi des dépendances SCD | complexité, et un oubli de dépendance fait diverger gold de silver sans signal | [runbook gold](runbook/gold.md#ce-que-fait-une-exécution) |
| 10 | Journal de source des checkpoints du streaming | `/datalake/streaming` plein en environ 120 jours | nouveau checkpoint périodique avec nouvelles tables | une procédure de bascule | [ADR-0008](adr/0008-streaming.md#conséquences) |
| 11 | `_record_hash` en hexadécimal | 73,4 % des octets de `bronze/orders` | stocker les 32 octets en binaire | changement de schéma bronze et silver | [performance](performance.md#constat--_record_hash-en-bronze) |
| 12 | Risque résiduel de pseudonymisation | `user_agent`, requête de `path`, préfixe IP, quasi-identifiants | tronquer l'agent utilisateur et la requête | perte d'analyse sur ces champs | [ADR-0004](adr/0004-pseudonymisation.md) |
| 13 | Tables écrites avant #22 sans `_built_for.json` | une version produit renvoyée sur un jour incomplet passe jusqu'à la reconstruction | reconstruire silver une fois | un rejeu de J1 à J3 | [#22](https://github.com/BAYRYO/globaltech-data-platform/pull/22) |
| 14 | Mesures sur une machine, un tour d'échauffement, 5 exécutions | effets de quelques pour cent non détectables, cache de pages partagé | 2 tours d'échauffement (défaut actuel de l'outil), cluster multi-machines | temps de cluster exclusif | [performance](performance.md#protocole-commun) |
| 15 | Couverture sans seuil en CI | une baisse de couverture ne bloque pas une PR | `fail_under` dans `pyproject.toml` | aucun | `pyproject.toml` |

## 13. Annexes

### A. Décisions d'architecture

| ADR | Décision en une ligne |
| --- | --- |
| [0001](adr/0001-image-hadoop-spark.md) | Une seule image Hadoop 3.3.6 et Spark 3.5.3 construite par le projet, un rôle par service. |
| [0002](adr/0002-parquet-idempotence.md) | Partitions nommées, écrites puis échangées sous un verrou jugé par l'état YARN ; audit en ajout. |
| [0003](adr/0003-configuration-toml.md) | Configuration TOML par environnement, surchargeable par variable et `--set`, sel hors fichiers. |
| [0004](adr/0004-pseudonymisation.md) | SHA-256 salé en fonctions natives, sel passé comme donnée et contrôlé par son empreinte. |
| [0005](adr/0005-silver-incremental.md) | Silver incrémental par jour d'ingestion, publié par jour complet, jours traités dans l'ordre. |
| [0006](adr/0006-topologie-hdfs.md) | 3 DataNodes, réplication 3, `XOR-2-1` comparé, détection d'un nœud mort en 90 s. |
| [0007](adr/0007-evolution-schema.md) | Schéma brut par en-tête de fichier en bronze, schéma canonique en silver, `discount_pct` à 0 par défaut. |
| [0008](adr/0008-streaming.md) | Watermark de 2 min, fenêtres de 1 min en `append`, une alerte par capteur et fenêtre, écriture par jour et `batch_id`, housekeeping horaire. |

### B. Grille d'évaluation : où trouver chaque point

| Critère du sujet (niveau Excellent) | Section |
| --- | --- |
| Erasure coding comparé à la réplication avec chiffres, sortie `fsck` analysée | [9](#réplication-3-contre-erasure-coding), [11](#11-résilience) |
| Relance par un tiers en moins de 10 min | [3](#relance-par-un-tiers) |
| SCD2 correct (prix à la date de commande), changement de schéma, pseudonymisation justifiée | [5](#5-couche-silver), [6](#6-couche-gold) |
| Propres + rejets + doublons = total, rejeu sans doublon | [4](#4-ingestion-et-couche-bronze), [5](#réconciliation-de-j1-à-j3) |
| Deux corrections du skew comparées, partitions justifiées par la mesure, trois `explain` | [9](#déséquilibre-de-clé--deux-corrections-et-la-diffusion) |
| Écriture idempotente prouvée, compromis complexité contre performance | [7](#7-streaming), [8](#complexité-du-code-contre-performance) |
| Leviers classés par impact, limites et coûts, résultats reproductibles | [9](#leviers-classés-par-impact) |
| Revues visibles, journalisation et audit exploitables, tests des règles métier | [10](#10-industrialisation-et-qualité) |
| Documentation utilisable par un tiers, décisions argumentées | [annexe A](#a-décisions-darchitecture), [runbooks](runbook/README.md), [dictionnaire](data-dictionary/README.md) |
| Recul critique sur les limites | [12](#12-limites-et-pistes-damélioration) |

### C. Glossaire

| Terme | Sens dans ce projet |
| --- | --- |
| Bronze, silver, gold | couches du lake : brut sans perte, fiable et historisé, prêt pour l'analyse |
| Dépôt J1, J2, J3 | fichiers d'un jour d'ingestion déposés dans `landing`, avec leur manifeste |
| Manifeste | `_MANIFEST.json` du générateur : lignes, SHA-256 et anomalies injectées par fichier |
| Jour complet | jour dont les quatre sources silver sont publiées et vérifiées (`_SUCCESS`) |
| Disposition | sort d'une ligne bronze en silver : propre, rejet, doublon exact, doublon de clé, doublon inter-jours |
| SCD type 1, type 2 | dimension qui garde la dernière valeur (clients), ou chaque version avec ses dates de validité (produits) |
| Prix applicable | prix de la version produit valide à l'heure de la commande |
| `content_sum` | somme des 60 premiers bits du SHA-256 de chaque ligne d'une table silver ou gold : empreinte indépendante de l'ordre des lignes |
| `hash_sum` | somme de contrôle des `_record_hash` d'une partition bronze, indépendante de l'ordre des lignes |
| Écrire puis échanger | écriture complète dans un répertoire de transit, puis remplacement des partitions par renommage |
| Watermark | retard maximal accepté sur `event_time` avant qu'une fenêtre soit close |
| Micro-batch | lot de fichiers traité par une requête Structured Streaming à chaque déclenchement |
| Skew | déséquilibre de clé : une clé porte beaucoup plus de lignes que les autres |
| Diffusion | envoi d'une petite table à chaque exécuteur, ce qui supprime le shuffle de la jointure |
| Salage | ajout d'une sous-clé aléatoire pour répartir une clé chaude sur plusieurs partitions |
| `XOR-2-1` | erasure coding à 2 blocs de données et 1 bloc de parité par groupe |
| p | p bilatéral d'un test exact de Mann-Whitney ; différence retenue si p < 0,05 |

### D. Reproduire les chiffres

Les commandes se lancent depuis la racine du dépôt, cluster démarré
([guide de la plateforme](platform.md)). `GTDP_PSEUDO_SALT` est exigé par silver et par
`--env cluster`.

| Chiffres | Commande | Preuve versionnée ou publiée |
| --- | --- | --- |
| Démarrage du cluster | `scripts/cluster/up.sh && scripts/cluster/init-hdfs.sh && scripts/cluster/smoke/run.sh` | [guide](platform.md#démarrage-en-moins-de-10-minutes) |
| Bronze J1 à J3 et rejeu | `scripts/cluster/submit.sh bronze --date 2026-01-0N`, puis `scripts/cluster/submit.sh src/gtdp/bronze/report.py` | [#7](https://github.com/BAYRYO/globaltech-data-platform/pull/7#issuecomment-7202646257) |
| Réconciliation silver | `scripts/cluster/submit.sh silver --date 2026-01-0N`, puis `scripts/cluster/submit.sh src/gtdp/silver/report.py` | [runbook silver](runbook/silver.md#vérifier-silver) |
| Gold, rejeu et contrôles | `scripts/cluster/submit.sh gold --date 2026-01-0N`, puis `scripts/cluster/submit.sh src/gtdp/gold/report.py` | [runbook gold](runbook/gold.md#vérifier-gold) |
| Streaming et reprise | `scripts/streaming/demo.sh --interval 5 --housekeeping`, puis la variante `--crash-after N` et `scripts/streaming/report.py` | [test de reprise](runbook/streaming.md#test-de-reprise) |
| MapReduce contre Spark | `scripts/mapreduce/compare.sh --date 2026-01-03 --runs 5 --volumes ...` | [campagne](mapreduce-vs-spark.md#campagne-de-mesure), [rapport](perf-evidence/mr-vs-spark-campaign.md) |
| Performance | `scripts/perf/bench.sh formats`, `skew`, `cache`, `partitions`, `explain` ; `scripts/perf/storage.sh` | [reproduire](performance.md#reproduire), [`perf-evidence/`](perf-evidence/) |
| Rapports de performance sans cluster | `scripts/perf/bench.sh report --out docs/perf-evidence/<série>` | `runs.json` de chaque série |
| Perte d'un DataNode | `scripts/resilience/datanode-failure.sh --victim datanode1 --date 2026-01-03`, `scripts/resilience/gold-write-under-failure.sh --victim datanode1 --date 2026-01-03` | [rejouer la panne](runbook/hdfs.md#rejouer-la-panne) |
| Tests et couverture | `scripts/test.sh --cov --cov-config=pyproject.toml` | sortie de la CI de chaque PR |
| Nombre de PR | `gh pr list --state all --limit 50` | GitHub |
