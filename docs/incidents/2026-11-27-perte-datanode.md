# Incident du 27 novembre 2026 : perte d'un DataNode pendant l'écriture de gold

| Champ | Valeur |
| --- | --- |
| Type | exercice planifié (pannes provoquées), format sans recherche de coupable |
| Date | 27 novembre 2026 UTC. Premier exercice de 07:44:17 à 09:47:51 (2 h 03 min 34 s), second de 11:44:00 à 11:49:05, retour au nominal final à 12:05:30 |
| Composant | HDFS : DataNode `datanode1` (conteneur `gtdp-datanode1-1`) |
| Gravité | mineure : aucune donnée perdue, aucun job en échec. Le rejeu gold est passé de 40 s à 178 s, la reconstruction complète de gold de 49 s à 159 s |
| Statut | résolu. À 12:05:53, 3 DataNodes vivants, aucun mort, aucun bloc sous-répliqué, manquant ou corrompu |
| Rédaction | rôle exploitation plateforme (Lot 8, KAN-52) |
| Preuves | dossier [`2026-11-27-perte-datanode/`](2026-11-27-perte-datanode/) pour le premier exercice, sous-dossier [`ecriture-sous-panne/`](2026-11-27-perte-datanode/ecriture-sous-panne/) pour le second ; scénarios `scripts/resilience/datanode-failure.sh` et `scripts/resilience/gold-write-under-failure.sh` |

## Résumé

Deux exercices ont arrêté `datanode1` (`docker stop`, signal SIGTERM) pendant que le job gold du
2026-01-03 écrivait sur YARN.

**Le second exercice prouve qu'aucune donnée n'est perdue pendant l'écriture.** Le job a
reconstruit gold en entier dans une zone vide, `/user/etl/gold-write-under-failure`, hors de
`/datalake`. `datanode1` s'est arrêté à 11:44:00, 3 s après le premier bloc de `fact_sales`. Le
job a réussi en 159 s, contre 49 s pour la même reconstruction sans panne. Il a écrit les
14 tables : 1 128 995 lignes, dont les 970 262 lignes et les 367 partitions de `fact_sales`.
Selon le journal du NameNode, 160 des 367 fichiers de `fact_sales` ont été écrits sur les
2 DataNodes restants pendant que `datanode1` était arrêté. L'empreinte de cette gold est
identique à celle de `/datalake/gold`, table par table et partition par partition. `fsck` n'a
trouvé aucun bloc manquant ni corrompu.

**Le premier exercice a mesuré la détection, la re-réplication et le retour au nominal.** Son
rejeu gold, interrompu par l'arrêt de 07:44:17, n'a réécrit que 13 tables. `fact_sales`, 86 % des
lignes de gold, n'avait pas changé : le job l'a relue sans l'écrire. Le NameNode a déclaré
`datanode1` mort 90 s après son dernier battement de cœur, puis a retiré ses répliques à 07:45:49.
Les 5 568 blocs répliqués sont passés à 2 répliques sur 3, les 3 groupes en erasure coding de
`/benchmarks/ec/orders_20260101.csv` à 2 unités sur 3. Aucun bloc n'a manqué. La lecture du
fichier en erasure coding a reconstruit les données. Le DataNode de secours `datanode4` a reçu
5 571 répliques et 3 unités d'erasure coding en 69 min 50 s, au rythme de 1,33 bloc par seconde.
Les groupes en erasure coding, les plus exposés, sont passés en dernier, après 46 min d'attente.
`datanode1` est ensuite revenu, puis `datanode4` a été décommissionné, arrêté et supprimé avec son
volume. Un redémarrage du NameNode à 12:05:19 a effacé `datanode4` de la liste des DataNodes
morts.

**Le ralentissement vient des écritures ouvertes avant que le NameNode déclare le nœud mort.**
Avec 3 DataNodes et une réplication 3, chaque nouveau bloc reçoit un pipeline qui contient
`datanode1`. Le client échoue à joindre ce DataNode, puis redemande le bloc sans lui. Chaque
échec coûte de quelques millisecondes à 65 s selon la réponse du réseau pour l'adresse du
conteneur arrêté. Au premier exercice, le driver a attendu 4 fois, 136,4 s en tout.

## Chronologie horodatée

Toutes les heures sont en UTC. Les horloges de l'hôte et des conteneurs concordent à moins
d'une seconde (mesure de 07:32:46). « Échantillon » renvoie à `timeline.csv` (un échantillon
toutes les 5 s), « événement » à `events.csv`, écrit par le scénario. Les heures au millième de
seconde viennent des journaux du NameNode et des DataNodes.

### Préparation, hors incident

| Heure | Fait | Source |
| --- | --- | --- |
| 07:40:45 | Aucune application YARN en cours, `hdfs fsck /` `HEALTHY`, 0 bloc sous-répliqué | commande manuelle |
| 07:40:50 | `hdfs-site.xml` (avec `dfs.hosts.exclude`) et `dfs.exclude` vide copiés dans `gtdp-namenode-1`, identiques au dépôt (SHA-256) | `docker cp`, `sha256sum` |
| 07:41:00 | Premier redémarrage du NameNode (`docker restart gtdp-namenode-1`) | commande manuelle |
| 07:41:04 | Les 3 DataNodes se réenregistrent du premier coup | journal du NameNode |
| 07:41:11 | `Safe mode is OFF`, sorti après 7 s ; `fsck /` `HEALTHY`, 3 DataNodes vivants, `dfs.hosts.exclude` chargé (servlet `/conf`) | journal, commandes |
| 07:42:06 | Début du scénario : fichier d'exclusion vidé, journal `BlockStateChange` du NameNode en DEBUG, échantillonneur démarré | événement `start` |
| 07:42:31 | Exécution gold de référence `application_1795715756506_0442` : `SUCCESS` en 40,0 s | audit |
| 07:43:13 | Empreinte gold de référence (`fingerprint-before.json`) | événement |

### Premier exercice : rejeu gold, re-réplication, décommission

| Heure | Fait | Source |
| --- | --- | --- |
| 07:43:37 | Rejeu gold du 2026-01-03 soumis : `application_1795715756506_0444`, exécution `20261127T074347Z-6938fb03` | événement, journal du driver |
| 07:44:15 | Dernier battement de cœur reçu de `datanode1` | `Last contact` de `dfsadmin -report` |
| 07:44:17 | `gold/_state.json` passe à `PUBLISHING` ; `docker stop gtdp-datanode1-1`. `datanode1` journalise `RECEIVED SIGNAL 15: SIGTERM` à 07:44:17,856 et `SHUTDOWN_MSG` 28 ms plus tard. Les blocs de `dim_date` sont alloués avant, de 07:44:17,185 à 07:44:17,679 | `wait-state.log`, événement, journaux |
| 07:44:18,052 | Premier pipeline vers `datanode1` : un exécuteur (`dim_customer`) échoue en 0,3 s (`Connection reset by peer`), redemande le bloc et l'écrit sur `datanode2` et `datanode3` | `namenode-allocations-replay.log`, `gold-replay-executors-hdfs.log` |
| 07:44:18,382 | Seule reprise de pipeline d'un bloc en cours d'écriture : le journal d'événements Spark du rejeu (`updatePipeline(blk_1073794502...)`), pas un fichier gold | `namenode-allocations-replay.log` |
| 07:44:18,471 | Le driver ouvre `_dim_customer.lock.takeover-...tmp` sur le pipeline `datanode3`, `datanode2`, `datanode1`. `datanode2` attend sa connexion vers `datanode1`, troisième, 65 s | `namenode-allocations-replay.log` |
| 07:44:19 | Le NameNode, qui croit `datanode1` vivant, lui demande de recevoir des copies de blocs écrits pendant la panne : 17 demandes, perdues d'avance | journal du NameNode |
| 07:45:23,500 | `datanode2` abandonne : `ConnectTimeoutException: 65000 millis ... remote=/172.22.0.4:9866`. Le driver redemande le bloc sans `datanode1` à 07:45:23,517 | `datanodes-mirror-replay.log` |
| 07:45:23,571 | Le driver ouvre `_dim_product.lock...tmp` avec `datanode1` en premier, et le redemande 2,1 s plus tard | `namenode-allocations-replay.log` |
| 07:45:25,6 à 07:45:28,3 | Un autre conteneur répond à l'adresse `172.22.0.4` de `datanode1` : 28 connexions refusées dans les journaux de `datanode2` et `datanode3`, 12 dans ceux des clients. 39 écritures redemandent leur bloc en 11 à 44 ms | journaux des DataNodes et des clients |
| 07:45:28,776 | Le driver ouvre `_kpi_revenue_moving_avg.lock.takeover-...tmp` avec `datanode1` en deuxième ; `datanode2` échoue sur `No route to host` 9,2 s plus tard | journaux du NameNode et de `datanode2` |
| 07:45:38,473 | Le driver ouvre `_kpi_price_deviation.lock...tmp` avec `datanode1` en premier. Sa propre connexion attend 60 s (`ConnectTimeoutException: 60000 millis`) | journaux du NameNode et du driver |
| 07:45:46 | Premier échantillon où `datanode1` est `dead` (2 vivants, 1 mort), 91 s après le dernier battement de cœur | échantillon |
| 07:45:49,352 | `BLOCK* removeDeadDatanode: lost heartbeat from 172.22.0.4:9866`. Les 99 fichiers ouverts ensuite reçoivent un pipeline de 2 DataNodes | journal du NameNode |
| 07:45:49 | Le groupe en erasure coding de `products_20260101.csv` (une seule cellule) est recopié sur `datanode3` | journal du NameNode |
| 07:45:51 | Compteur `Under replicated blocks` à 5 552 (il ne compte pas les 17 copies demandées), 3 groupes en erasure coding en redondance réduite, 0 manquant, 0 corrompu | échantillon |
| 07:46:38,553 | Le driver redemande son dernier bloc sans `datanode1` | journal du NameNode |
| 07:46:45 | Fin du rejeu : `SUCCESS`, 178,2 s, `rows_in` = `rows_out` = 970 262, `dates_rewritten` = 0 | audit |
| 07:46:52 | `fsck /` : `HEALTHY`, 5 568 blocs sur 5 568 sous-répliqués, 0 manquant, 0 corrompu | `fsck-during.txt` |
| 07:47:00 | Lecture dégradée de `/benchmarks/ec/orders_20260101.csv` (713 401 398 octets) en 3,0 s, MD5 identique à la copie répliquée | `ec-read-during.txt` |
| 07:47:32 | Empreinte gold après le rejeu identique à la référence (première version de l'empreinte) | événement `fingerprints_identical` |
| 07:47:35 | Démarrage du DataNode de secours `datanode4` (profil `spare`) | événement |
| 07:47:36 | `datanode4` s'enregistre avec l'adresse `172.22.0.4`, libérée par `datanode1` ; le NameNode efface l'entrée morte de `datanode1` | journal du NameNode, échantillon |
| 07:47:37 | Premières copies vers `datanode4` | journal du NameNode |
| 08:33:16 | Le NameNode planifie enfin la reconstruction des 3 groupes en erasure coding, alors que 1 916 blocs répliqués attendent encore ; `datanode4` reçoit les 3 unités reconstruites à 08:33:19, 08:33:19 et 08:33:22 | journal du NameNode |
| 08:57:26 | Premier échantillon sans bloc sous-répliqué. Le NameNode a demandé 5 574 copies vers `datanode4` : 5 571 répliques et 3 unités, en 69 min 50 s | échantillon, journal du NameNode |
| 08:57:34 | `docker start gtdp-datanode1-1` (réparation) | événement |
| 08:57:36 | `datanode1` s'enregistre sous la nouvelle adresse `172.22.0.12` et envoie un rapport de 5 607 blocs. Le NameNode en garde 5 452 ; les 155 autres appartenaient à des fichiers supprimés pendant la panne | journal du NameNode (`processReport`) |
| 08:57:38 | Le NameNode choisit 5 452 répliques en trop : 5 448 sur `datanode2`, 4 sur `datanode1` | journal du NameNode (`chooseExcessRedundancies`) |
| 08:58:06 | Plus aucune réplique en trop ni suppression en attente ; `datanode2` ne porte plus que 127 blocs | échantillon |
| 08:58:12 | `datanode4` écrit dans `dfs.exclude`, `-refreshNodes` : `Starting decommission of 172.22.0.4:9866 ... with 5574 blocks` | journal du NameNode |
| 09:45:34 | `Decommissioning complete for node 172.22.0.4:9866` : 5 574 blocs recopiés, surtout vers `datanode2`, en 47 min 22 s | journal du NameNode |
| 09:45:42 | `docker stop gtdp-datanode4-1` | événement |
| 09:47:11 | `datanode4` mort et `Decommissioned`, 89 s après son arrêt | échantillon |
| 09:47:19 | 3 DataNodes vivants, 0 bloc sous-répliqué, manquant ou corrompu, `fsck /` `HEALTHY` ; fin du scénario (`==> resilience scenario OK`) | `dfsadmin-after.txt`, `fsck-after.txt` |
| 09:47:51 | Fichier d'exclusion vidé et `-refreshNodes`, à la main (le scénario le fait désormais) : `datanode4` passe en `Decommission Status : Normal`, toujours mort | `events.csv`, `dfsadmin-final.txt` |
| 09:47:55 | `fsck /` : `HEALTHY`, 3 DataNodes, 0 bloc sous-répliqué, manquant ou corrompu | `fsck-final.txt` |

### Second exercice : gold reconstruite en entier pendant la panne

| Heure | Fait | Source |
| --- | --- | --- |
| 11:42:27 | Début de `gold-write-under-failure.sh` : cluster nominal, aucune application YARN, zone `/user/etl/gold-write-under-failure` absente | `events.csv` |
| 11:43:08 | Empreinte de `/datalake/gold` avec la nouvelle fonction (`fingerprint-datalake.json`) | événement |
| 11:43:09 | Job gold soumis, gold et audit dirigés vers la zone vide : `application_1795715756506_0447`, exécution `20261127T114323Z-f2bd3a51` | événement, audit |
| 11:43:57,361 | Premier bloc de `fact_sales` (répertoire de préparation `_fact_sales.staging-...`) | `namenode-allocations.log` |
| 11:43:59 | Le déclencheur voit les premiers fichiers de `fact_sales` (105 déjà créés à cet instant) | `trigger.log` |
| 11:44:00,244 | `datanode1` journalise `RECEIVED SIGNAL 15: SIGTERM`, puis `SHUTDOWN_MSG` 35 ms plus tard | `datanode1.log` |
| 11:44:00,258 à 11:44:00,430 | 20 fichiers de `fact_sales` reçoivent un pipeline avec `datanode1` et s'écrivent sans erreur : le DataNode finissait de s'arrêter | `allocations.txt` |
| 11:44:00,87 | 2 connexions refusées par le conteneur qui s'arrête ; reprise du pipeline du journal d'événements Spark, seule reprise de la panne | `datanodes-mirror.log`, `namenode-allocations.log` |
| 11:44:00,9 | 4 écritures d'exécuteurs, ouvertes 0,7 s après l'arrêt, attendent 38,6 s, 60,0 s, 65,1 s et 65,1 s : l'adresse du conteneur arrêté répondait encore à la résolution | `allocations.txt` |
| 11:44:39,517 | Premier `No route to host` : à partir de là, une connexion vers `datanode1` échoue en 3 s environ | `datanodes-mirror.log` |
| 11:45:28 | Premier échantillon où `datanode1` est `dead`, 88 s après l'arrêt | échantillon |
| 11:45:39,762 | `BLOCK* removeDeadDatanode: lost heartbeat from 172.22.0.12:9866`, 99,5 s après l'arrêt. 72 fichiers de `fact_sales` avaient redemandé leur bloc sans `datanode1` (`Excluding datanode`) | journaux du NameNode et des exécuteurs |
| 11:45:39,8 à 11:45:44,026 | Les 88 derniers fichiers de `fact_sales` reçoivent directement un pipeline de 2 DataNodes | `namenode-allocations.log` |
| 11:46:04 | Fin du job : `SUCCESS`, 159,3 s, 367 dates écrites, 970 262 lignes relues | audit |
| 11:46:05 | `fsck` de `fact_sales` dans la zone : `HEALTHY`, 367 blocs, tous à 2 répliques vivantes, 0 manquant, 0 corrompu | `fsck-fact-sales-outage.txt` |
| 11:46:07 | `docker start gtdp-datanode1-1` ; `datanode1` s'enregistre à 11:46:08,896 sous l'adresse `172.22.0.11` | événement, journal du NameNode |
| 11:49:05 | Compteur `Under replicated blocks` à 0. 49 copies restaient demandées (`fsck-after.txt` compte 49 blocs sous-répliqués) : le script attend désormais aussi `pending_reconstruction` à 0. `fsck /` en compte 0 à 12:04:52 | échantillon, `fsck-after.txt` |
| 11:49:37 | Empreinte de la zone identique à celle de `/datalake/gold` : 14 tables et 367 partitions de `fact_sales` | événement `fingerprints_identical` |
| 11:49:42 | Zone effacée | événement `scratch_deleted` |
| 11:52:31 | Même reconstruction sans panne (`application_1795715756506_0449`, zone `/user/etl/gold-write-baseline`) : `SUCCESS` en 49,1 s, même empreinte, `fact_sales` écrite en 4,4 s. Zone effacée | `gold-audit.txt`, `baseline-fact-sales-allocations.log` |

### Retour au nominal final

| Heure | Fait | Source |
| --- | --- | --- |
| 12:04:44 | Conteneur et volume de `datanode4` supprimés (`docker compose --profile spare rm --stop --force datanode4`, `docker volume rm gtdp_datanode4`) | commande manuelle |
| 12:04:52 | Aucune application YARN, `fsck /` `HEALTHY`, 3 DataNodes vivants, `datanode4` encore listé mort | commande manuelle |
| 12:05:19 | Second et dernier redémarrage du NameNode (`docker restart gtdp-namenode-1`) | commande manuelle |
| 12:05:22 à 12:05:24 | Les 3 DataNodes se réenregistrent | journal du NameNode |
| 12:05:30 | `Safe mode is OFF` | journal du NameNode |
| 12:05:53 | 3 DataNodes vivants, aucun mort, 0 bloc sous-répliqué, manquant ou corrompu, `fsck /` `HEALTHY`, JMX `NumDeadDataNodes` = 0 | `dfsadmin-nominal.txt`, `fsck-nominal.txt` |

## Détection (symptôme)

Le cluster n'a pas d'alerte : l'échantillonneur du scénario et `hdfs dfsadmin -report` ont fait
la détection. Trois symptômes sont apparus dans cet ordre.

1. **Erreurs d'écriture, dès l'arrêt.** Au premier exercice, les clients HDFS ont abandonné
   44 blocs alloués à `datanode1` et les ont redemandés sans lui : 12 dans le journal du driver,
   32 dans ceux des exécuteurs (`Excluding datanode DatanodeInfoWithStorage[172.22.0.4:9866...]`).
   Au second, les exécuteurs l'ont fait 72 fois. Personne ne lit ces journaux en temps réel.
2. **DataNode mort dans le rapport, 90 s après son dernier battement de cœur.** Le dernier
   battement de cœur date de 07:44:15. L'échantillon de 07:45:41 montre encore `datanode1`
   vivant, celui de 07:45:46 le montre mort. Le passage a eu lieu entre 86 s et 91 s après le
   dernier battement de cœur, pour 90 s attendues (`2 × 30 s + 10 × 3 s`).
   `dfsadmin -report` calcule cet état à chaque appel.
3. **Répliques retirées 94 s après le dernier battement de cœur.** Le contrôle périodique du
   NameNode, toutes les 30 s, a retiré `datanode1` à 07:45:49,352 (`removeDeadDatanode`).
   Le compteur de blocs sous-répliqués est passé de 15 à 5 552 entre les échantillons de 07:45:46
   et 07:45:51. L'écart attendu entre le seuil de 90 s et ce contrôle va de 0 à 35 s (période de
   30 s, plus le sommeil de 5 s du moniteur). Il a été de 4 s.

Avant 07:45:49, le compteur de blocs sous-répliqués est resté bas (24 au plus) : seuls les blocs
écrits pendant la panne sur 2 DataNodes comptaient. Le NameNode les croyait réparables vers
`datanode1` et a demandé 17 copies vers lui. Ces 17 demandes sont restées en attente au-delà du
délai de 300 s (`dfs.namenode.reconstruction.pending.timeout-sec`). La colonne
`pending_reconstruction` passe de 21 à 4 à 07:51:11, quand le NameNode les abandonne ensemble.

## Impact

### Jobs

- Le rejeu du premier exercice, `application_1795715756506_0444`, a réussi : code de sortie 0,
  audit `SUCCESS`, `rows_in` = `rows_out` = 970 262. Ses 130 jobs Spark ont réussi, sans tâche en
  échec ni tâche tuée (Spark History Server).
- La reconstruction du second exercice, `application_1795715756506_0447`, a réussi : audit
  `SUCCESS`, 367 dates de `fact_sales` écrites, 970 262 lignes relues.
- Aucun conteneur YARN n'a été perdu. Les DataNodes et les NodeManagers sont des conteneurs
  distincts : arrêter `gtdp-datanode1-1` n'a touché ni `nodemanager1` ni `nodemanager2`.
- Aucun job n'a été tué : aucun verrou de table n'a eu besoin d'être repris.

### Données

- Aucune donnée perdue. `hdfs fsck /` est resté `HEALTHY` pendant les deux pannes avec
  `Missing blocks: 0` et `Corrupt blocks: 0` (`fsck-during.txt`, `fsck-datalake-during.txt`,
  `ecriture-sous-panne/fsck-fact-sales-outage.txt`).
- **Écritures pendant la panne, second exercice.** Les 14 tables ont été écrites dans une zone
  vide. Sur les 367 fichiers de `fact_sales`, 187 ont été ouverts avant l'arrêt, 20 dans les
  0,2 s où `datanode1` s'arrêtait, 72 entre son arrêt et sa déclaration de mort (bloc redemandé
  sans lui) et 88 après (pipeline de 2 DataNodes d'emblée). Les 13 autres tables ont été écrites
  après 11:45:44, `datanode1` mort. L'empreinte de la zone est identique à celle de
  `/datalake/gold` (`fingerprint-scratch.json` et `fingerprint-datalake.json` identiques octet
  pour octet), et à celle de la même reconstruction sans panne (`fingerprint-baseline.json`).
- **Écritures pendant la panne, premier exercice.** Le rejeu a remplacé 13 tables. `dim_date`
  était publiée 0,2 s avant l'arrêt, les 12 autres l'ont été après. `fact_sales` n'a pas été
  écrite : ses 367 partitions avaient la même empreinte que celles publiées (`dates_rewritten` =
  0), et ses fichiers datent toujours de 01:15. Le rejeu ne prouve donc rien pour 86 % des lignes
  de gold. Les empreintes avant et après sont identiques, mais elles ont été calculées avec la
  première version de l'empreinte, aveugle à une valeur déplacée vers une colonne nulle voisine.
- **Empreintes recalculées.** La gold publiée n'a pas changé depuis le rejeu de 07:43 :
  `_state.json` porte toujours son exécution, `20261127T074347Z-6938fb03`. L'empreinte de 11:43,
  calculée avec la nouvelle fonction, est donc l'empreinte « après » du premier exercice. Son
  empreinte « avant » ne peut plus être recalculée pour les 13 tables remplacées, dont les
  anciens fichiers ont été supprimés. Elle vaut l'empreinte « après » pour `fact_sales`, que le
  rejeu n'a pas touchée.

La nouvelle empreinte hache, pour chaque ligne, `to_json` de toutes ses colonnes avec leurs
valeurs nulles et les horodatages à la microseconde, puis somme ces `xxhash64` par table et par
partition de `fact_sales` (`scripts/resilience/gold_fingerprint.py`).

| Table | Lignes | Somme `xxhash64` (nouvelle empreinte, `/datalake/gold` = zone du second exercice) |
| --- | ---: | ---: |
| `fact_sales` | 970 262 | 2582311945595577817704 |
| `fact_web_events` | 100 046 | -784160607974483122052 |
| `dim_customer` | 5 024 | 102621340915101577186 |
| `dim_product` | 1 321 | 453377749488891012564 |
| `dim_date` | 367 | -160928901499044930732 |
| 9 tables `kpi_*` | 51 975 au total | une somme par table, dans les fichiers JSON |

- **Exposition, premier exercice.** Les répliques de `datanode1` ont disparu à son arrêt, à
  07:44:18. Jusqu'à 08:57:26, soit 73 min 08 s, les blocs répliqués n'avaient plus que
  2 répliques : une seconde panne n'aurait rien perdu, une troisième aurait perdu des blocs.
  Les 3 groupes de `/benchmarks/ec/orders_20260101.csv` n'avaient plus aucune tolérance (2 unités
  sur 3 en `XOR-2-1`) jusqu'à 08:33:22, soit 49 min 04 s. Une seconde panne parmi `datanode2` et
  `datanode3` rendait alors ce fichier de 713 Mo illisible.

### Performance

**Premier exercice : 178,2 s contre 40,0 s.** L'exécution de référence a tourné une minute plus
tôt sur les mêmes données. Le rejeu a atteint `PUBLISHING` 30 s après son début, avant l'arrêt.
Le driver écrit l'un après l'autre de petits fichiers de verrou. Il a attendu 4 fois, 136,4 s des
138,2 s d'écart :

| Fichier du driver | Pipeline proposé | Ouvert | Redemandé | Attente | Ce qui a fini l'attente |
| --- | --- | --- | --- | ---: | --- |
| `_dim_customer.lock.takeover` | `datanode3`, `datanode2`, `datanode1` | 07:44:18,471 | 07:45:23,517 | 65,0 s | délai de `datanode2` vers `datanode1` : 60 s de `dfs.client.socket-timeout`, plus 5 s par DataNode en aval |
| `_dim_product.lock` | `datanode1`, `datanode2`, `datanode3` | 07:45:23,571 | 07:45:25,635 | 2,1 s | début de la fenêtre de refus |
| `_kpi_revenue_moving_avg.lock.takeover` | `datanode2`, `datanode1`, `datanode3` | 07:45:28,776 | 07:45:38,018 | 9,2 s | `No route to host` sur `datanode2` |
| `_kpi_price_deviation.lock` | `datanode1`, `datanode2`, `datanode3` | 07:45:38,473 | 07:46:38,553 | 60,1 s | délai du driver lui-même vers `datanode1` : `dfs.client.socket-timeout` |

Les 40 autres écritures ont redemandé leur bloc en moins d'une seconde, dont 39 entre 07:45:25,8
et 07:45:28,3. Pendant ces 3 s, un autre conteneur tenait l'adresse `172.22.0.4` et refusait les
connexions. Docker redonne l'adresse d'un conteneur arrêté au conteneur suivant : le rapport de
07:46:52 montre `172.22.0.4` résolu en `gtdp-client-961864-28955`, un conteneur client. Le
scénario ne démarre aucun conteneur entre 07:44:18 et 07:46:47 ; celui de la fenêtre n'est pas
identifié. Le placement de `datanode1` dans les 44 pipelines était aléatoire : 14 fois premier,
19 fois deuxième, 11 fois troisième (`allocations-replay.txt`).

**Second exercice : 159,3 s contre 49,1 s pour la même reconstruction sans panne.** L'écriture
de `fact_sales` a pris 106,7 s au lieu de 4,4 s. Aucun conteneur n'a démarré pendant la panne :
le script n'en lance aucun entre l'arrêt et la fin du job. Les 72 écritures redemandées se
répartissent ainsi (`allocations.txt`) :

| Délai avant de redemander le bloc | Écritures | Réponse du réseau |
| --- | ---: | --- |
| moins de 1 s | 13 | 8 dans la seconde de l'arrêt (4 refus, 4 connexions coupées par le conteneur qui s'arrêtait), 5 `No route to host` ensuite |
| 1 à 3 s | 18 | `No route to host` |
| 3 à 4 s | 34 | `No route to host`, 3,1 s le plus souvent |
| 4 à 15 s | 3 | `No route to host` (8,2 s, 8,2 s et 11,7 s) |
| 15 s et plus | 4 | délais de connexion de 60,0 s (client) et de 65,1 s (DataNode, deux fois), `No route to host` à 38,6 s |

Sur ces 72 échecs, 61 sont des `No route to host` (18 côté client, 43 côté DataNode), 4 des
refus, 4 des connexions coupées et 3 des délais de connexion (`gold-executors-hdfs.log`,
`datanodes-mirror.log`).

Les 4 attentes longues ont commencé 0,7 s après l'arrêt. L'adresse de `datanode1` se résolvait
encore, les paquets partaient vers un conteneur disparu, et la connexion attendait son délai. À
partir de 11:44:39, 39 s après l'arrêt, l'adresse ne se résolvait plus et chaque connexion
échouait en 3 s environ. Chaque tâche écrit ses fichiers l'un après l'autre et paie un échec par
fichier ouvert avant la déclaration de mort. D'où les 102 s de plus pour `fact_sales`.

**Ce qu'il faut attendre ailleurs.** Ces mesures valent pour ce banc d'essai : deux exécutions,
un seul DataNode arrêté, Docker sur une machine. Elles bornent le coût d'une panne. Avec
3 DataNodes pour une réplication 3, chaque écriture ouverte avant que le NameNode déclare le nœud
mort, 90 s à 125 s après son dernier battement de cœur, échoue une fois. Cet échec dure 60 s (client vers le premier DataNode) ou
60 s plus 5 s par DataNode en aval (DataNode vers le suivant) tant que l'adresse du nœud se
résout encore, environ 3 s ensuite, quelques millisecondes si un autre hôte répond à cette
adresse. Le ralentissement d'un job dépend donc du nombre de blocs que chaque écrivain ouvre
dans cette fenêtre. Sur des machines physiques, aucune autre machine ne reprend l'adresse en
quelques secondes : le second exercice, sans réattribution, est la référence la plus proche.
C'est une déduction, pas une mesure. Les 178 s du premier exercice dépendent du conteneur qui a
repris l'adresse pendant 3 s.

**Lecture et re-réplication, premier exercice.**

- La lecture complète de `/benchmarks/ec/orders_20260101.csv` pendant la panne a pris 3,0 s,
  contre 2,6 s pour sa copie répliquée lue juste après. Chacun de ses 3 groupes avait perdu une
  unité de données, pas de parité : le client a reconstruit ces cellules par XOR.
- La re-réplication a pris 69 min 50 s pour 5 571 répliques et 3 unités, de l'enregistrement de
  `datanode4` à 07:47:36 au premier échantillon sans bloc sous-répliqué à 08:57:26. Le rythme,
  1,33 bloc par seconde, est constant sur chaque fenêtre de 10 min.

## Hypothèses et preuves

| Hypothèse | Verdict | Preuve |
| --- | --- | --- |
| Le rejeu perd ou duplique des lignes quand un DataNode tombe pendant ses écritures. | réfutée | second exercice : 160 fichiers de `fact_sales` et 13 tables écrits sur 2 DataNodes, empreinte identique à `/datalake/gold` sur 14 tables et 367 partitions, `fsck` sans bloc manquant |
| Le premier rejeu a écrit toutes les tables pendant la panne. | réfutée | audit : `dates_rewritten` = 0, `dates_unchanged` = 367 ; fichiers de `fact_sales` datés de 01:15 |
| Le NameNode déclare le nœud mort en 90 s. | confirmée | `Last contact` 07:44:15, mort entre 07:45:41 et 07:45:46, `removeDeadDatanode` à 07:45:49 |
| Avec 3 DataNodes, les répliques perdues n'ont aucune cible. | confirmée | de 07:45:50 à 07:47:36, 5 552 blocs sous-répliqués et aucune copie demandée ; les copies démarrent à 07:47:37, une seconde après l'enregistrement de `datanode4` |
| Le ralentissement vient des écritures vers le nœud arrêté. | confirmée | 4 attentes du driver, 136,4 s des 138,2 s d'écart ; second exercice, 72 écritures redemandées et `fact_sales` écrite en 106,7 s au lieu de 4,4 s |
| Le NameNode propose `datanode1` en premier, et l'attente est le délai du client. | réfutée | `datanode1` était troisième dans le pipeline de l'attente de 65 s ; ces 65 s sont le délai de `datanode2` vers son DataNode suivant (`datanode2` : `ConnectTimeoutException: 65000 millis`) |
| Les exécuteurs ont reçu un refus immédiat du nœud arrêté. | réfutée | un conteneur arrêté ne répond pas ; les refus viennent d'un autre conteneur qui tenait `172.22.0.4` de 07:45:25,8 à 07:45:28,3, et le driver en a reçu aussi |
| `dfs.namenode.avoid.write.stale.datanode` évite ces attentes avec 3 DataNodes. | réfutée | HDFS jetable, image du cluster : 8 premières allocations sur 8 nomment le nœud arrêté, réglage à `true` comme à `false` ; avec 4 DataNodes, 0 sur 8 à `true`, 6 sur 8 à `false` (`minihdfs-stale-3.txt`, `minihdfs-stale-4.txt`) |
| La re-réplication est limitée par `dfs.namenode.replication.max-streams`. | confirmée, avec la limite dure | 1,33 bloc par seconde ; `pending_reconstruction` reste à 4. HDFS jetable, après la mort du nœud : 4 copies par passage de 3 s avec les réglages actuels, 8 avec `max-streams` 8 et `multiplier` 4, 12 en relevant aussi la limite dure à 8 (`minihdfs-rate.txt`) |
| Les groupes en erasure coding, de priorité la plus haute, sont réparés en premier. | réfutée | entrés en file de priorité 0 à 07:45:49, ils ne sont planifiés qu'à 08:33:16 (`BLOCK* ask [172.22.0.6:9866, 172.22.0.5:9866] to replicate blk_-9223372036854775504_50169 to datanode(s) 172.22.0.4:9866`), alors que 1 916 blocs répliqués attendent encore |
| La lecture d'un fichier en erasure coding reconstruit les données perdues. | confirmée | MD5 `1d3e3c5f1698eba670ee9d04664db2a5` identique pour la copie EC et la copie répliquée ; CRC composite `5056866c` identique à celui du Lot 7 ; `fsck` montre l'unité de données manquante dans chaque groupe |

## Cause

**Cause directe.** Le conteneur `gtdp-datanode1-1` a été arrêté volontairement, pendant
l'exercice, alors que gold écrivait.

**Facteurs qui ont déterminé l'impact.**

1. **3 DataNodes pour une réplication 3.** Les répliques d'un nœud perdu n'ont nulle part où
   aller : sans `datanode4`, les blocs restent à 2 répliques jusqu'au retour du nœud.
2. **Jusqu'à 125 s pendant lesquelles le NameNode croit le nœud vivant.** Il lui faut 3 cibles
   parmi 3 DataNodes : chaque nouveau bloc reçoit un pipeline qui contient le nœud arrêté, quelle
   que soit sa place. Chaque écrivain échoue une fois par bloc ouvert dans cette fenêtre, puis
   redemande le bloc sans lui. `dfs.namenode.avoid.write.stale.datanode` n'y change rien avec
   3 DataNodes : faute d'une troisième cible fraîche, le NameNode reprend le nœud silencieux.
3. **Un échec coûte ce que le réseau répond pour l'adresse du nœud arrêté.** Tant que l'adresse
   se résout encore (les 39 premières secondes au second exercice), la connexion attend son
   délai : 60 s pour un client vers le premier DataNode (`dfs.client.socket-timeout`), 60 s plus
   5 s par DataNode en aval pour un DataNode vers le suivant. Ensuite, elle échoue en 3 s
   environ (`No route to host`). Un conteneur qui reprend l'adresse la refuse en quelques
   millisecondes. Le driver écrit ses verrous l'un après l'autre : ses attentes s'additionnent.
4. **Re-réplication bridée.** Chaque DataNode source reçoit 2 copies à la fois
   (`dfs.namenode.replication.max-streams`), et au plus 4 au total
   (`dfs.namenode.replication.max-streams-hard-limit`), à chaque battement de cœur de 3 s :
   1,33 bloc par seconde. Les blocs sont petits (453 957 octets en moyenne) : le réseau et les
   disques ne limitent pas.
5. **Les blocs les plus exposés réparés en dernier.** Le NameNode parcourt ses files de blocs
   à réparer avec un signet, 6 blocs par passage de 3 s (3 DataNodes vivants ×
   `dfs.namenode.replication.work.multiplier.per.iteration` = 2). Un bloc sans cible au moment
   du passage reste derrière le signet. Les 3 groupes en erasure coding ont été examinés à
   07:45:49, dans le passage qui a recopié la cellule de `products`, avant l'arrivée de
   `datanode4` : aucune cible. Le signet ne revient en tête qu'à la fin d'un parcours complet
   des files, ou toutes les 2 400 itérations, soit 2 h
   (`dfs.namenode.redundancy.queue.restart.iterations`). Le parcours des quelque 5 400 blocs
   répliqués restants, à 2 blocs par seconde, devait finir entre 08:32:38 et 08:33:52, et le
   NameNode a planifié les 3 groupes à 08:33:16. Aucun journal ne montre le signet : ce facteur
   est déduit de ces chiffres.
6. **Adresses IP réattribuées par Docker.** Un conteneur a repris l'adresse de `datanode1`
   pendant 3 s au premier exercice, puis `datanode4` l'a reçue. Le NameNode a effacé l'entrée
   morte de `datanode1` à l'enregistrement de `datanode4`. Les journaux par adresse IP deviennent
   ambigus, et le fichier d'exclusion, résolu en adresses IP, peut viser le mauvais conteneur
   (voir la résolution).

## Résolution

Les étapes suivent la procédure « Perte d'un DataNode » du [runbook HDFS](../runbook/hdfs.md).

1. **Remplacer le nœud perdu.** `docker compose --profile spare up --detach --no-deps
   datanode4` à 07:47:35. `datanode4` s'est enregistré une seconde plus tard et a reçu
   5 571 répliques et 3 unités d'erasure coding. Plus aucun bloc sous-répliqué à 08:57:26.
   Les 3 groupes en erasure coding sont restés sans tolérance jusqu'à 08:33:22 (facteur 5
   de la cause).
2. **Réparer `datanode1`.** `docker start gtdp-datanode1-1` à 08:57:34. Il s'est enregistré
   du premier coup sous une nouvelle adresse, `172.22.0.12`. Chaque bloc avait alors
   4 répliques. Le NameNode a choisi 5 448 des 5 452 répliques en trop sur `datanode2`, pas sur
   le nœud revenu : il supprime la réplique du DataNode qui annonce le moins d'espace libre, et
   `datanode2` annonçait 706,84 Go contre 706,97 Go pour les autres. Les 4 DataNodes partagent le
   disque de la machine Docker : l'écart, 0,02 %, ne veut rien dire. Les suppressions étaient
   finies à 08:58:06. `datanode2` ne portait plus que 127 blocs, et presque tous les autres
   n'avaient plus de réplique que sur `datanode1`, `datanode3` et `datanode4`.
3. **Retirer le secours.** `datanode4` écrit dans `/opt/hadoop/etc/hadoop/dfs.exclude` du
   NameNode, puis `hdfs dfsadmin -refreshNodes` à 08:58:12. Le NameNode devait mettre sur un
   autre nœud chacun des 5 574 blocs de `datanode4` : presque tous sont allés vers `datanode2`,
   vidé à l'étape précédente. La décommission a fini à 09:45:34, en 47 min 22 s, à 2 blocs par
   seconde. Les copies sont parties de 3 sources à parts presque égales (`datanode4` 1 860,
   `datanode3` 1 849, `datanode1` 1 815 demandes) : 3 sources × 2 copies par battement de cœur
   de 3 s donnent 2 blocs par seconde, contre 1,33 avec les 2 sources de la re-réplication
   (`datanode2` 2 786, `datanode3` 2 785).
4. **Arrêter le secours.** `docker stop gtdp-datanode4-1` à 09:45:42 ; mort et
   `Decommissioned` 89 s plus tard. À 09:47:51, le fichier d'exclusion a été vidé et relu : le
   rapport de 09:47 montrait déjà l'adresse `172.22.0.4` de `datanode4` résolue en
   `gtdp-client-1136911-31632`, un conteneur client démarré entre-temps (`dfsadmin-after.txt`).
   Le NameNode gardait cette adresse dans sa liste d'exclusion : un DataNode qui l'aurait reçue
   aurait été décommissionné.
5. **Supprimer le secours et l'effacer du NameNode.** Le volume de `datanode4` gardait ses
   5 574 fichiers de blocs (2,8 Go), que le NameNode ne comptait plus. Conteneur et volume ont été
   supprimés à 12:04:44. `datanode4` restait listé mort, `Decommission Status : Normal`, et
   déclenchait l'alerte de l'action 2. Le redémarrage du NameNode à 12:05:19 l'a effacé.

## Validation du retour au nominal

Dernier `hdfs dfsadmin -report` (`dfsadmin-nominal.txt`, 12:05:53, après le redémarrage du
NameNode) :

```
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
Erasure Coded Block Groups: 
	Low redundancy block groups: 0
	Block groups with corrupt internal blocks: 0
	Missing block groups: 0
Live datanodes (3):
	datanode1  Normal  Num of Blocks: 5589
	datanode2  Normal  Num of Blocks: 5590
	datanode3  Normal  Num of Blocks: 5590
```

Aucune section `Dead datanodes`. JMX `FSNamesystemState` : `NumLiveDataNodes` 3,
`NumDeadDataNodes` 0, `NumStaleDataNodes` 0.

Dernier `hdfs fsck /` (`fsck-nominal.txt`, 12:06:11) :

```
Status: HEALTHY
 Number of data-nodes:	3
 Total blocks (validated):	5586 (avg. block size 453502 B)
 Under-replicated blocks:	0 (0.0 %)
 Missing blocks:		0
 Corrupt blocks:		0
 Total block groups (validated):	4 (avg. block group size 178370341 B)
 Under-erasure-coded block groups:	0 (0.0 %)
 Missing block groups:		0
 Corrupt block groups:		0
The filesystem under path '/' is HEALTHY
```

`scripts/cluster/status.sh` répond `HDFS  3 live / 0 dead DataNodes, safe mode off` et
`YARN  2 RUNNING / 2 NodeManagers`. Aucune application YARN ne tourne. Le conteneur et le volume
de `datanode4` n'existent plus. Le fichier d'exclusion est vide et `dfs.hosts.exclude` reste
chargé. Le journal `BlockStateChange` du NameNode est au niveau INFO.

La chronologie `timeline.csv` compte deux fois `datanode4` dans les colonnes `live` et
`decommissioning` de 08:58:16 à 09:45:31 (5 et 2 au lieu de 4 et 1) :
`dfsadmin -report` liste un nœud en décommission dans deux sections, et l'échantillonneur de
l'exécution lisait la seconde comme des nœuds vivants. Les colonnes par DataNode sont justes.
Le défaut est corrigé et testé sur le rapport capturé (`report-decommissioning.txt`).

## Actions préventives

| # | Action | Rôle propriétaire | Effet attendu |
| --- | --- | --- | --- |
| 1 | Garder 4 DataNodes en service pour une réplication 3, ou au moins le secours `datanode4` prêt à démarrer, comme aujourd'hui. Avec plusieurs racks, activer la topologie (`net.topology.script.file.name`). | architecte plateforme | les répliques perdues ont une cible dès la détection, sans geste manuel, et l'action 3 devient utile ; 640 Mio de mémoire en plus |
| 2 | Alerter sur `NumDeadDataNodes - NumDecomDeadDataNodes > 0`, sur `MissingBlocks > 0` (alerte immédiate), sur `LowRedundancyECBlockGroups > 0` et sur `UnderReplicatedBlocks + PendingReconstructionBlocks > 0` pendant plus de 10 min (JMX `FSNamesystem` du NameNode), et sur les `JvmPauseMonitor` de niveau WARN des DataNodes. Un secours retiré reste compté mort jusqu'au redémarrage du NameNode : la procédure de retrait finit par ce redémarrage (runbook, étape 5.7). | exploitation (SRE) | détection en moins de 2 min sans échantillonneur manuel ; sur le cluster nominal, `NumDeadDataNodes` vaut 0 (1 avant le redémarrage de 12:05, 0 après) |
| 3 | Activer `dfs.namenode.avoid.write.stale.datanode` et `dfs.namenode.avoid.read.stale.datanode` (seuil par défaut de 30 s) une fois l'action 1 faite, pas avant. Mesuré dans un HDFS jetable de l'image du cluster (`scripts/resilience/minihdfs/run.sh stale 3` et `stale 4`) : avec 3 DataNodes, 8 premières allocations sur 8 nomment le nœud arrêté depuis 45 s, réglage à `true` comme à `false` ; avec 4 DataNodes, 0 sur 8 à `true`, 6 sur 8 à `false`. | architecte plateforme | avec 4 DataNodes, plus de nouveau pipeline vers un nœud silencieux depuis 30 s ; aucun effet pendant ces 30 premières secondes, ni avec 3 DataNodes |
| 4 | Passer `dfs.namenode.replication.max-streams` de 2 à 8, `dfs.namenode.replication.work.multiplier.per.iteration` de 2 à 4 et `dfs.namenode.replication.max-streams-hard-limit` de 4 à 8. Les trois se changent à chaud : modifier `hdfs-site.xml` du NameNode, puis `hdfs dfsadmin -reconfig namenode namenode:8020 start`. Mesuré dans un HDFS jetable (`scripts/resilience/minihdfs/run.sh rate`, 900 blocs, un DataNode sur 3 tué, un quatrième vivant), en régime établi après la mort du nœud : 4 copies par passage de 3 s avec les valeurs actuelles (1,33 bloc par seconde), 8 sans relever la limite dure (2,67), 12 avec les trois (4,0). Chaque passage examine `multiplier` × DataNodes vivants blocs et confie à chaque source au plus le plus petit de `max-streams` et de la limite dure. | architecte plateforme | re-réplication 3 fois plus rapide : les 5 571 répliques du 27 novembre en 23 min environ au lieu de 70 (calculé sur ce rythme) |
| 5 | Ramener `dfs.namenode.redundancy.queue.restart.iterations` de 2 400 à 20 (lu au démarrage du NameNode), et appliquer la règle du runbook : redémarrer le DataNode, puis démarrer `datanode4` s'il n'est pas `live` 5 min après, ou tout de suite si la cause empêche un redémarrage rapide. | architecte plateforme | signet en tête toutes les 60 s : les groupes en erasure coding sans tolérance passent dans la minute qui suit l'arrivée du secours, au lieu de 46 min (déduit du mécanisme, pas mesuré) |
| 6 | Arbitrage de l'erasure coding : `XOR-2-1` sur 3 DataNodes tolère une panne et ne se répare qu'avec un troisième nœud disponible. Le réserver aux données froides et reproductibles ; `RS-3-2` demande 5 DataNodes (ADR-0006). | architecte données | pas de donnée de production sans tolérance pendant une panne |
| 7 | Suivre la procédure du [runbook HDFS](../runbook/hdfs.md#perte-dun-datanode) : vider le fichier d'exclusion une fois `datanode4` mort, à cause de la réattribution des adresses IP, supprimer son conteneur et son volume, redémarrer le NameNode. | exploitation (SRE) | aucun DataNode décommissionné par erreur, aucun ancien bloc rapporté par un secours réutilisé, aucune fausse alerte de nœud mort |
| 8 | Donner de la marge mémoire à la machine Docker pendant les bancs d'essai, et envisager `dfs.namenode.datanode.registration.ip-hostname-check=false` sur ce réseau privé (voir l'événement antérieur). | architecte plateforme | plus de DataNode déclaré mort pour une machine saturée, plus de refus d'enregistrement transitoire |

## Événement antérieur du même jour

L'enquête a trouvé une perte non planifiée de `datanode3`, deux heures avant l'exercice. Elle
n'a pas eu de conséquence visible, mais elle confirme deux risques.

- De 05:28 à 05:33, `datanode1` et `datanode3` ont journalisé des pauses de JVM jusqu'à 22 s,
  dont un ramasse-miettes « jeune » de 28 s (`GC pool 'G1 Young Generation' had collection(s):
  count=2 time=28009ms`). Un tel temps sur un petit tas de 384 Mio signale une machine hôte
  saturée, pas un tas trop petit. Les mesures de performance du Lot 7 tournaient à ce moment.
- Le NameNode a déclaré `datanode3` mort à 05:33:21 (`removeDeadDatanode: lost heartbeat from
  172.22.0.6:9866`).
- À 05:39:12, `datanode3` a voulu se réenregistrer et le NameNode l'a refusé :
  `DisallowedDatanodeException: Datanode denied communication with namenode because hostname
  cannot be resolved (ip=172.22.0.6, hostname=172.22.0.6)`. Le DataNode s'est arrêté, Docker l'a
  relancé (`restart: unless-stopped`, `RestartCount` = 1), et le second enregistrement a réussi
  à 05:39:22.

Actions 2 et 8 du tableau ci-dessus.

## Fichiers de preuve

Le dossier [`2026-11-27-perte-datanode/`](2026-11-27-perte-datanode/) contient la sortie des
scénarios, taillée.

| Fichier | Contenu |
| --- | --- |
| `timeline.csv`, `summary.txt` | premier exercice : un échantillon toutes les 5 s de 07:42 à 09:47, et les jalons calculés par `hdfs_watch.py summarize` |
| `events.csv`, `wait-state.log` | les étapes du premier exercice, la ligne de 09:47:51 ajoutée à la main |
| `dfsadmin-*.txt`, `fsck-*.txt` | rapports avant, pendant, après la re-réplication, pendant la décommission, à la fin du scénario (`after`), après le vidage du fichier d'exclusion (`final`) et après le redémarrage du NameNode (`nominal`) ; `fsck` sans la liste des blocs |
| `ec-*.txt` | emplacement des unités, CRC composites et MD5 de la lecture dégradée |
| `fingerprint-before.json`, `fingerprint-after.json`, `gold-audit.txt`, `gold-runs.log` | empreintes du premier exercice (première version de la fonction), lignes d'audit, début et fin des deux exécutions |
| `gold-replay-driver-hdfs.log`, `gold-replay-executors-hdfs.log` | avertissements du client HDFS du rejeu, sans les piles d'appels ni lignes fusionnées : 12 et 32 `Excluding datanode` |
| `namenode-allocations-replay.log` | toutes les allocations de blocs du NameNode de 07:44:15 à 07:46:47, les reprises de pipeline et la déclaration de mort |
| `allocations-replay.txt` | `scripts/resilience/allocations.py` sur ce journal : les 44 écritures ouvertes vers `datanode1` entre son arrêt et sa mort, et leur délai |
| `datanodes-mirror-replay.log` | une ligne par connexion de `datanode2` et `datanode3` vers `datanode1` qui a échoué, et les 17 copies demandées vers lui |
| `namenode-excerpts.log` | lignes du NameNode : nœud mort, enregistrements, copies demandées, répliques en trop, décommission |
| `minihdfs-stale-3.txt`, `minihdfs-stale-4.txt`, `minihdfs-rate.txt` | sorties de `scripts/resilience/minihdfs/run.sh` (HDFS jetable), mesures des actions 3 et 4 |
| `ecriture-sous-panne/` | second exercice : `events.csv`, `trigger.log`, `timeline.csv`, `gold-audit.txt` (avec la reconstruction sans panne), les 3 empreintes, `namenode-allocations.log`, `allocations.txt`, `datanodes-mirror.log`, `datanode1.log`, avertissements du client HDFS, `dfsadmin` et `fsck` avant et après |

Pour recalculer un tableau d'allocations :

```bash
python scripts/resilience/allocations.py docs/incidents/2026-11-27-perte-datanode/namenode-allocations-replay.log \
	--address 172.22.0.4 --start 07:44:17.856 --end 07:45:49.352
python scripts/resilience/allocations.py docs/incidents/2026-11-27-perte-datanode/ecriture-sous-panne/namenode-allocations.log \
	--address 172.22.0.12 --start 11:44:00.244 --end 11:45:39.762
```

## Rejouer l'exercice

```bash
scripts/resilience/datanode-failure.sh --victim datanode1 --date 2026-01-03
scripts/resilience/gold-write-under-failure.sh --victim datanode1 --date 2026-01-03
```

Les scripts et leurs prérequis sont décrits dans le [runbook HDFS](../runbook/hdfs.md#rejouer-la-panne).
`datanode1` a été choisi parce qu'il portait une unité de données de chacun des 4 groupes en
erasure coding de `/benchmarks/ec` : sa perte force une reconstruction dans chaque groupe.
`datanode2` portait la parité de 2 des 3 groupes de `orders` : l'arrêter n'aurait demandé de
reconstruction que dans un groupe.
