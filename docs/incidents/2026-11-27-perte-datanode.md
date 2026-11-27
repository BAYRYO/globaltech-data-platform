# Incident du 27 novembre 2026 : perte d'un DataNode pendant un rejeu gold

| Champ | Valeur |
| --- | --- |
| Type | exercice planifié (panne provoquée), format sans recherche de coupable |
| Date | 27 novembre 2026, de 07:44:17 à 09:47:51 UTC (2 h 03 min 34 s) |
| Composant | HDFS : DataNode `datanode1` (conteneur `gtdp-datanode1-1`) |
| Gravité | mineure : aucune donnée perdue, aucun job en échec, rejeu gold ralenti de 40 s à 178 s |
| Statut | résolu, cluster revenu à l'état nominal à 09:47:19 UTC |
| Rédaction | rôle exploitation plateforme (Lot 8, KAN-52) |
| Preuves | dossier [`2026-11-27-perte-datanode/`](2026-11-27-perte-datanode/), scénario `scripts/resilience/datanode-failure.sh` |

## Résumé

Le DataNode `datanode1` a été arrêté (`docker stop`, signal SIGTERM) à 07:44:17 UTC, au moment
où le job gold du 2026-01-03 commençait à publier ses tables sur YARN. Le job a fini en succès
à 07:46:45 : sa ligne d'audit est `SUCCESS` et les 14 tables gold ont exactement les mêmes lignes
qu'avant la panne (même nombre de lignes et même somme `xxhash64` par table, mêmes lignes par
partition de `fact_sales`). Le rejeu a duré 178 s contre 40 s pour l'exécution de référence :
le driver a attendu deux fois une connexion vers le DataNode arrêté (65 s puis 60 s), que le
NameNode proposait encore comme cible d'écriture.

Le NameNode a déclaré `datanode1` mort 90 s après son dernier battement de cœur, puis a retiré
ses répliques à 07:45:49. Les 5 552 blocs répliqués sont alors passés à 2 répliques sur 3, et
les 3 groupes de blocs en erasure coding de `/benchmarks/ec/orders_20260101.csv` à 2 unités sur
3. Aucun bloc n'a manqué : `hdfs fsck /` est resté `HEALTHY` avec `Missing blocks: 0` et
`Corrupt blocks: 0`. La lecture du fichier en erasure coding pendant la panne a reconstruit les
données : même MD5 que sa copie répliquée, même CRC composite qu'au Lot 7 (`5056866c`).

Le DataNode de secours `datanode4` a reçu les répliques manquantes en 69 min 50 s, au rythme de
1,33 bloc par seconde. Les 3 groupes en erasure coding, les plus exposés, ont attendu 46 min
leur tour au lieu de passer en premier. `datanode1` est ensuite revenu, le NameNode a supprimé les répliques en
trop, puis `datanode4` a été décommissionné et arrêté. Le cluster est revenu à 3 DataNodes
vivants, 0 bloc sous-répliqué, manquant ou corrompu.

## Chronologie horodatée

Toutes les heures sont en UTC. Les horloges de l'hôte et des conteneurs concordent à moins
d'une seconde (mesure de 07:32:46). « Échantillon » renvoie à `timeline.csv` (un échantillon
toutes les 5 s) ; « événement » à `events.csv`, écrit par le scénario.

### Préparation, hors incident

| Heure | Fait | Source |
| --- | --- | --- |
| 07:40:45 | Aucune application YARN en cours, `hdfs fsck /` `HEALTHY`, 0 bloc sous-répliqué | commande manuelle |
| 07:40:50 | `hdfs-site.xml` (avec `dfs.hosts.exclude`) et `dfs.exclude` vide copiés dans `gtdp-namenode-1`, identiques au dépôt (SHA-256) | `docker cp`, `sha256sum` |
| 07:41:00 | Redémarrage du NameNode (`docker restart gtdp-namenode-1`), le seul de l'exercice | commande manuelle |
| 07:41:04 | Les 3 DataNodes se réenregistrent du premier coup | journal du NameNode |
| 07:41:11 | `Safe mode is OFF`, sorti après 7 s ; `fsck /` `HEALTHY`, 3 DataNodes vivants, `dfs.hosts.exclude` chargé (servlet `/conf`) | journal, commandes |
| 07:42:06 | Début du scénario : fichier d'exclusion vidé, journal `BlockStateChange` du NameNode en DEBUG, échantillonneur démarré | événement `start` |
| 07:42:31 | Exécution gold de référence `application_1795715756506_0442` : `SUCCESS` en 40,0 s | audit |
| 07:43:13 | Empreinte gold de référence (`fingerprint-before.json`) | événement |

### Incident

| Heure | Fait | Source |
| --- | --- | --- |
| 07:43:37 | Rejeu gold du 2026-01-03 soumis : `application_1795715756506_0444`, exécution `20261127T074347Z-6938fb03` | événement, journal du driver |
| 07:44:15 | Dernier battement de cœur reçu de `datanode1` | `Last contact` de `dfsadmin -report` |
| 07:44:17 | `gold/_state.json` passe à `PUBLISHING` ; `docker stop gtdp-datanode1-1` ; `datanode1` journalise `RECEIVED SIGNAL 15: SIGTERM` à 07:44:17,856 et `SHUTDOWN_MSG` 28 ms plus tard | `wait-state.log`, événement, journal de `datanode1` |
| 07:44:18 | Premières erreurs d'écriture côté clients : `Bad response ERROR ... from datanode 172.22.0.4`, reprise du pipeline sur 2 DataNodes (`best-effort`), `Excluding datanode` | journaux du driver et des exécuteurs |
| 07:44:18 | Le driver ouvre une écriture vers `172.22.0.4` et attend : `Slow waitForAckedSeqno took 65058ms` sur `_dim_customer.lock...tmp` | journal du driver |
| 07:44:19 | Le NameNode, qui croit `datanode1` vivant, demande de copier vers lui un bloc écrit pendant la panne (`BLOCK* ask [172.22.0.6:9866] to replicate ... to datanode(s) 172.22.0.4:9866`) | journal du NameNode |
| 07:45:23 | Le driver reprend. Jusqu'à 07:45:28, 41 écritures du driver et des exécuteurs excluent `datanode1` et repartent sur `datanode2` et `datanode3` | journaux du driver et des exécuteurs |
| 07:45:38 | Seconde attente du driver : `ConnectTimeoutException: 60000 millis` vers `172.22.0.4:9866` | journal du driver |
| 07:45:46 | Premier échantillon où `datanode1` est `dead` (2 vivants, 1 mort), 91 s après le dernier battement de cœur | échantillon |
| 07:45:49 | `BLOCK* removeDeadDatanode: lost heartbeat from 172.22.0.4:9866`, `Removing a node` | journal du NameNode |
| 07:45:49 | Le groupe en erasure coding de `products_20260101.csv` (une seule cellule) est recopié sur `datanode3` | journal du NameNode |
| 07:45:51 | 5 552 blocs sous-répliqués, 3 groupes en erasure coding en redondance réduite, 0 manquant, 0 corrompu | échantillon |
| 07:46:38 | Le driver reprend après la seconde attente | journal du driver |
| 07:46:45 | Fin du rejeu : `SUCCESS`, 178,2 s, `rows_in` = `rows_out` = 970 262 | audit |
| 07:46:52 | `fsck /` : `HEALTHY`, 5 568 blocs sur 5 568 sous-répliqués, 0 manquant, 0 corrompu | `fsck-during.txt` |
| 07:47:00 | Lecture dégradée de `/benchmarks/ec/orders_20260101.csv` (713 401 398 octets) en 3,0 s, MD5 identique à la copie répliquée | `ec-read-during.txt` |
| 07:47:32 | Empreinte gold après le rejeu identique à la référence | événement `fingerprints_identical` |
| 07:47:35 | Démarrage du DataNode de secours `datanode4` (profil `spare`) | événement |
| 07:47:36 | `datanode4` s'enregistre avec l'adresse `172.22.0.4`, libérée par `datanode1` ; le NameNode efface l'entrée morte de `datanode1` | journal du NameNode, échantillon |
| 07:47:37 | Premières copies vers `datanode4` | journal du NameNode |
| 08:33:16 | Le NameNode planifie enfin la reconstruction des 3 groupes en erasure coding, alors que 1 916 blocs répliqués attendent encore ; `datanode4` reçoit les 3 unités reconstruites à 08:33:19, 08:33:19 et 08:33:22 | journal du NameNode |
| 08:57:26 | Premier échantillon sans bloc sous-répliqué : 5 552 blocs copiés en 69 min 50 s | échantillon |
| 08:57:34 | `docker start gtdp-datanode1-1` (réparation) | événement |
| 08:57:36 | `datanode1` s'enregistre du premier coup, sous la nouvelle adresse `172.22.0.12`, et envoie son rapport de 5 452 blocs | journal du NameNode |
| 08:57:38 | Le NameNode choisit 5 452 répliques en trop : 5 448 sur `datanode2`, 4 sur `datanode1` | journal du NameNode (`chooseExcessRedundancies`) |
| 08:58:06 | Plus aucune réplique en trop ni suppression en attente ; `datanode2` ne porte plus que 127 blocs | échantillon |
| 08:58:12 | `datanode4` écrit dans `dfs.exclude`, `-refreshNodes` : `Starting decommission of 172.22.0.4:9866 ... with 5574 blocks` | journal du NameNode |
| 09:45:34 | `Decommissioning complete for node 172.22.0.4:9866` : 5 574 blocs recopiés, surtout vers `datanode2`, en 47 min 22 s | journal du NameNode |
| 09:45:42 | `docker stop gtdp-datanode4-1` | événement |
| 09:47:11 | `datanode4` mort et `Decommissioned`, 89 s après son arrêt | échantillon |
| 09:47:19 | État nominal vérifié : 3 DataNodes vivants, 0 bloc sous-répliqué, manquant ou corrompu, `fsck /` `HEALTHY` ; fin du scénario (`==> resilience scenario OK`) | `dfsadmin-after.txt`, `fsck-after.txt` |
| 09:47:51 | Fichier d'exclusion vidé et `-refreshNodes`, à la main (le scénario le fait désormais) : `datanode4` passe en `Decommission Status : Normal`, toujours mort | `events.csv`, `dfsadmin-final.txt` |
| 09:47:55 | Dernier `fsck /` : `HEALTHY`, 3 DataNodes, 0 bloc sous-répliqué, manquant ou corrompu | `fsck-final.txt` |

## Détection (symptôme)

Le cluster n'a pas d'alerte : l'échantillonneur du scénario et `hdfs dfsadmin -report` ont fait
la détection. Trois symptômes sont apparus dans cet ordre.

1. **Erreurs d'écriture, dès l'arrêt.** Les clients HDFS du driver et des exécuteurs ont
   journalisé 44 écritures exclues de `datanode1` (`Excluding datanode
   DatanodeInfoWithStorage[172.22.0.4:9866...]`) et une reprise de pipeline sur les DataNodes
   restants (`Failed to replace datanode. Continue with the remaining datanodes since
   dfs.client.block.write.replace-datanode-on-failure.best-effort is set to true`). Personne ne
   lit ces journaux en temps réel.
2. **DataNode mort dans le rapport, 90 s après son dernier battement de cœur.** Le dernier
   battement de cœur date de 07:44:15. L'échantillon de 07:45:41 montre encore `datanode1`
   vivant, celui de 07:45:46 le montre mort : le passage a eu lieu entre 86 s et 91 s après le
   dernier battement de cœur, pour 90 s attendues (`2 × 30 s + 10 × 3 s`).
   `dfsadmin -report` calcule cet état à chaque appel.
3. **Répliques retirées 94 s après le dernier battement de cœur.** Le contrôle périodique du
   NameNode, toutes les 30 s, a retiré `datanode1` à 07:45:49,352 (`removeDeadDatanode`).
   Le compteur de blocs sous-répliqués est passé de 15 à 5 552 entre les échantillons de 07:45:46
   et 07:45:51. L'écart attendu entre le seuil de 90 s et ce contrôle va de 0 à 35 s (période de
   30 s, plus le sommeil de 5 s du moniteur) : 4 s ici.

Avant 07:45:49, le compteur de blocs sous-répliqués est resté bas (24 au plus) : seuls les blocs
écrits pendant la panne sur 2 DataNodes comptaient. Le NameNode les croyait réparables vers
`datanode1` et a demandé 17 copies vers lui, perdues d'avance. Ces 17 demandes sont restées en
attente au-delà du délai de 300 s (`dfs.namenode.reconstruction.pending.timeout-sec`) : la
colonne `pending_reconstruction` passe de 21 à 4 à 07:51:11, quand le NameNode les abandonne
ensemble.

## Impact

### Jobs

- Le rejeu gold `application_1795715756506_0444` a réussi : code de sortie 0, audit `SUCCESS`,
  `rows_in` = `rows_out` = 970 262, `dates_rewritten` = 0, `dates_unchanged` = 367. Ses
  130 jobs Spark ont réussi, sans tâche en échec ni tâche tuée (Spark History Server).
- Aucun conteneur YARN n'a été perdu. Les DataNodes et les NodeManagers sont des conteneurs
  distincts : arrêter `gtdp-datanode1-1` n'a touché ni `nodemanager1` ni `nodemanager2`. Les deux
  exécuteurs du rejeu ont tourné du début à la fin.
- Le job n'a pas été tué : aucun verrou de table n'a eu besoin d'être repris.

### Données

- Aucune donnée perdue. `hdfs fsck /` est resté `HEALTHY` pendant toute la panne avec
  `Missing blocks: 0` et `Corrupt blocks: 0` (`fsck-during.txt`, `fsck-datalake-during.txt`).
- Les 14 tables gold ont la même empreinte avant et après le rejeu : même nombre de lignes, même
  somme `xxhash64` de toutes les colonnes, mêmes lignes dans chacune des 367 partitions
  `order_date` de `fact_sales` (`fingerprint-before.json` et `fingerprint-after.json`
  identiques octet pour octet).

| Table | Lignes | Somme `xxhash64` (avant = après) |
| --- | ---: | ---: |
| `fact_sales` | 970 262 | -2881570462660486831646 |
| `fact_web_events` | 100 046 | -1681996213791944574442 |
| `dim_customer` | 5 024 | 24507648744726280158 |
| `dim_product` | 1 321 | 265062534061753846413 |
| `dim_date` | 367 | 194534025747741642416 |
| 9 tables `kpi_*` | 51 975 au total | une somme par table, dans les fichiers JSON |

- **Exposition.** De 07:45:49 à 08:57:26, soit 71 min 37 s, les 5 552 blocs répliqués
  n'avaient plus que 2 répliques : une seconde panne n'aurait rien perdu, une troisième aurait perdu des blocs.
  Les 3 groupes de `/benchmarks/ec/orders_20260101.csv` n'avaient plus aucune tolérance :
  2 unités sur 3 en `XOR-2-1`. Une seconde panne parmi `datanode2` et `datanode3` rendait ce
  fichier de 713 Mo illisible, pendant 47 min 33 s (de 07:45:49 à 08:33:22).

### Performance

- Le rejeu a duré 178,2 s, contre 40,0 s pour l'exécution de référence lancée une minute plus
  tôt sur les mêmes données. Les 138 s d'écart viennent de deux attentes du driver, toutes deux
  en écrivant un petit fichier de verrou de table : 65 s (`Slow waitForAckedSeqno took
  65058ms`, fichier `_dim_customer.lock...tmp`) et 60 s (`ConnectTimeoutException: 60000
  millis`, fichier `_kpi_price_deviation.lock.tmp`). Le NameNode proposait encore `datanode1`
  comme premier DataNode du pipeline, et une connexion vers l'adresse d'un conteneur arrêté
  attend la fin du délai `dfs.client.socket-timeout` (60 s). Les exécuteurs, eux, ont reçu un
  refus immédiat (`Connection refused`) ou une erreur du premier DataNode du pipeline
  (`firstBadLink as 172.22.0.4:9866`) et ont repris en moins d'une seconde. Le reste de
  l'écart, 13 s, correspond à ces reprises. Une seule mesure de chaque côté : l'ordre de
  grandeur est sûr, pas la seconde près.
- La lecture complète de `/benchmarks/ec/orders_20260101.csv` pendant la panne a pris 3,0 s,
  contre 2,6 s pour sa copie répliquée lue juste après. Chacun de ses 3 groupes avait perdu une
  unité de données, pas de parité : le client a reconstruit ces cellules par XOR.
- La re-réplication a pris 69 min 50 s pour 5 552 blocs, de l'enregistrement de `datanode4` à
  07:47:36 au premier échantillon sans bloc sous-répliqué à 08:57:26 : 1,33 bloc par seconde,
  la valeur que donne la limite de copies en cours.

## Hypothèses et preuves

| Hypothèse | Verdict | Preuve |
| --- | --- | --- |
| Le rejeu perd ou duplique des lignes quand un DataNode tombe pendant ses écritures. | réfutée | empreintes identiques, audit `rows_in` = `rows_out`, `fsck` sans bloc manquant |
| Le NameNode déclare le nœud mort en 90 s. | confirmée | `Last contact` 07:44:15, mort entre 07:45:41 et 07:45:46, `removeDeadDatanode` à 07:45:49 |
| Avec 3 DataNodes, les répliques perdues n'ont aucune cible. | confirmée | de 07:45:50 à 07:47:36, 5 552 blocs sous-répliqués et aucune copie demandée (`BLOCK* ask` absent du journal) ; les copies démarrent à 07:47:37, une seconde après l'enregistrement de `datanode4` |
| Le ralentissement du rejeu vient des écritures vers le nœud arrêté. | confirmée | deux attentes de 65 s et 60 s du driver vers `172.22.0.4:9866`, 0 tâche en échec ; le rejeu a atteint `PUBLISHING` 30 s après son début, avant l'arrêt, et toute la différence de durée tombe après 07:44:17 |
| La re-réplication est limitée par `dfs.namenode.replication.max-streams`. | confirmée | 1,33 bloc par seconde, constant ; `pending_reconstruction` reste à 4 = 2 DataNodes sources × 2 copies en cours chacun ; chaque source reçoit ses 2 copies au battement de cœur suivant, toutes les 3 s : 4 copies par 3 s font 1,33 par seconde |
| Les groupes en erasure coding, de priorité la plus haute, sont réparés en premier. | réfutée | entrés en file de priorité 0 à 07:45:49, ils ne sont planifiés qu'à 08:33:16 (`BLOCK* ask [172.22.0.6:9866, 172.22.0.5:9866] to replicate blk_-9223372036854775504_50169 to datanode(s) 172.22.0.4:9866`), alors que 1 916 blocs répliqués attendent encore ; `datanode4` reçoit les 3 unités reconstruites entre 08:33:19 et 08:33:22 |
| La lecture d'un fichier en erasure coding reconstruit les données perdues. | confirmée | MD5 `1d3e3c5f1698eba670ee9d04664db2a5` identique pour la copie EC et la copie répliquée ; CRC composite `5056866c` identique à celui du Lot 7 ; `fsck` montre l'unité de données manquante dans chaque groupe |

Les extraits de journaux sont dans `namenode-excerpts.log`, `gold-replay-driver-hdfs.log` et
`gold-replay-executors-hdfs.log`, les mesures dans `timeline.csv` et `summary.txt`.

## Cause

**Cause directe.** Le conteneur `gtdp-datanode1-1` a été arrêté volontairement, pendant
l'exercice, alors que gold écrivait.

**Facteurs qui ont déterminé l'impact.**

1. **3 DataNodes pour une réplication 3.** Les répliques d'un nœud perdu n'ont nulle part où
   aller : sans `datanode4`, les 5 552 blocs restent à 2 répliques jusqu'au retour du nœud.
2. **90 s pendant lesquelles le NameNode croit le nœud vivant.** Il le propose comme cible
   d'écriture et de copie. `dfs.namenode.avoid.write.stale.datanode` vaut `false` : le
   NameNode n'écarte pas un DataNode silencieux depuis 30 s (`dfs.namenode.stale.datanode.interval`).
   Chaque écriture qui commence par ce nœud attend jusqu'à 60 s (`dfs.client.socket-timeout`).
3. **Re-réplication bridée.** `dfs.namenode.replication.max-streams` = 2 copies en cours par
   DataNode source, battement de cœur toutes les 3 s : 1,33 bloc par seconde. Les blocs sont
   petits (453 957 octets en moyenne) : le réseau et les disques ne limitent pas.
4. **Les blocs les plus exposés réparés en dernier.** Le NameNode parcourt ses files de blocs
   à réparer avec un signet, 6 blocs par passage de 3 s (3 DataNodes vivants ×
   `dfs.namenode.replication.work.multiplier.per.iteration` = 2). Un bloc sans cible au moment
   du passage reste derrière le signet. Les 3 groupes en erasure coding ont été examinés à
   07:45:49, dans le passage qui a recopié la cellule de `products`, avant l'arrivée de
   `datanode4` : aucune cible. Le signet ne revient en tête qu'à la fin d'un parcours complet
   des files (ou toutes les 2 400 itérations, soit 2 h,
   `dfs.namenode.redundancy.queue.restart.iterations`). Le parcours des quelque 5 400 blocs
   répliqués restants, à 2 blocs par seconde, devait finir entre 08:32:38 et 08:33:52 : le
   NameNode a planifié les 3 groupes à 08:33:16. Les blocs répliqués en souffrent de la même
   façon : à chaque passage, le signet visite 6 blocs mais n'en confie que 4 aux sources
   (limite de copies en cours), et les 2 autres attendent le parcours suivant.
5. **Adresses IP réattribuées par Docker.** `datanode4` a reçu l'adresse de `datanode1`. Le
   NameNode a effacé l'entrée morte de `datanode1` à l'enregistrement de `datanode4`. Rien de
   grave ici, mais les journaux par adresse IP deviennent ambigus et le fichier d'exclusion,
   résolu en adresses IP, peut viser le mauvais conteneur (voir la résolution).

## Résolution

Les étapes suivent la procédure « Perte d'un DataNode » du [runbook HDFS](../runbook/hdfs.md).

1. **Remplacer le nœud perdu.** `docker compose --profile spare up --detach --no-deps
   datanode4` à 07:47:35. `datanode4` s'est enregistré une seconde plus tard et a reçu
   5 552 répliques et 3 unités d'erasure coding. Plus aucun bloc sous-répliqué à 08:57:26.
   Les 3 groupes en erasure coding sont restés sans tolérance jusqu'à 08:33:22 (facteur 4
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
   aurait été décommissionné. `datanode4` reste listé mort, `Decommission Status : Normal`,
   sans aucun bloc, jusqu'au prochain redémarrage du NameNode.

## Validation du retour au nominal

Dernier `hdfs dfsadmin -report` (`dfsadmin-final.txt`, pris à 09:47 après le vidage du fichier d'exclusion) :

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
	datanode1  Normal  Num of Blocks: 5574
	datanode2  Normal  Num of Blocks: 5575
	datanode3  Normal  Num of Blocks: 5575
Dead datanodes (1):
	datanode4  Normal  Num of Blocks: 0
```

Dernier `hdfs fsck /` (`fsck-final.txt`, 09:47:55) :

```
Status: HEALTHY
 Number of data-nodes:	3
 Total blocks (validated):	5571 (avg. block size 453786 B)
 Under-replicated blocks:	0 (0.0 %)
 Missing blocks:		0
 Corrupt blocks:		0
 Total block groups (validated):	4 (avg. block group size 178370341 B)
 Under-erasure-coded block groups:	0 (0.0 %)
 Missing block groups:		0
 Corrupt block groups:		0
The filesystem under path '/' is HEALTHY
```

`scripts/cluster/status.sh` répond `HDFS  3 live / 1 dead DataNodes, safe mode off` et
`YARN  2 RUNNING / 2 NodeManagers`. Le conteneur `gtdp-datanode4-1` est arrêté
(`Exited (143)`), son volume `gtdp_datanode4` conservé. Le journal `BlockStateChange` du
NameNode est revenu au niveau INFO.

La chronologie `timeline.csv` compte deux fois `datanode4` dans les colonnes `live` et
`decommissioning` de 08:58:16 à 09:45:31 (5 et 2 au lieu de 4 et 1) :
`dfsadmin -report` liste un nœud en décommission dans deux sections, et l'échantillonneur de
l'exécution lisait la seconde comme des nœuds vivants. Les colonnes par DataNode sont justes.
Le défaut est corrigé et testé sur le rapport capturé (`report-decommissioning.txt`).

## Actions préventives

| # | Action | Rôle propriétaire | Effet attendu |
| --- | --- | --- | --- |
| 1 | Garder 4 DataNodes en service pour une réplication 3, ou au moins le secours `datanode4` prêt à démarrer, comme aujourd'hui. Avec plusieurs racks, activer la topologie (`net.topology.script.file.name`). | architecte plateforme | les répliques perdues ont une cible dès la détection, sans geste manuel ; 640 Mio de mémoire en plus |
| 2 | Alerter sur `NumDeadDataNodes - NumDecomDeadDataNodes > 0`, sur `MissingBlocks > 0` (alerte immédiate), sur `LowRedundancyECBlockGroups > 0` et sur `UnderReplicatedBlocks > 0` pendant plus de 10 min (JMX `FSNamesystem` du NameNode), et sur les `JvmPauseMonitor` de niveau WARN des DataNodes. | exploitation (SRE) | détection en moins de 2 min sans échantillonneur manuel |
| 3 | Activer `dfs.namenode.avoid.write.stale.datanode` et `dfs.namenode.avoid.read.stale.datanode` (seuil par défaut de 30 s), puis rejouer `scripts/resilience/datanode-failure.sh`. | architecte plateforme | fenêtre où le NameNode propose le nœud arrêté réduite de 90 s à 30 s ; attentes de 60 s du driver évitées après 30 s |
| 4 | Régler `dfs.namenode.replication.max-streams` (2) et `dfs.namenode.replication.work.multiplier.per.iteration` (2) pour des blocs de 450 Ko, par exemple 8 et 4. Les deux se changent à chaud (`hdfs dfsadmin -reconfig namenode namenode:8020 start`). Mesurer avec le scénario. | architecte plateforme | re-réplication environ 3 fois plus rapide : 4 blocs par seconde (3 DataNodes × 4 blocs examinés par passage de 3 s) au lieu de 1,33 ; exposition réduite d'autant |
| 5 | Ramener `dfs.namenode.redundancy.queue.restart.iterations` de 2 400 à 20 (retour du signet en tête toutes les 60 s ; lu au démarrage du NameNode), et démarrer `datanode4` dès la première alerte, avant la fin des 90 s, pour que le premier passage trouve une cible. | architecte plateforme | groupes en erasure coding sans tolérance réparés environ une minute après l'arrivée du secours, au lieu de 46 min |
| 6 | Arbitrage de l'erasure coding : `XOR-2-1` sur 3 DataNodes tolère une panne et ne se répare qu'avec un troisième nœud disponible. Le réserver aux données froides et reproductibles ; `RS-3-2` demande 5 DataNodes (ADR-0006). | architecte données | pas de donnée de production sans tolérance pendant une panne |
| 7 | Suivre la procédure du [runbook HDFS](../runbook/hdfs.md#perte-dun-datanode) : vider le fichier d'exclusion une fois `datanode4` mort, à cause de la réattribution des adresses IP. | exploitation (SRE) | aucun DataNode décommissionné par erreur |
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

Le dossier [`2026-11-27-perte-datanode/`](2026-11-27-perte-datanode/) contient la sortie du
scénario, taillée :

| Fichier | Contenu |
| --- | --- |
| `timeline.csv`, `summary.txt` | un échantillon toutes les 5 s de 07:42 à 09:47, et les jalons calculés par `hdfs_watch.py summarize` |
| `events.csv`, `wait-state.log` | les étapes du scénario, la ligne de 09:47:51 ajoutée à la main |
| `dfsadmin-*.txt`, `fsck-*.txt` | rapports avant, pendant, après la re-réplication, pendant la décommission, à la fin du scénario (`after`) et après le vidage du fichier d'exclusion (`final`) ; `fsck` sans la liste des blocs |
| `ec-*.txt` | emplacement des unités, CRC composites et MD5 de la lecture dégradée |
| `fingerprint-before.json`, `fingerprint-after.json`, `gold-audit.txt`, `gold-runs.log` | empreintes gold, lignes d'audit et début et fin des deux exécutions |
| `gold-replay-driver-hdfs.log`, `gold-replay-executors-hdfs.log` | avertissements du client HDFS du rejeu, sans les piles d'appels |
| `namenode-excerpts.log` | lignes du NameNode : nœud mort, enregistrements, copies demandées, répliques en trop, décommission |

## Rejouer l'exercice

```bash
scripts/resilience/datanode-failure.sh --victim datanode1 --date 2026-01-03
```

Le script et ses prérequis sont décrits dans le [runbook HDFS](../runbook/hdfs.md#rejouer-la-panne).
`datanode1` a été choisi parce qu'il portait une unité de données de chacun des 4 groupes en
erasure coding de `/benchmarks/ec` : sa perte force une reconstruction dans chaque groupe.
`datanode2` portait la parité de 2 des 3 groupes de `orders` : l'arrêter n'aurait demandé de
reconstruction que dans un groupe.
